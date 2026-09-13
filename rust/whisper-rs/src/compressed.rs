use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

type Table = BTreeMap<i64, f64>;

fn unpack_buffer(bytes: &[u8]) -> Vec<Point> {
    bytes
        .as_chunks::<12>()
        .0
        .iter()
        .map(|b| Point {
            timestamp: be_u32(&b[..4]) as i64,
            value: be_f64(&b[4..]),
        })
        .collect()
}
fn pack_point(bytes: &mut [u8], point: Point) {
    bytes[..4].copy_from_slice(&(point.timestamp as u32).to_be_bytes());
    bytes[4..12].copy_from_slice(&point.value.to_bits().to_be_bytes());
}

impl Whisper {
    fn current_compressed_table(&self, index: usize) -> io::Result<Table> {
        let archive = &self.archives[index];
        let compressed = &self.compressed_archives.as_ref().unwrap()[index];
        if compressed.ranges[compressed.current].start == 0 {
            return Ok(Table::new());
        }
        let mut block = vec![0; compressed.block_size];
        read_exact_at(
            &self.file,
            &mut block,
            archive.offset + compressed.current as u64 * compressed.block_size as u64,
        )?;
        Ok(decode_block(&block, archive.retention.seconds_per_point)?
            .into_iter()
            .map(|p| (p.timestamp, p.value))
            .collect())
    }
    fn compressed_table(&self, index: usize) -> io::Result<Table> {
        let archive = &self.archives[index];
        let compressed = &self.compressed_archives.as_ref().unwrap()[index];
        let mut table = Table::new();
        for (n, range) in compressed.ranges.iter().enumerate() {
            if range.start == 0 {
                continue;
            }
            let mut block = vec![0; compressed.block_size];
            read_exact_at(
                &self.file,
                &mut block,
                archive.offset + n as u64 * compressed.block_size as u64,
            )?;
            for point in decode_block(&block, archive.retention.seconds_per_point)? {
                if point.timestamp < range.start || point.timestamp > range.end {
                    return Err(invalid("compressed point outside block range"));
                }
                table.insert(point.timestamp, point.value);
            }
        }
        Ok(table)
    }

    pub(super) fn compressed_live_points(&self, index: usize) -> io::Result<Vec<Point>> {
        let archives = self.compressed_archives.as_ref().unwrap();
        let mut propagated = Vec::<Point>::new();
        for (i, archive) in archives.iter().enumerate().take(index + 1) {
            let mut points = unpack_buffer(&archive.buffer)
                .into_iter()
                .filter(|p| p.timestamp != 0)
                .collect::<Vec<_>>();
            points.splice(0..0, propagated);
            if i == index {
                return Ok(points);
            }
            let step = self.archives[i + 1].retention.seconds_per_point;
            let mut groups = BTreeMap::<i64, Vec<f64>>::new();
            for p in points {
                groups
                    .entry(floor(p.timestamp, step))
                    .or_default()
                    .push(p.value);
            }
            // Go exposes live buffer aggregates before the XFF-controlled disk propagation.
            propagated = groups
                .into_iter()
                .map(|(timestamp, values)| Point {
                    timestamp,
                    value: aggregate(self.metadata.aggregation, &values),
                })
                .collect();
        }
        Ok(vec![])
    }

    pub(super) fn update_compressed(&mut self, points: &[Point], now: i64) -> io::Result<()> {
        let mut tables = (0..self.archives.len())
            .map(|i| self.current_compressed_table(i))
            .collect::<io::Result<Vec<_>>>()?;
        let original = tables.clone();
        let mut buffers = self
            .compressed_archives
            .as_ref()
            .unwrap()
            .iter()
            .map(|a| unpack_buffer(&a.buffer))
            .collect::<Vec<_>>();
        for point in points {
            if point.timestamp <= 0 || point.timestamp > u32::MAX as i64 || point.value.is_nan() {
                return Err(invalid("invalid compressed point"));
            }
        }
        let grouped = self.group_points(points, now);
        let mut dropped = Vec::new();
        for (i, group) in grouped.into_iter().enumerate() {
            let aligned: Table = group
                .into_iter()
                .map(|p| {
                    (
                        floor(p.timestamp, self.archives[i].retention.seconds_per_point),
                        p.value,
                    )
                })
                .collect();
            let points = aligned
                .into_iter()
                .map(|(timestamp, value)| Point { timestamp, value })
                .collect::<Vec<_>>();
            self.apply_compressed(i, &points, &mut tables, &mut buffers, &mut dropped)?;
        }
        if self.options.out_of_order && !dropped.is_empty() {
            let path = out_of_order_sidecar_path(&self.path);
            let options = Options {
                sparse: true,
                flock: self.options.flock,
                ..Options::default()
            };
            let sidecar = match Self::open(&path, options) {
                Ok(w) => w,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    let mut metadata = self.metadata.clone();
                    metadata.compressed = false;
                    Self::create(&path, metadata, options)?
                }
                Err(e) => return Err(e),
            };
            if sidecar.metadata.retentions != self.metadata.retentions {
                return Err(invalid("incompatible out-of-order sidecar"));
            }
            for (i, point) in dropped {
                sidecar.write_point(i, point)?;
                sidecar.propagate_from(i, point.timestamp)?;
            }
            sidecar.file.sync_data()?;
        }
        self.write_compressed_changes(original, tables, buffers, now)
    }

    fn write_compressed_changes(
        &mut self,
        original: Vec<Table>,
        tables: Vec<Table>,
        buffers: Vec<Vec<Point>>,
        now: i64,
    ) -> io::Result<()> {
        let header_len = self.archives[0].offset as usize;
        if header_len > 64 * 1024 * 1024 {
            return Err(invalid("compressed header exceeds safety limit"));
        }
        let mut header = vec![0; header_len];
        read_exact_at(&self.file, &mut header, 0)?;
        let compressed = self.compressed_archives.as_ref().unwrap();
        let mut range_offset = 63 + 128 * self.archives.len();
        let mut writes = Vec::new();
        let mut need_growth = false;
        for (i, table) in tables.iter().enumerate() {
            let c = &compressed[i];
            let archive = &self.archives[i];
            let p = 63 + 128 * i;
            if table != &original[i] {
                let points = table
                    .iter()
                    .map(|(&timestamp, &value)| Point { timestamp, value })
                    .collect::<Vec<_>>();
                let mut offset = 0;
                let mut block_index = c.current;
                while offset < points.len() {
                    let remaining = &points[offset..];
                    let mut low = 1;
                    let mut high = remaining.len();
                    let mut best = None;
                    while low <= high {
                        let n = low + (high - low) / 2;
                        match encode_block_state(
                            &remaining[..n],
                            archive.retention.seconds_per_point,
                            c.block_size,
                        ) {
                            Ok(block) => {
                                best = Some((n, block));
                                low = n + 1;
                            }
                            Err(e) if e.kind() == ErrorKind::WriteZero => {
                                high = n - 1;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    let Some((n, block)) = best else {
                        need_growth = true;
                        break;
                    };
                    let final_block = offset + n == points.len();
                    let base = archive.offset as usize + block_index * c.block_size;
                    let checksum = if final_block {
                        block.crc
                    } else {
                        crc32fast::hash(&block.bytes)
                    };
                    let r = range_offset + block_index * 16;
                    put(&mut header, r, remaining[0].timestamp as u32);
                    put(&mut header, r + 4, remaining[n - 1].timestamp as u32);
                    put(&mut header, r + 8, n.saturating_sub(1) as u32);
                    put(&mut header, r + 12, checksum);
                    if final_block {
                        put(&mut header, p + 24, block_index as u32);
                        pack_point(&mut header[p + 28..p + 40], remaining[0]);
                        pack_point(&mut header[p + 40..p + 52], remaining[n - 1]);
                        pack_point(&mut header[p + 52..p + 64], remaining[n.saturating_sub(2)]);
                        put(&mut header, p + 64, block.last_byte as u32);
                        put(&mut header, p + 68, (base + block.last_offset) as u32);
                        put(&mut header, p + 72, block.bit as u32);
                        put(&mut header, p + 76, n.saturating_sub(1) as u32);
                        put(&mut header, p + 80, checksum);
                    }
                    writes.push((base as u64, block.bytes));
                    offset += n;
                    if !final_block {
                        block_index = (block_index + 1) % c.ranges.len();
                        let oldest = now
                            - archive.retention.seconds_per_point as i64
                                * archive.retention.points as i64;
                        if block_index == c.current
                            || c.ranges[block_index].end > oldest
                                && c.ranges[block_index].start != 0
                        {
                            need_growth = true;
                            break;
                        }
                    }
                }
            }
            range_offset += c.ranges.len() * 16;
            for point in &buffers[i] {
                pack_point(&mut header[range_offset..range_offset + 12], *point);
                range_offset += 12;
            }
            if need_growth {
                break;
            }
        }
        if need_growth {
            let mut full = (0..self.archives.len())
                .map(|i| self.compressed_table(i))
                .collect::<io::Result<Vec<_>>>()?;
            for (i, table) in tables.into_iter().enumerate() {
                full[i].extend(table);
            }
            return self.rewrite_compressed(full, buffers, now);
        }
        for (offset, bytes) in writes {
            write_all_at(&self.file, &bytes, offset)?;
        }
        put(&mut header, 43, 0);
        let checksum = crc32fast::hash(&header);
        put(&mut header, 43, checksum);
        write_all_at(&self.file, &header, 0)?;
        let next = Self::open_compressed(
            self.path.clone(),
            self.file.try_clone()?,
            self.lock_file.take(),
            self.options,
        )?;
        *self = next;
        Ok(())
    }

    fn apply_compressed(
        &self,
        index: usize,
        points: &[Point],
        tables: &mut [Table],
        buffers: &mut [Vec<Point>],
        dropped: &mut Vec<(usize, Point)>,
    ) -> io::Result<()> {
        if buffers[index].is_empty() {
            for &point in points {
                if tables[index]
                    .last_key_value()
                    .is_some_and(|(t, _)| *t >= point.timestamp)
                {
                    dropped.push((index, point));
                } else {
                    tables[index].insert(point.timestamp, point.value);
                }
            }
            return Ok(());
        }
        let step = self.archives[index].retention.seconds_per_point;
        let lower_step = self.archives[index + 1].retention.seconds_per_point;
        let slots = (lower_step / step) as usize;
        for &point in points {
            'point: {
                let unit_times = buffers[index]
                    .chunks(slots)
                    .map(|unit| {
                        unit.iter()
                            .find(|p| p.timestamp > 0)
                            .map(|p| floor(p.timestamp, lower_step))
                            .unwrap_or(0)
                    })
                    .collect::<Vec<_>>();
                let minimum = unit_times.iter().copied().min().unwrap_or(0);
                let time = floor(point.timestamp, lower_step);
                if minimum != 0 && time < minimum {
                    dropped.push((index, point));
                    break 'point;
                }
                let current = unit_times
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, t)| *t)
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                let target = unit_times
                    .iter()
                    .position(|t| *t == time)
                    .or_else(|| (unit_times[current] == 0).then_some(current));
                if let Some(unit) = target {
                    buffers[index]
                        [unit * slots + ((point.timestamp - time) / step as i64) as usize] = point;
                    break 'point;
                }
                let next = (current + 1) % unit_times.len();
                let mut flushed = Vec::new();
                for p in &mut buffers[index][next * slots..(next + 1) * slots] {
                    if p.timestamp > 0 {
                        flushed.push(*p);
                    }
                    *p = Point {
                        timestamp: 0,
                        value: 0.0,
                    };
                }
                flushed.sort_by_key(|p| p.timestamp);
                let mut accepted = Vec::new();
                for p in flushed {
                    if tables[index]
                        .last_key_value()
                        .is_some_and(|(t, _)| *t >= p.timestamp)
                    {
                        dropped.push((index, p));
                    } else {
                        tables[index].insert(p.timestamp, p.value);
                        accepted.push(p);
                    }
                }
                if !accepted.is_empty()
                    && accepted.len() as f32 / slots as f32 >= self.metadata.x_files_factor
                {
                    let values = accepted.iter().map(|p| p.value).collect::<Vec<_>>();
                    self.apply_compressed(
                        index + 1,
                        &[Point {
                            timestamp: floor(accepted[0].timestamp, lower_step),
                            value: aggregate(self.metadata.aggregation, &values),
                        }],
                        tables,
                        buffers,
                        dropped,
                    )?;
                }
                // The cleared unit is where the next interval starts, even when the other unit is newer.
                buffers[index][next * slots + ((point.timestamp - time) / step as i64) as usize] =
                    point;
            }
        }
        Ok(())
    }

    pub(super) fn compact_compressed(&mut self, now: i64) -> io::Result<()> {
        let path = out_of_order_sidecar_path(&self.path);
        let sidecar = match Self::open(
            &path,
            Options {
                flock: self.options.flock,
                ..Options::default()
            },
        ) {
            Ok(w) => w,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        if sidecar.metadata.retentions != self.metadata.retentions {
            return Err(invalid("incompatible out-of-order sidecar"));
        }
        let mut tables = (0..self.archives.len())
            .map(|i| self.compressed_table(i))
            .collect::<io::Result<Vec<_>>>()?;
        let mut buffers = self
            .compressed_archives
            .as_ref()
            .unwrap()
            .iter()
            .map(|a| unpack_buffer(&a.buffer))
            .collect::<Vec<_>>();
        let mut touched = vec![BTreeSet::new(); tables.len()];
        for i in 0..tables.len() {
            for p in &buffers[i] {
                if p.timestamp > 0 {
                    tables[i].insert(p.timestamp, p.value);
                }
            }
            let archive = &sidecar.archives[i];
            let mut bytes = vec![0; archive_size(archive.retention) as usize];
            read_exact_at(&sidecar.file, &mut bytes, archive.offset)?;
            let oldest =
                now - archive.retention.seconds_per_point as i64 * archive.retention.points as i64;
            for p in unpack_buffer(&bytes) {
                if p.timestamp > 0 && p.timestamp > oldest {
                    touched[i].insert(p.timestamp);
                    tables[i].entry(p.timestamp).or_insert(p.value);
                }
            }
            if i > 0 {
                let lower = self.archives[i].retention.seconds_per_point;
                let higher = self.archives[i - 1].retention.seconds_per_point;
                let windows = touched[i - 1]
                    .iter()
                    .map(|t| floor(*t, lower))
                    .collect::<BTreeSet<_>>();
                for start in windows {
                    let values = tables[i - 1]
                        .range(start..start + lower as i64)
                        .map(|(_, v)| *v)
                        .collect::<Vec<_>>();
                    if !values.is_empty()
                        && values.len() as f32 / (lower / higher) as f32
                            >= self.metadata.x_files_factor
                    {
                        tables[i].insert(start, aggregate(self.metadata.aggregation, &values));
                        touched[i].insert(start);
                    }
                }
            }
        }
        // Buffer-resident values remain live: closing them into blocks here would
        // suppress future coarse propagation when Go resumes writing this file.
        for (i, buffer) in buffers.iter_mut().enumerate() {
            for point in buffer.iter_mut().filter(|p| p.timestamp > 0) {
                if let Some(value) = tables[i].remove(&point.timestamp) {
                    point.value = value;
                }
            }
        }
        self.rewrite_compressed(tables, buffers, now)?;
        drop(sidecar);
        fs::remove_file(path)?;
        File::open(self.path.parent().unwrap_or(Path::new(".")))?.sync_all()
    }

    // Growth and sidecar compaction replace the file under its stable path lock.
    fn rewrite_compressed(
        &mut self,
        mut tables: Vec<Table>,
        buffers: Vec<Vec<Point>>,
        now: i64,
    ) -> io::Result<()> {
        let count = tables.len();
        let mut blocks = Vec::<Vec<(Vec<Point>, EncodedBlock)>>::new();
        let mut sizes = Vec::new();
        let mut block_counts = Vec::new();
        for (i, table) in tables.iter_mut().enumerate() {
            let retention = self.archives[i].retention;
            table.retain(|time, _| {
                *time > now - retention.seconds_per_point as i64 * retention.points as i64
            });
            let points = table
                .iter()
                .map(|(&timestamp, &value)| Point { timestamp, value })
                .collect::<Vec<_>>();
            let ppb = (retention.points as usize).clamp(1, 7200);
            let mut encoded = Vec::new();
            let mut size = 32;
            for chunk in points.chunks(ppb) {
                let block =
                    encode_block_state(chunk, retention.seconds_per_point, chunk.len() * 15 + 32)?;
                size = size.max(block.last_offset + 6);
                encoded.push((chunk.to_vec(), block));
            }
            let size = size + 32;
            let number = (retention.points as usize).div_ceil(ppb).max(encoded.len()) + 1;
            sizes.push(size);
            block_counts.push(number);
            blocks.push(encoded);
        }
        let header_len = 63
            + 128 * count
            + block_counts.iter().map(|n| n * 16).sum::<usize>()
            + buffers.iter().map(|b| b.len() * 12).sum::<usize>();
        let mut header = vec![0; header_len];
        header[..18].copy_from_slice(COMPRESSED_MAGIC);
        header[18] = 1;
        put(&mut header, 19, self.metadata.aggregation as u32);
        put(&mut header, 23, self.max_retention() as u32);
        put(&mut header, 27, self.metadata.x_files_factor.to_bits());
        put(&mut header, 31, 7200);
        put(&mut header, 35, count as u32);
        put(&mut header, 39, 2f32.to_bits());
        let mut offset = header_len;
        let mut range_offset = 63 + 128 * count;
        let mut disk_blocks = Vec::new();
        for i in 0..count {
            let p = 63 + i * 128;
            let retention = self.archives[i].retention;
            if offset > u32::MAX as usize {
                return Err(invalid("compressed file exceeds format limit"));
            }
            put(&mut header, p, offset as u32);
            put(&mut header, p + 4, retention.seconds_per_point);
            put(&mut header, p + 8, retention.points);
            put(&mut header, p + 12, sizes[i] as u32);
            put(&mut header, p + 16, block_counts[i] as u32);
            let average = sizes[i] as f32 / (retention.points.clamp(1, 7200) as f32);
            put(&mut header, p + 20, average.to_bits());
            let last = blocks[i].len().saturating_sub(1);
            put(&mut header, p + 24, last as u32);
            put(&mut header, p + 68, (offset + last * sizes[i]) as u32);
            put(&mut header, p + 72, 7);
            for (n, (points, block)) in blocks[i].iter().enumerate() {
                let mut raw = block.bytes[..sizes[i].min(block.bytes.len())].to_vec();
                raw.resize(sizes[i], 0);
                let first = points[0];
                let final_point = *points.last().unwrap();
                let checksum = if n == last {
                    block.crc
                } else {
                    crc32fast::hash(&raw)
                };
                let r = range_offset + n * 16;
                put(&mut header, r, first.timestamp as u32);
                put(&mut header, r + 4, final_point.timestamp as u32);
                put(&mut header, r + 8, points.len().saturating_sub(1) as u32);
                put(&mut header, r + 12, checksum);
                if n == last {
                    pack_point(&mut header[p + 28..p + 40], first);
                    pack_point(&mut header[p + 40..p + 52], final_point);
                    pack_point(
                        &mut header[p + 52..p + 64],
                        points[points.len().saturating_sub(2)],
                    );
                    put(&mut header, p + 64, block.last_byte as u32);
                    put(
                        &mut header,
                        p + 68,
                        (offset + n * sizes[i] + block.last_offset) as u32,
                    );
                    put(&mut header, p + 72, block.bit as u32);
                    put(&mut header, p + 76, points.len().saturating_sub(1) as u32);
                    put(&mut header, p + 80, checksum);
                }
                disk_blocks.push((offset + n * sizes[i], raw));
            }
            range_offset += block_counts[i] * 16;
            for point in &buffers[i] {
                pack_point(&mut header[range_offset..range_offset + 12], *point);
                range_offset += 12;
            }
            offset += sizes[i] * block_counts[i];
        }
        let checksum = crc32fast::hash(&header);
        put(&mut header, 43, checksum);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| invalid("clock before epoch"))?
            .as_nanos();
        let temp = self
            .path
            .with_extension(format!("wsp.rewrite.{}.{stamp}", std::process::id()));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temp)?;
        let result = (|| {
            file.set_permissions(self.file.metadata()?.permissions())?;
            if self.options.flock {
                file.lock()?;
            }
            write_all_at(&file, &header, 0)?;
            file.set_len(offset as u64)?;
            for (at, bytes) in disk_blocks {
                write_all_at(&file, &bytes, at as u64)?;
            }
            file.sync_all()?;
            let mut next = Self::open_compressed(self.path.clone(), file, None, self.options)?;
            fs::rename(&temp, &self.path)?;
            next.lock_file = self.lock_file.take();
            *self = next;
            File::open(self.path.parent().unwrap_or(Path::new(".")))?.sync_all()
        })();
        if result.is_err() && temp.exists() {
            let _ = fs::remove_file(&temp);
        }
        result
    }
}

fn put(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_be_bytes());
}
