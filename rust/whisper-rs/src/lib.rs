//! Whisper's classic on-disk format.
//!
//! The layout and semantics in this module are derived from go-whisper,
//! distributed under the BSD 3-Clause License (see the vendored LICENCE.txt).

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Error, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
mod compressed;

const POINT_SIZE: u64 = 12;
const METADATA_SIZE: u64 = 16;
const ARCHIVE_INFO_SIZE: u64 = 12;
const COMPRESSED_MAGIC: &[u8] = b"whisper_compressed";

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retention {
    pub seconds_per_point: u32,
    pub points: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum Aggregation {
    Average = 1,
    Sum = 2,
    Last = 3,
    Max = 4,
    Min = 5,
    First = 6,
}

impl Aggregation {
    fn from_disk(v: u32) -> io::Result<Self> {
        match v {
            1 => Ok(Self::Average),
            2 => Ok(Self::Sum),
            3 => Ok(Self::Last),
            4 => Ok(Self::Max),
            5 => Ok(Self::Min),
            6 => Ok(Self::First),
            _ => Err(invalid("unknown aggregation method")),
        }
    }
}

/// Go's titleized names, the wire format of carbonserver's consolidationFunc.
impl std::fmt::Display for Aggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Average => "Average",
            Self::Sum => "Sum",
            Self::Last => "Last",
            Self::Max => "Max",
            Self::Min => "Min",
            Self::First => "First",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    pub aggregation: Aggregation,
    pub x_files_factor: f32,
    pub retentions: Vec<Retention>,
    pub compressed: bool,
}

impl Metadata {
    /// Validate storage settings and normalize retention order before admission.
    pub fn validate(&mut self) -> io::Result<()> {
        if !self.x_files_factor.is_finite() || !(0.0..=1.0).contains(&self.x_files_factor) {
            return Err(invalid("invalid x_files_factor"));
        }
        validate(&mut self.retentions)
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Options {
    pub sparse: bool,
    pub flock: bool,
    pub compressed: bool,
    pub out_of_order: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimeSeries {
    pub from: i64,
    pub until: i64,
    pub step: u32,
    pub values: Vec<Option<f64>>,
}

#[derive(Clone, Debug)]
struct Archive {
    offset: u64,
    retention: Retention,
}
#[derive(Clone, Debug)]
struct BlockRange {
    start: i64,
    end: i64,
}
#[derive(Clone, Debug)]
struct CompressedArchive {
    block_size: usize,
    ranges: Vec<BlockRange>,
    buffer: Vec<u8>,
    current: usize,
}

/// OOO point counts since this handle was opened, including failed update attempts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OutOfOrderStats {
    /// Rejected by the compressed encoder, including points subsequently diverted.
    pub discarded: u64,
    /// Successfully written to the sidecar; compaction does not reset this total.
    pub diverted: u64,
}

pub struct Whisper {
    path: PathBuf,
    file: File,
    // Kept alive so a replacement during compaction retains the path lock.
    #[allow(dead_code)]
    lock_file: Option<File>,
    metadata: Metadata,
    archives: Vec<Archive>,
    compressed_archives: Option<Vec<CompressedArchive>>,
    options: Options,
    out_of_order_stats: OutOfOrderStats,
}

fn invalid(msg: &'static str) -> Error {
    Error::new(ErrorKind::InvalidData, msg)
}
fn unsupported(msg: &'static str) -> Error {
    Error::new(ErrorKind::Unsupported, msg)
}
pub fn out_of_order_sidecar_path(path: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(format!("{}.ooo", path.as_ref().display()))
}
fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes(b.try_into().unwrap())
}
fn be_f32(b: &[u8]) -> f32 {
    f32::from_bits(be_u32(b))
}
fn be_f64(b: &[u8]) -> f64 {
    f64::from_bits(u64::from_be_bytes(b.try_into().unwrap()))
}
fn floor(t: i64, step: u32) -> i64 {
    t.div_euclid(i64::from(step)) * i64::from(step)
}
fn interval(t: i64, step: u32) -> i64 {
    floor(t, step) + i64::from(step)
}

impl Whisper {
    pub fn create(
        path: impl AsRef<Path>,
        mut metadata: Metadata,
        options: Options,
    ) -> io::Result<Self> {
        if options.compressed || metadata.compressed {
            metadata.compressed = true;
            return Self::create_compressed(path, metadata, options);
        }
        metadata.validate()?;
        let path = path.as_ref().to_path_buf();
        let mut open = OpenOptions::new();
        open.read(true).write(true).create_new(true);
        let lock_file = path_lock(&path, options.flock)?;
        let file = open.open(&path)?;
        if options.flock {
            file.lock()?;
        }
        let archives = archive_layout(&metadata.retentions)?;
        let total = archives
            .last()
            .map(|a| a.offset + archive_size(a.retention))
            .unwrap_or(METADATA_SIZE);
        let mut header =
            vec![
                0;
                usize::try_from(METADATA_SIZE + ARCHIVE_INFO_SIZE * archives.len() as u64)
                    .map_err(|_| invalid("header too large"))?
            ];
        header[0..4].copy_from_slice(&(metadata.aggregation as u32).to_be_bytes());
        let max_retention = metadata
            .retentions
            .last()
            .unwrap()
            .seconds_per_point
            .checked_mul(metadata.retentions.last().unwrap().points)
            .ok_or_else(|| invalid("retention overflow"))?;
        header[4..8].copy_from_slice(&max_retention.to_be_bytes());
        header[8..12].copy_from_slice(&metadata.x_files_factor.to_bits().to_be_bytes());
        header[12..16].copy_from_slice(&(archives.len() as u32).to_be_bytes());
        for (i, a) in archives.iter().enumerate() {
            let n = 16 + i * 12;
            header[n..n + 4].copy_from_slice(
                &(u32::try_from(a.offset).map_err(|_| invalid("file too large"))?).to_be_bytes(),
            );
            header[n + 4..n + 8].copy_from_slice(&a.retention.seconds_per_point.to_be_bytes());
            header[n + 8..n + 12].copy_from_slice(&a.retention.points.to_be_bytes());
        }
        write_all_at(&file, &header, 0)?;
        if options.sparse {
            write_all_at(&file, &[0], total - 1)?;
        } else {
            allocate(&file, total)?;
        }
        Ok(Self {
            path,
            file,
            lock_file,
            metadata,
            archives,
            compressed_archives: None,
            options,
            out_of_order_stats: OutOfOrderStats::default(),
        })
    }

    pub fn open(path: impl AsRef<Path>, options: Options) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let lock_file = path_lock(&path, options.flock)?;
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        if options.flock {
            file.lock()?;
        }
        let mut prefix = [0; 18];
        read_exact_at(&file, &mut prefix, 0)?;
        if prefix == COMPRESSED_MAGIC {
            return Self::open_compressed(path, file, lock_file, options);
        }
        let mut header = [0; 16];
        read_exact_at(&file, &mut header, 0)?;
        let raw_aggregation = be_u32(&header[0..4]);
        // Pre-1.1 Whisper starts with lastUpdate; it implies average.
        let aggregation = if raw_aggregation > 1024 {
            Aggregation::Average
        } else {
            Aggregation::from_disk(raw_aggregation)?
        };
        let x_files_factor = be_f32(&header[8..12]);
        if !x_files_factor.is_finite() || !(0.0..=1.0).contains(&x_files_factor) {
            return Err(invalid("invalid x_files_factor"));
        }
        let count =
            usize::try_from(be_u32(&header[12..16])).map_err(|_| invalid("archive count"))?;
        if count == 0 || count > 4096 || 16 + count as u64 * 12 > file.metadata()?.len() {
            return Err(invalid("invalid archive count"));
        }
        let mut raw = vec![
            0;
            count
                .checked_mul(12)
                .ok_or_else(|| invalid("header overflow"))?
        ];
        read_exact_at(&file, &mut raw, 16)?;
        let mut archives = Vec::with_capacity(count);
        for i in 0..count {
            let p = &raw[i * 12..i * 12 + 12];
            archives.push(Archive {
                offset: u64::from(be_u32(&p[..4])),
                retention: Retention {
                    seconds_per_point: be_u32(&p[4..8]),
                    points: be_u32(&p[8..12]),
                },
            });
        }
        let original_rets = archives.iter().map(|a| a.retention).collect::<Vec<_>>();
        let mut rets = original_rets.clone();
        validate(&mut rets)?;
        if rets != original_rets {
            return Err(invalid("archives are not ordered"));
        }
        let len = file.metadata()?.len();
        let mut previous_end = METADATA_SIZE + count as u64 * ARCHIVE_INFO_SIZE;
        for a in &archives {
            if a.offset < previous_end
                || a.offset
                    .checked_add(archive_size(a.retention))
                    .filter(|n| *n <= len)
                    .is_none()
            {
                return Err(invalid("archive exceeds file"));
            }
            previous_end = a.offset + archive_size(a.retention);
        }
        Ok(Self {
            path,
            file,
            lock_file,
            metadata: Metadata {
                aggregation,
                x_files_factor,
                retentions: rets,
                compressed: false,
            },
            archives,
            compressed_archives: None,
            options,
            out_of_order_stats: OutOfOrderStats::default(),
        })
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Snapshot without resetting counters; ordinary and expired points do not count.
    pub fn out_of_order_stats(&self) -> OutOfOrderStats {
        self.out_of_order_stats
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn create_compressed(
        path: impl AsRef<Path>,
        mut metadata: Metadata,
        options: Options,
    ) -> io::Result<Self> {
        metadata.validate()?;
        let path = path.as_ref().to_path_buf();
        let lock_file = path_lock(&path, options.flock)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        if options.flock {
            file.lock()?;
        }
        let mut blocks = Vec::new();
        let mut block_sizes = Vec::new();
        for r in &metadata.retentions {
            let ppb = if r.points >= 21_600 {
                7200
            } else if r.points > 256 {
                (r.points + 16) / 4
            } else {
                r.points + 16
            };
            blocks.push(usize::try_from(r.points.div_ceil(ppb) + 1).unwrap());
            block_sizes.push(usize::try_from(ppb).unwrap() * 2 + 5);
        }
        let ranges_len: usize = blocks
            .iter()
            .try_fold(0usize, |n, b| n.checked_add(b.checked_mul(16)?))
            .ok_or_else(|| invalid("compressed header too large"))?;
        let buffers_len: usize = metadata
            .retentions
            .windows(2)
            .try_fold(0usize, |n, pair| {
                n.checked_add(
                    usize::try_from(pair[1].seconds_per_point / pair[0].seconds_per_point)
                        .unwrap()
                        .checked_mul(24)?,
                )
            })
            .ok_or_else(|| invalid("compressed header too large"))?;
        let header_len = 63usize
            .checked_add(
                metadata
                    .retentions
                    .len()
                    .checked_mul(128)
                    .ok_or_else(|| invalid("compressed header too large"))?,
            )
            .and_then(|n| n.checked_add(ranges_len))
            .and_then(|n| n.checked_add(buffers_len))
            .ok_or_else(|| invalid("compressed header too large"))?;
        let mut offsets = Vec::new();
        let mut offset = header_len as u64;
        for (i, r) in metadata.retentions.iter().enumerate() {
            offsets.push(offset);
            offset = offset
                .checked_add(
                    (block_sizes[i] as u64)
                        .checked_mul(blocks[i] as u64)
                        .ok_or_else(|| invalid("compressed file too large"))?,
                )
                .ok_or_else(|| invalid("compressed file too large"))?;
            let _ = r;
        }
        let mut h = vec![0; header_len];
        h[..18].copy_from_slice(COMPRESSED_MAGIC);
        h[18] = 1;
        h[19..23].copy_from_slice(&(metadata.aggregation as u32).to_be_bytes());
        let max = metadata
            .retentions
            .last()
            .unwrap()
            .seconds_per_point
            .checked_mul(metadata.retentions.last().unwrap().points)
            .ok_or_else(|| invalid("retention overflow"))?;
        h[23..27].copy_from_slice(&max.to_be_bytes());
        h[27..31].copy_from_slice(&metadata.x_files_factor.to_bits().to_be_bytes());
        h[31..35].copy_from_slice(&7200u32.to_be_bytes());
        h[35..39].copy_from_slice(&(metadata.retentions.len() as u32).to_be_bytes());
        h[39..43].copy_from_slice(&2f32.to_bits().to_be_bytes());
        for (i, r) in metadata.retentions.iter().enumerate() {
            let p = 63 + i * 128;
            h[p..p + 4].copy_from_slice(&(offsets[i] as u32).to_be_bytes());
            h[p + 4..p + 8].copy_from_slice(&r.seconds_per_point.to_be_bytes());
            h[p + 8..p + 12].copy_from_slice(&r.points.to_be_bytes());
            h[p + 12..p + 16].copy_from_slice(&(block_sizes[i] as u32).to_be_bytes());
            h[p + 16..p + 20].copy_from_slice(&(blocks[i] as u32).to_be_bytes());
            h[p + 20..p + 24].copy_from_slice(&2f32.to_bits().to_be_bytes());
            h[p + 68..p + 72].copy_from_slice(&(offsets[i] as u32).to_be_bytes());
            h[p + 72..p + 76].copy_from_slice(&7u32.to_be_bytes());
        }
        let crc = crc32fast::hash(&h);
        h[43..47].copy_from_slice(&crc.to_be_bytes());
        write_all_at(&file, &h, 0)?;
        allocate(&file, offset)?;
        Self::open_compressed(path, file, lock_file, options)
    }

    pub fn update_many(&mut self, points: &[Point], now: i64) -> io::Result<()> {
        if self.metadata.compressed {
            return self.update_compressed(points, now);
        }
        let mut grouped = self.group_points(points, now);
        for (i, values) in grouped.iter_mut().enumerate() {
            for point in values.iter_mut() {
                point.timestamp = floor(
                    point.timestamp,
                    self.archives[i].retention.seconds_per_point,
                );
            }
            values.sort_by_key(|p| p.timestamp);
            let mut unique = Vec::with_capacity(values.len());
            for point in values.iter().rev() {
                if unique.last().map(|last: &Point| last.timestamp) != Some(point.timestamp) {
                    unique.push(*point);
                }
            }
            unique.reverse();
            *values = unique;
            for &point in values.iter() {
                self.write_point(i, point)?;
            }
            for &point in values.iter() {
                self.propagate_from(i, point.timestamp)?;
            }
        }
        Ok(())
    }

    fn group_points(&self, points: &[Point], now: i64) -> Vec<Vec<Point>> {
        let mut remaining = points.iter().rev().copied().collect::<Vec<_>>();
        remaining.sort_by_key(|p| std::cmp::Reverse(p.timestamp));
        self.archives
            .iter()
            .map(|a| {
                let oldest = now.saturating_sub(
                    a.retention.seconds_per_point as i64 * a.retention.points as i64,
                );
                // Preserve the pinned Go extractPoints boundary: when an older point is
                // present, the immediately preceding point is routed to the next archive too.
                let end = remaining
                    .iter()
                    .position(|p| p.timestamp < oldest)
                    .map(|i| i.saturating_sub(1))
                    .unwrap_or(remaining.len());
                let mut current = remaining.drain(..end).collect::<Vec<_>>();
                current.reverse();
                current
            })
            .collect()
    }

    pub fn fetch(
        &mut self,
        mut from: i64,
        mut until: i64,
        now: i64,
    ) -> io::Result<Option<TimeSeries>> {
        if from > until {
            return Err(invalid("from is after until"));
        }
        let oldest = now - self.max_retention();
        if from > now || until < oldest {
            return Ok(None);
        }
        from = from.max(oldest);
        until = until.min(now);
        let age = now - from;
        let index = self
            .archives
            .iter()
            .position(|a| {
                age <= i64::from(a.retention.seconds_per_point) * i64::from(a.retention.points)
            })
            .unwrap_or(self.archives.len() - 1);
        let a = &self.archives[index];
        let step = a.retention.seconds_per_point;
        let start = interval(from, step);
        let mut end = interval(until, step);
        if self.metadata.compressed {
            return self.fetch_compressed(index, start, end);
        }
        if start == end {
            let mut base = [0; 4];
            read_exact_at(&self.file, &mut base, a.offset)?;
            if be_u32(&base) != 0 {
                end += step as i64;
            }
        }
        let count = usize::try_from((end - start) / i64::from(step))
            .map_err(|_| invalid("fetch range too large"))?;
        let mut values = Vec::with_capacity(count);
        for i in 0..count {
            values.push(
                self.read_point(index, start + i as i64 * i64::from(step))?
                    .map(|p| p.value),
            );
        }
        Ok(Some(TimeSeries {
            from: start,
            until: end,
            step,
            values,
        }))
    }

    pub fn compact_out_of_order(&mut self, now: i64) -> io::Result<()> {
        if self.metadata.compressed {
            return self.compact_compressed(now);
        }
        Ok(())
    }

    fn open_compressed(
        path: PathBuf,
        file: File,
        lock_file: Option<File>,
        options: Options,
    ) -> io::Result<Self> {
        let len = file.metadata()?.len();
        let mut fixed = [0; 45];
        read_exact_at(&file, &mut fixed, 18)?;
        if fixed[0] != 1 {
            return Err(unsupported("unsupported compressed Whisper version"));
        }
        let aggregation = Aggregation::from_disk(be_u32(&fixed[1..5]))?;
        let x_files_factor = be_f32(&fixed[9..13]);
        let count =
            usize::try_from(be_u32(&fixed[17..21])).map_err(|_| invalid("archive count"))?;
        if count == 0
            || count > 4096
            || 63 + count as u64 * 128 > len
            || !x_files_factor.is_finite()
            || !(0.0..=1.0).contains(&x_files_factor)
        {
            return Err(invalid("invalid compressed header"));
        }
        let mut raw = vec![
            0;
            count
                .checked_mul(128)
                .ok_or_else(|| invalid("header overflow"))?
        ];
        read_exact_at(&file, &mut raw, 63)?;
        let mut archives = Vec::with_capacity(count);
        let mut compressed = Vec::with_capacity(count);
        let mut block_counts = Vec::with_capacity(count);
        for i in 0..count {
            let b = &raw[i * 128..i * 128 + 128];
            let offset = u64::from(be_u32(&b[0..4]));
            let retention = Retention {
                seconds_per_point: be_u32(&b[4..8]),
                points: be_u32(&b[8..12]),
            };
            if retention.seconds_per_point == 0 || retention.points == 0 {
                return Err(invalid("zero compressed retention"));
            }
            let block_size =
                usize::try_from(be_u32(&b[12..16])).map_err(|_| invalid("block size"))?;
            let block_count =
                usize::try_from(be_u32(&b[16..20])).map_err(|_| invalid("block count"))?;
            let current = be_u32(&b[24..28]) as usize;
            if offset >= len
                || block_size < 17
                || block_count == 0
                || current >= block_count
                || offset
                    .checked_add(
                        (block_size as u64)
                            .checked_mul(block_count as u64)
                            .ok_or_else(|| invalid("block range overflow"))?,
                    )
                    .filter(|n| *n <= len)
                    .is_none()
            {
                return Err(invalid("compressed archive exceeds file"));
            }
            archives.push(Archive { offset, retention });
            compressed.push(CompressedArchive {
                block_size,
                ranges: Vec::with_capacity(block_count),
                buffer: Vec::new(),
                current,
            });
            block_counts.push(block_count);
        }
        let mut at = 63u64 + raw.len() as u64;
        for i in 0..count {
            let mut ranges = vec![
                0;
                block_counts[i]
                    .checked_mul(16)
                    .ok_or_else(|| invalid("block ranges overflow"))?
            ];
            read_exact_at(&file, &mut ranges, at)?;
            at += ranges.len() as u64;
            for b in ranges.as_chunks::<16>().0 {
                compressed[i].ranges.push(BlockRange {
                    start: i64::from(be_u32(&b[0..4])),
                    end: i64::from(be_u32(&b[4..8])),
                });
            }
            let buffer = if i + 1 < count {
                usize::try_from(
                    archives[i + 1].retention.seconds_per_point
                        / archives[i].retention.seconds_per_point,
                )
                .unwrap_or(0)
                .checked_mul(24)
                .ok_or_else(|| invalid("buffer overflow"))?
            } else {
                0
            };
            if at
                .checked_add(buffer as u64)
                .filter(|n| *n <= len)
                .is_none()
            {
                return Err(invalid("compressed buffer exceeds file"));
            }
            compressed[i].buffer = vec![0; buffer];
            read_exact_at(&file, &mut compressed[i].buffer, at)?;
            at += buffer as u64;
        }
        let original_rets = archives.iter().map(|a| a.retention).collect::<Vec<_>>();
        let mut rets = original_rets.clone();
        validate(&mut rets)?;
        if rets != original_rets {
            return Err(invalid("compressed archives are not ordered"));
        }
        let mut previous_end = at;
        for (archive, compressed) in archives.iter().zip(&compressed) {
            if archive.offset < previous_end {
                return Err(invalid("overlapping compressed archives"));
            }
            previous_end =
                archive.offset + compressed.block_size as u64 * compressed.ranges.len() as u64;
        }
        if at > 64 * 1024 * 1024 {
            return Err(invalid("compressed header exceeds safety limit"));
        }
        let mut header = vec![0; at as usize];
        read_exact_at(&file, &mut header, 0)?;
        let expected_crc = be_u32(&header[43..47]);
        header[43..47].fill(0);
        if crc32fast::hash(&header) != expected_crc {
            return Err(invalid("compressed header CRC mismatch"));
        }
        Ok(Self {
            path,
            file,
            lock_file,
            metadata: Metadata {
                aggregation,
                x_files_factor,
                retentions: rets,
                compressed: true,
            },
            archives,
            compressed_archives: Some(compressed),
            options,
            out_of_order_stats: OutOfOrderStats::default(),
        })
    }

    fn fetch_compressed(
        &mut self,
        index: usize,
        start: i64,
        end: i64,
    ) -> io::Result<Option<TimeSeries>> {
        let a = &self.archives[index];
        let c = &self.compressed_archives.as_ref().unwrap()[index];
        let count = usize::try_from((end - start) / i64::from(a.retention.seconds_per_point))
            .map_err(|_| invalid("fetch range too large"))?;
        let mut values = vec![None; count];
        for (block, range) in c.ranges.iter().enumerate() {
            if range.start == 0 || range.end < start || range.start > end {
                continue;
            }
            let mut raw = vec![0; c.block_size];
            read_exact_at(
                &self.file,
                &mut raw,
                a.offset + block as u64 * c.block_size as u64,
            )?;
            for p in decode_block(&raw, a.retention.seconds_per_point)? {
                if p.timestamp >= start && p.timestamp < end {
                    let n = usize::try_from(
                        (p.timestamp - start) / i64::from(a.retention.seconds_per_point),
                    )
                    .unwrap_or(count);
                    if n < count {
                        values[n] = Some(p.value);
                    }
                }
            }
        }
        for p in self.compressed_live_points(index)? {
            if p.timestamp >= start && p.timestamp < end {
                values
                    [((p.timestamp - start) / i64::from(a.retention.seconds_per_point)) as usize] =
                    Some(p.value);
            }
        }
        // A present sidecar is classic Whisper. Main values remain authoritative.
        let sidecar_path = out_of_order_sidecar_path(&self.path);
        if sidecar_path.exists() {
            let sidecar = Whisper::open(
                &sidecar_path,
                Options {
                    flock: self.options.flock,
                    ..Options::default()
                },
            )?;
            if sidecar.metadata.retentions != self.metadata.retentions {
                return Err(invalid("incompatible out-of-order sidecar"));
            }
            for (n, dst) in values.iter_mut().enumerate() {
                if dst.is_none() {
                    *dst = sidecar
                        .read_point(
                            index,
                            start + n as i64 * a.retention.seconds_per_point as i64,
                        )?
                        .map(|p| p.value);
                }
            }
        }
        Ok(Some(TimeSeries {
            from: start,
            until: end,
            step: a.retention.seconds_per_point,
            values,
        }))
    }

    fn max_retention(&self) -> i64 {
        let a = self.archives.last().unwrap().retention;
        i64::from(a.seconds_per_point) * i64::from(a.points)
    }
    fn point_offset(&self, index: usize, timestamp: i64) -> io::Result<u64> {
        let a = &self.archives[index];
        let mut b = [0; 4];
        read_exact_at(&self.file, &mut b, a.offset)?;
        let base = i64::from(be_u32(&b));
        if base == 0 {
            return Ok(a.offset);
        }
        let slots = i64::from(a.retention.points);
        let distance = (timestamp - base).div_euclid(i64::from(a.retention.seconds_per_point));
        let slot = distance.rem_euclid(slots) as u64;
        Ok(a.offset + slot * POINT_SIZE)
    }
    fn write_point(&self, index: usize, point: Point) -> io::Result<()> {
        let timestamp = u32::try_from(point.timestamp)
            .map_err(|_| invalid("timestamp outside Whisper u32 range"))?;
        let offset = self.point_offset(index, point.timestamp)?;
        let mut b = [0; 12];
        b[..4].copy_from_slice(&timestamp.to_be_bytes());
        b[4..].copy_from_slice(&point.value.to_bits().to_be_bytes());
        write_all_at(&self.file, &b, offset)
    }
    fn read_point(&self, index: usize, timestamp: i64) -> io::Result<Option<Point>> {
        let offset = self.point_offset(index, timestamp)?;
        let mut b = [0; 12];
        read_exact_at(&self.file, &mut b, offset)?;
        let actual = i64::from(be_u32(&b[..4]));
        Ok(
            (actual == timestamp && !be_f64(&b[4..]).is_nan()).then(|| Point {
                timestamp: actual,
                value: be_f64(&b[4..]),
            }),
        )
    }
    fn propagate_from(&self, mut high: usize, timestamp: i64) -> io::Result<()> {
        while high + 1 < self.archives.len() {
            let low = high + 1;
            let lower = self.archives[low].retention;
            let start = floor(timestamp, lower.seconds_per_point);
            let higher = self.archives[high].retention;
            let mut values = Vec::new();
            let slots = lower.seconds_per_point / higher.seconds_per_point;
            for n in 0..slots {
                if let Some(p) =
                    self.read_point(high, start + i64::from(n * higher.seconds_per_point))?
                {
                    values.push(p.value);
                }
            }
            if values.is_empty()
                || values.len() as f32 / (slots as f32) < self.metadata.x_files_factor
            {
                break;
            }
            self.write_point(
                low,
                Point {
                    timestamp: start,
                    value: aggregate(self.metadata.aggregation, &values),
                },
            )?;
            high = low;
        }
        Ok(())
    }
}

fn archive_size(r: Retention) -> u64 {
    u64::from(r.points) * POINT_SIZE
}
fn archive_layout(rets: &[Retention]) -> io::Result<Vec<Archive>> {
    let mut offset = METADATA_SIZE
        .checked_add(
            ARCHIVE_INFO_SIZE
                .checked_mul(rets.len() as u64)
                .ok_or_else(|| invalid("header overflow"))?,
        )
        .ok_or_else(|| invalid("header overflow"))?;
    let mut out = Vec::with_capacity(rets.len());
    for &retention in rets {
        out.push(Archive { offset, retention });
        offset = offset
            .checked_add(archive_size(retention))
            .ok_or_else(|| invalid("file too large"))?;
    }
    Ok(out)
}
fn validate(rets: &mut [Retention]) -> io::Result<()> {
    rets.sort_by_key(|r| r.seconds_per_point);
    if rets.is_empty() {
        return Err(invalid("no retentions"));
    }
    for (i, r) in rets.iter().enumerate() {
        if r.seconds_per_point == 0 || r.points == 0 {
            return Err(invalid("zero retention"));
        }
        if r.seconds_per_point.checked_mul(r.points).is_none() {
            return Err(invalid("retention exceeds format limit"));
        }
        if i > 0 {
            let p = rets[i - 1];
            if r.seconds_per_point <= p.seconds_per_point
                || r.seconds_per_point % p.seconds_per_point != 0
                || u64::from(r.seconds_per_point) * u64::from(r.points)
                    <= u64::from(p.seconds_per_point) * u64::from(p.points)
                || p.points < r.seconds_per_point / p.seconds_per_point
            {
                return Err(invalid("invalid retention sequence"));
            }
        }
    }
    Ok(())
}
fn aggregate(method: Aggregation, values: &[f64]) -> f64 {
    match method {
        Aggregation::Average => values.iter().sum::<f64>() / values.len() as f64,
        Aggregation::Sum => values.iter().sum(),
        Aggregation::Last => *values.last().unwrap(),
        Aggregation::First => values[0],
        Aggregation::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        Aggregation::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
    }
}
fn allocate(file: &File, size: u64) -> io::Result<()> {
    let mut at = file.metadata()?.len();
    let zero = [0; 16_384];
    while at < size {
        let n = usize::try_from((size - at).min(zero.len() as u64)).unwrap();
        write_all_at(file, &zero[..n], at)?;
        at += n as u64;
    }
    Ok(())
}
fn path_lock(path: &Path, enabled: bool) -> io::Result<Option<File>> {
    if !enabled {
        return Ok(None);
    }
    let mut name = path.as_os_str().to_os_string();
    name.push(".lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(PathBuf::from(name))?;
    lock.lock()?;
    Ok(Some(lock))
}
fn read_exact_at(file: &File, b: &mut [u8], offset: u64) -> io::Result<()> {
    file.read_exact_at(b, offset)
}
fn write_all_at(file: &File, b: &[u8], offset: u64) -> io::Result<()> {
    file.write_all_at(b, offset)
}

struct Bits<'a> {
    b: &'a [u8],
    byte: usize,
    bit: i8,
}

struct BitWriter {
    bytes: Vec<u8>,
    index: usize,
    bit: i8,
}
impl BitWriter {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: vec![0; capacity],
            index: 0,
            bit: 7,
        }
    }
    // This deliberately follows go-whisper's little-endian staging plus
    // most-significant-bit emission, rather than a conventional bit stream.
    fn write(&mut self, mut width: usize, mut data: u64) -> io::Result<()> {
        if width > 64 {
            return Err(invalid("invalid compressed bit width"));
        }
        while width > 0 {
            let chunk = width.min(8);
            for n in (0..chunk).rev() {
                if self.index >= self.bytes.len() {
                    return Err(Error::new(ErrorKind::WriteZero, "compressed block full"));
                }
                self.bytes[self.index] |= (((data >> n) & 1) as u8) << self.bit;
                self.bit -= 1;
                if self.bit < 0 {
                    self.index += 1;
                    self.bit = 7;
                }
            }
            data >>= chunk;
            width -= chunk;
        }
        Ok(())
    }
}

struct EncodedBlock {
    bytes: Vec<u8>,
    last_byte: u8,
    last_offset: usize,
    bit: i8,
    crc: u32,
    /// Bytes holding data plus the end-of-block marker; the rest is padding.
    used: usize,
}
/// Resume state of an archive's current block, as Go stores it in the header.
struct BlockTail {
    p1: Point,
    p2: Point,
    last_byte: u8,
    /// Absolute file offset of the partially written last byte.
    offset: u64,
    bit: i8,
    count: u32,
    crc: u32,
}
#[cfg(test)]
fn encode_block(points: &[Point], step: u32, capacity: usize) -> io::Result<Vec<u8>> {
    Ok(encode_block_state(points, step, capacity)?.bytes)
}
fn encode_block_state(points: &[Point], step: u32, capacity: usize) -> io::Result<EncodedBlock> {
    if points.is_empty() {
        return Ok(EncodedBlock {
            bytes: vec![0; capacity],
            last_byte: 0,
            last_offset: 0,
            bit: 7,
            crc: 0,
            used: 0,
        });
    }
    if capacity < 17 {
        return Err(invalid("compressed block too short"));
    }
    let mut out = BitWriter::new(capacity);
    out.bytes[..4].copy_from_slice(
        &u32::try_from(points[0].timestamp)
            .map_err(|_| invalid("timestamp outside Whisper u32 range"))?
            .to_be_bytes(),
    );
    out.bytes[4..12].copy_from_slice(&points[0].value.to_bits().to_be_bytes());
    out.index = 12;
    encode_points(&mut out, points[0], points[0], &points[1..], step)?;
    finish_block(out, 0)
}
/// Continues a block from its stored tail, encoding only `points` (all newer than `tail.p1`)
/// into a buffer that starts at the tail byte and covers the `remaining` bytes of the block.
/// `WriteZero` means the block is full and the caller must rotate.
fn append_block_state(
    tail: &BlockTail,
    points: &[Point],
    step: u32,
    remaining: usize,
) -> io::Result<EncodedBlock> {
    if remaining == 0 || points.is_empty() {
        return Err(Error::new(ErrorKind::WriteZero, "compressed block full"));
    }
    let mut out = BitWriter::new(remaining);
    // Bits at and below the resume position hold the old end-of-block marker.
    out.bytes[0] = tail.last_byte & !(((1u16 << (tail.bit + 1)) - 1) as u8);
    out.bit = tail.bit;
    encode_points(&mut out, tail.p2, tail.p1, points, step)?;
    finish_block(out, tail.crc)
}
fn finish_block(mut out: BitWriter, crc_seed: u32) -> io::Result<EncodedBlock> {
    let last_offset = out.index;
    let bit = out.bit;
    let last_byte = out.bytes[last_offset];
    let mut hasher = crc32fast::Hasher::new_with_initial(crc_seed);
    hasher.update(&out.bytes[..last_offset]);
    let crc = hasher.finalize();
    out.write(4, 15)?;
    out.write(32, 0)?;
    let used = (out.index + 1).min(out.bytes.len());
    Ok(EncodedBlock {
        bytes: out.bytes,
        last_byte,
        last_offset,
        bit,
        crc,
        used,
    })
}
fn encode_points(
    out: &mut BitWriter,
    mut p2: Point,
    mut p1: Point,
    points: &[Point],
    step: u32,
) -> io::Result<()> {
    let capacity = out.bytes.len();
    for &p in points {
        if p.timestamp <= p1.timestamp {
            return Err(invalid("compressed points must be ascending"));
        }
        let d = ((p.timestamp - p1.timestamp) - (p1.timestamp - p2.timestamp)) / i64::from(step);
        if d == 0 {
            out.write(1, 0)?;
        } else if (-63..64).contains(&d) {
            out.write(2, 2)?;
            out.write(7, if d < 0 { (-d as u64) | 64 } else { d as u64 })?;
        } else if (-255..256).contains(&d) {
            out.write(3, 6)?;
            out.write(9, if d < 0 { (-d as u64) | 256 } else { d as u64 })?;
        } else if (-2047..2048).contains(&d) {
            out.write(4, 14)?;
            out.write(12, if d < 0 { (-d as u64) | 2048 } else { d as u64 })?;
        } else {
            out.write(4, 15)?;
            out.write(32, p.timestamp as u64)?;
        }
        let xor = p1.value.to_bits() ^ p.value.to_bits();
        let previous = p1.value.to_bits() ^ p2.value.to_bits();
        if xor == 0 {
            out.write(1, 0)?;
        } else {
            let lz = xor.leading_zeros() as usize;
            let tz = xor.trailing_zeros() as usize;
            let plz = previous.leading_zeros() as usize;
            let ptz = previous.trailing_zeros() as usize;
            if previous != 0 && plz <= lz && ptz <= tz {
                out.write(2, 2)?;
                out.write(64 - plz - ptz, xor >> ptz)?;
            } else {
                let lz = lz.min(31);
                let meaningful = 64 - lz - tz;
                let (stored, bits, payload) = if meaningful >= 63 {
                    (63, 64, xor)
                } else {
                    (meaningful, meaningful, xor >> tz)
                };
                out.write(2, 3)?;
                out.write(5, lz as u64)?;
                out.write(6, stored as u64)?;
                out.write(bits, payload)?;
            }
        }
        if out.index + 5 >= capacity {
            return Err(Error::new(ErrorKind::WriteZero, "compressed block full"));
        }
        p2 = p1;
        p1 = p;
    }
    Ok(())
}
impl<'a> Bits<'a> {
    fn read(&mut self, n: usize) -> io::Result<u64> {
        if n > 64 {
            return Err(invalid("invalid compressed bit width"));
        }
        let mut data = 0;
        for _ in 0..n {
            if self.byte >= self.b.len() {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "compressed block ends early",
                ));
            }
            data = (data << 1) | u64::from((self.b[self.byte] >> self.bit) & 1);
            self.bit -= 1;
            if self.bit < 0 {
                self.byte += 1;
                self.bit = 7;
            }
        }
        let mut result = 0;
        for i in (8..=64).step_by(8) {
            if n < i {
                let rem = n % 8;
                result |= (data & ((1u64 << rem) - 1)) << (i - 8);
                break;
            }
            result |= ((data >> (n - i)) & 0xff) << (i - 8);
        }
        Ok(result)
    }
    fn peek(&self, n: usize) -> io::Result<u64> {
        let mut copy = Self {
            b: self.b,
            byte: self.byte,
            bit: self.bit,
        };
        copy.read(n)
    }
}
fn decode_block(b: &[u8], step: u32) -> io::Result<Vec<Point>> {
    if b.len() < 12 {
        return Err(invalid("compressed block too short"));
    }
    let first = Point {
        timestamp: i64::from(be_u32(&b[0..4])),
        value: be_f64(&b[4..12]),
    };
    if first.timestamp == 0 {
        return Ok(Vec::new());
    }
    let mut out = vec![first];
    let mut bits = Bits {
        b,
        byte: 12,
        bit: 7,
    };
    let mut previous = first;
    let mut before = first;
    loop {
        if bits.byte >= b.len() {
            break;
        }
        let prefix = bits.peek(4)?;
        let (skip, width) = if bits.peek(1)? == 0 {
            (0, 1)
        } else if bits.peek(2)? == 2 {
            (2, 7)
        } else if bits.peek(3)? == 6 {
            (3, 9)
        } else if prefix == 14 {
            (4, 12)
        } else if prefix == 15 {
            (4, 32)
        } else {
            return Err(invalid("invalid compressed timestamp prefix"));
        };
        bits.read(skip)?;
        let raw = bits.read(width)?;
        if width == 32 && raw == 0 {
            break;
        }
        let timestamp = if width == 32 {
            i64::try_from(raw).map_err(|_| invalid("timestamp overflow"))?
        } else {
            let mut d = raw as i64;
            if skip > 0 && (raw & (1 << (width - 1))) != 0 {
                d = -(raw as i64 & ((1 << (width - 1)) - 1));
            }
            2 * previous.timestamp + d * i64::from(step) - before.timestamp
        };
        let value = if bits.peek(1)? == 0 {
            bits.read(1)?;
            previous.value.to_bits()
        } else if bits.peek(2)? == 2 {
            bits.read(2)?;
            let xor = previous.value.to_bits() ^ before.value.to_bits();
            let lz = xor.leading_zeros() as usize;
            let tz = xor.trailing_zeros() as usize;
            let significant = 64usize.saturating_sub(lz + tz);
            if significant == 0 || tz >= 64 {
                return Err(invalid("invalid reused XOR window"));
            }
            previous.value.to_bits() ^ (bits.read(significant)? << tz)
        } else if bits.peek(2)? == 3 {
            bits.read(2)?;
            let lz = bits.read(5)? as usize;
            let m = bits.read(6)? as usize;
            let n = if m == 63 { 64 } else { m };
            let mut xor = bits.read(n)?;
            if m < 63 {
                xor <<= 64usize
                    .checked_sub(lz + m)
                    .ok_or_else(|| invalid("invalid compressed value width"))?;
            }
            previous.value.to_bits() ^ xor
        } else {
            return Err(invalid("invalid compressed value prefix"));
        };
        if timestamp <= previous.timestamp || timestamp > u32::MAX as i64 {
            return Err(invalid("invalid compressed timestamp ordering"));
        }
        let point = Point {
            timestamp,
            value: f64::from_bits(value),
        };
        before = previous;
        previous = point;
        out.push(point);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn metadata() -> Metadata {
        Metadata {
            aggregation: Aggregation::Average,
            x_files_factor: 0.5,
            retentions: vec![
                Retention {
                    seconds_per_point: 1,
                    points: 60,
                },
                Retention {
                    seconds_per_point: 10,
                    points: 60,
                },
            ],
            compressed: false,
        }
    }
    #[test]
    fn classic_roundtrip_and_propagation() {
        let path = std::env::temp_dir().join(format!("whisper-rs-{}", std::process::id()));
        let mut w = Whisper::create(&path, metadata(), Options::default()).unwrap();
        w.update_many(
            &[
                Point {
                    timestamp: 100,
                    value: 1.0,
                },
                Point {
                    timestamp: 101,
                    value: 3.0,
                },
                Point {
                    timestamp: 101,
                    value: 4.0,
                },
            ],
            110,
        )
        .unwrap();
        assert_eq!(
            w.fetch(100, 102, 110).unwrap().unwrap().values,
            vec![Some(4.0), None]
        );
        assert_eq!(w.fetch(100, 101, 161).unwrap().unwrap().step, 10);
        drop(w);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn corrupt_header_is_rejected() {
        let path = std::env::temp_dir().join(format!("whisper-rs-bad-{}", std::process::id()));
        std::fs::write(&path, [0; 16]).unwrap();
        assert!(Whisper::open(&path, Options::default()).is_err());
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn corrupted_archive_metadata_never_panics() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.wsp");
        let w = Whisper::create(
            &path,
            metadata(),
            Options {
                compressed: true,
                ..Options::default()
            },
        )
        .unwrap();
        drop(w);
        let original = std::fs::read(&path).unwrap();
        for offset in [19, 27, 35, 43, 63, 67, 71, 75, 79, 87] {
            for value in [0u32, u32::MAX] {
                let mut bytes = original.clone();
                bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
                std::fs::write(&path, bytes).unwrap();
                assert!(
                    std::panic::catch_unwind(|| Whisper::open(&path, Options::default())).is_ok()
                );
            }
        }
    }
    #[test]
    fn classic_same_interval_fetch_includes_one_slot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("same.wsp");
        let mut w = Whisper::create(path, metadata(), Options::default()).unwrap();
        assert!(w.fetch(100, 100, 110).unwrap().unwrap().values.is_empty());
        w.update_many(
            &[Point {
                timestamp: 101,
                value: 2.0,
            }],
            110,
        )
        .unwrap();
        let series = w.fetch(100, 100, 110).unwrap().unwrap();
        assert_eq!(
            (series.from, series.until, series.values),
            (101, 102, vec![Some(2.0)])
        );
    }
    #[test]
    fn flock_serializes_open_until_owner_drops() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("locked.wsp");
        let options = Options {
            flock: true,
            ..Options::default()
        };
        let owner = Whisper::create(&path, metadata(), options).unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let w = Whisper::open(path, options).unwrap();
            sender.send(()).unwrap();
            drop(w);
        });
        assert!(
            receiver
                .recv_timeout(std::time::Duration::from_millis(30))
                .is_err()
        );
        drop(owner);
        receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        reader.join().unwrap();
    }
    #[test]
    fn compressed_block_first_point() {
        let mut b = vec![0; 17];
        b[..4].copy_from_slice(&100u32.to_be_bytes());
        b[4..12].copy_from_slice(&2.5f64.to_bits().to_be_bytes());
        b[12] = 0xf0;
        assert_eq!(
            decode_block(&b, 1).unwrap(),
            vec![Point {
                timestamp: 100,
                value: 2.5
            }]
        );
    }
    #[test]
    fn compressed_empty_roundtrip() {
        let path = std::env::temp_dir().join(format!("whisper-rs-c-{}", std::process::id()));
        let mut w = Whisper::create(
            &path,
            metadata(),
            Options {
                compressed: true,
                ..Options::default()
            },
        )
        .unwrap();
        assert!(w.metadata().compressed);
        assert!(
            w.fetch(100, 101, 110)
                .unwrap()
                .unwrap()
                .values
                .iter()
                .all(Option::is_none)
        );
        drop(w);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn out_of_order_stats_count_rejections_and_survive_internal_reopens() {
        for compressed in [false, true] {
            for out_of_order in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("stats.wsp");
                let mut meta = metadata();
                meta.retentions.truncate(1);
                let options = Options {
                    compressed,
                    out_of_order,
                    ..Options::default()
                };
                let mut w = Whisper::create(&path, meta, options).unwrap();
                let point = |timestamp| Point {
                    timestamp,
                    value: 1.0,
                };
                w.update_many(&[point(100), point(101)], 110).unwrap();
                assert_eq!(w.out_of_order_stats(), OutOfOrderStats::default());
                w.update_many(&[point(1)], 110).unwrap();
                assert_eq!(w.out_of_order_stats(), OutOfOrderStats::default());
                // Same-batch duplicates are coalesced before compressed-encoder rejection.
                w.update_many(&[point(99), point(100), point(100)], 110)
                    .unwrap();
                let expected = OutOfOrderStats {
                    discarded: if compressed { 2 } else { 0 },
                    diverted: if compressed && out_of_order { 2 } else { 0 },
                };
                assert_eq!(w.out_of_order_stats(), expected);
                w.update_many(&[point(102)], 110).unwrap();
                assert_eq!(w.out_of_order_stats(), expected);
                if compressed && out_of_order {
                    assert!(out_of_order_sidecar_path(&path).exists());
                    // Compaction uses the same internal replacement path as block growth.
                    w.compact_out_of_order(110).unwrap();
                    assert!(!out_of_order_sidecar_path(&path).exists());
                    assert_eq!(w.out_of_order_stats(), expected);
                }
                drop(w);
                let w = Whisper::open(&path, options).unwrap();
                assert_eq!(w.out_of_order_stats(), OutOfOrderStats::default());
            }
        }
    }

    #[test]
    fn out_of_order_stats_keep_rejections_when_diversion_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stats.wsp");
        let mut w = Whisper::create(
            &path,
            metadata(),
            Options {
                compressed: true,
                out_of_order: true,
                ..Options::default()
            },
        )
        .unwrap();
        let points: Vec<_> = (80..=110)
            .map(|timestamp| Point {
                timestamp,
                value: 1.0,
            })
            .collect();
        w.update_many(&points, 110).unwrap();
        // Still-live buffer slots accept out-of-order updates without diversion.
        w.update_many(
            &[Point {
                timestamp: 108,
                value: 2.0,
            }],
            110,
        )
        .unwrap();
        assert_eq!(w.out_of_order_stats(), OutOfOrderStats::default());
        let sidecar = out_of_order_sidecar_path(&path);
        std::fs::create_dir(&sidecar).unwrap();
        let late = [Point {
            timestamp: 85,
            value: 3.0,
        }];
        assert!(w.update_many(&late, 110).is_err());
        assert_eq!(
            w.out_of_order_stats(),
            OutOfOrderStats {
                discarded: 1,
                diverted: 0
            }
        );
        std::fs::remove_dir(&sidecar).unwrap();
        w.update_many(&late, 110).unwrap();
        assert_eq!(
            w.out_of_order_stats(),
            OutOfOrderStats {
                discarded: 2,
                diverted: 1
            }
        );
    }

    #[test]
    fn compressed_block_encode_roundtrip() {
        let points = vec![
            Point {
                timestamp: 100,
                value: 1.0,
            },
            Point {
                timestamp: 101,
                value: 1.0,
            },
            Point {
                timestamp: 102,
                value: 2.5,
            },
            Point {
                timestamp: 104,
                value: -3.0,
            },
        ];
        assert_eq!(
            decode_block(&encode_block(&points, 1, 256).unwrap(), 1).unwrap(),
            points
        );
    }

    /// Appends resume from the header's block tail state instead of decoding the block.
    #[test]
    fn compressed_append_leaves_existing_block_body_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append.wsp");
        let mut meta = metadata();
        meta.compressed = true;
        meta.retentions.truncate(1);
        meta.retentions[0].points = 200;
        let options = Options {
            compressed: true,
            ..Options::default()
        };
        let mut w = Whisper::create(&path, meta, options).unwrap();
        let points: Vec<Point> = (0..150)
            .map(|i| Point {
                timestamp: 1000 + i,
                value: (i / 25) as f64,
            })
            .collect();
        w.update_many(&points, 1160).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut header = vec![0; 63 + 128];
        file.read_exact_at(&mut header, 0).unwrap();
        let body_start = u64::from(be_u32(&header[63..67])) + 12;
        let tail = u64::from(be_u32(&header[63 + 68..63 + 72]));
        let mut body = vec![0; (tail - body_start) as usize];
        assert!(body.len() > 16);
        file.read_exact_at(&mut body, body_start).unwrap();
        let scrambled: Vec<u8> = body.iter().map(|b| !b).collect();
        file.write_all_at(&scrambled, body_start).unwrap();
        w.update_many(
            &[Point {
                timestamp: 1150,
                value: 42.0,
            }],
            1160,
        )
        .unwrap();
        file.write_all_at(&body, body_start).unwrap();
        drop(w);
        let mut w = Whisper::open(&path, options).unwrap();
        let values: Vec<f64> = w
            .fetch(999, 1150, 1160)
            .unwrap()
            .unwrap()
            .values
            .into_iter()
            .flatten()
            .collect();
        let mut expected: Vec<f64> = points.iter().map(|p| p.value).collect();
        expected.push(42.0);
        assert_eq!(values, expected);
    }

    /// Incremental appends produce the same bytes as one write of all points.
    #[test]
    fn compressed_incremental_appends_match_single_batch_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut meta = metadata();
        meta.compressed = true;
        let options = Options {
            compressed: true,
            ..Options::default()
        };
        let points: Vec<Point> = (0..55)
            .map(|i| Point {
                timestamp: 1000 + i,
                value: (i % 3) as f64,
            })
            .collect();
        let single = dir.path().join("single.wsp");
        Whisper::create(&single, meta.clone(), options)
            .unwrap()
            .update_many(&points, 1055)
            .unwrap();
        let batched = dir.path().join("batched.wsp");
        let mut w = Whisper::create(&batched, meta, options).unwrap();
        let mut rest = &points[..];
        for n in [1, 2, 3, 5, 8, 13, 23] {
            let (head, tail) = rest.split_at(n);
            w.update_many(head, 1055).unwrap();
            rest = tail;
        }
        assert!(rest.is_empty());
        drop(w);
        assert_eq!(
            std::fs::read(&single).unwrap(),
            std::fs::read(&batched).unwrap()
        );
    }
}
