//! gzip file-list-cache compatibility with go-carbon's FLC v1 and v2.

use std::fs::{self, File};
use std::io::{self, Cursor, Read, Write};
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

const V2_MAGIC: &[u8] = b"\x7fflc02";
const MAX_PATH: usize = 8192;
const MAX_DECOMPRESSED_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Version {
    V1,
    V2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
    pub path: String,
    pub logical_size: u64,
    pub physical_size: u64,
    pub data_points: u64,
    pub first_seen_at: i64,
}

pub fn read(path: impl AsRef<Path>) -> io::Result<(Version, Vec<Entry>)> {
    let mut compressed = Vec::new();
    GzDecoder::new(File::open(path)?)
        .take(MAX_DECOMPRESSED_BYTES + 1)
        .read_to_end(&mut compressed)?;
    if compressed.len() as u64 > MAX_DECOMPRESSED_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "flc exceeds decompressed size limit",
        ));
    }
    if compressed.starts_with(V2_MAGIC) {
        read_v2(&compressed[V2_MAGIC.len()..]).map(|entries| (Version::V2, entries))
    } else {
        read_v1(&compressed).map(|entries| (Version::V1, entries))
    }
}

pub fn write(path: impl AsRef<Path>, version: Version, entries: &[Entry]) -> io::Result<()> {
    let path = path.as_ref();
    let mut raw = Vec::new();
    match version {
        Version::V1 => {
            for entry in entries {
                raw.extend_from_slice(entry.path.as_bytes());
                raw.push(b'\n');
            }
        }
        Version::V2 => {
            raw.extend_from_slice(V2_MAGIC);
            for entry in entries {
                let name = entry.path.as_bytes();
                if name.len() > MAX_PATH {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "flc path exceeds 8192 bytes",
                    ));
                }
                raw.extend_from_slice(&(name.len() as u64).to_be_bytes());
                raw.extend_from_slice(name);
                raw.extend_from_slice(&entry.logical_size.to_be_bytes());
                raw.extend_from_slice(&entry.physical_size.to_be_bytes());
                raw.extend_from_slice(&entry.data_points.to_be_bytes());
                raw.extend_from_slice(&(entry.first_seen_at as u64).to_be_bytes());
                raw.push(b'\n');
            }
        }
    }
    let mut tmp_name = path.as_os_str().to_os_string();
    tmp_name.push(".tmp");
    let tmp = PathBuf::from(tmp_name);
    let file = File::create(&tmp)?;
    let mut gzip = GzEncoder::new(file, Compression::default());
    gzip.write_all(&raw)?;
    let file = gzip.finish()?;
    file.sync_all()?;
    fs::rename(tmp, path)
}

fn read_v1(raw: &[u8]) -> io::Result<Vec<Entry>> {
    let text = std::str::from_utf8(raw)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "flc v1 is not utf-8"))?;
    Ok(text
        .lines()
        .map(|path| Entry {
            path: path.to_owned(),
            logical_size: 0,
            physical_size: 0,
            data_points: 0,
            first_seen_at: 0,
        })
        .collect())
}
fn read_v2(raw: &[u8]) -> io::Result<Vec<Entry>> {
    let mut input = Cursor::new(raw);
    let mut entries = Vec::new();
    while input.position() < raw.len() as u64 {
        let length = read_u64(&mut input)? as usize;
        if length > MAX_PATH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "flc v2 illegal path length",
            ));
        }
        let mut path = vec![0; length];
        input.read_exact(&mut path)?;
        let path = String::from_utf8(path)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "flc v2 path is not utf-8"))?;
        let logical_size = read_u64(&mut input)?;
        let physical_size = read_u64(&mut input)?;
        let data_points = read_u64(&mut input)?;
        let first_seen_at = read_u64(&mut input)? as i64;
        let mut separator = [0];
        input.read_exact(&mut separator)?;
        if separator[0] != b'\n' {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "flc v2 missing entry separator",
            ));
        }
        entries.push(Entry {
            path,
            logical_size,
            physical_size,
            data_points,
            first_seen_at,
        });
    }
    Ok(entries)
}
fn read_u64(input: &mut Cursor<&[u8]>) -> io::Result<u64> {
    let mut bytes = [0; 8];
    input.read_exact(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn v2_round_trip_and_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flc.gz");
        let entries = vec![Entry {
            path: "a/b.wsp".into(),
            logical_size: 1,
            physical_size: 2,
            data_points: 3,
            first_seen_at: -4,
        }];
        write(&path, Version::V2, &entries).unwrap();
        assert_eq!(read(&path).unwrap(), (Version::V2, entries));
        fs::write(&path, b"not gzip").unwrap();
        assert!(read(&path).is_err());
    }
}
