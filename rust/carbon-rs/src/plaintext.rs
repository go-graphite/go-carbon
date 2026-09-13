use std::fmt;
use whisper_rs::Point;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error;
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bad graphite plaintext message")
    }
}
impl std::error::Error for Error {}

/// Mirrors Go's PlainLine: trim only space, CR and LF; fields are separated by literal spaces.
pub fn parse_line(line: &[u8]) -> Result<(String, Point), Error> {
    let line = trim(line);
    let Some(first) = line.iter().position(|&b| b == b' ') else {
        return Err(Error);
    };
    if first == 0 {
        return Err(Error);
    }
    let rest = &line[first + 1..];
    let Some(second) = rest.iter().position(|&b| b == b' ') else {
        return Err(Error);
    };
    if second == 0 {
        return Err(Error);
    }
    let value = std::str::from_utf8(&rest[..second])
        .map_err(|_| Error)?
        .parse::<f64>()
        .map_err(|_| Error)?;
    let timestamp = std::str::from_utf8(&rest[second + 1..])
        .map_err(|_| Error)?
        .parse::<f64>()
        .map_err(|_| Error)?;
    if !value.is_finite()
        || !timestamp.is_finite()
        || timestamp < i64::MIN as f64
        || timestamp >= i64::MAX as f64
    {
        return Err(Error);
    }
    let metric = std::str::from_utf8(&line[..first])
        .map_err(|_| Error)?
        .to_owned();
    Ok((
        metric,
        Point {
            value,
            timestamp: timestamp as i64,
        },
    ))
}

pub fn parse_body(body: &[u8]) -> Result<Vec<(String, Point)>, Error> {
    let mut result = Vec::new();
    for line in body.split_inclusive(|&b| b == b'\n') {
        if line == b"\n" {
            continue;
        }
        if !line.ends_with(b"\n") {
            return Err(Error);
        }
        result.push(parse_line(line)?);
    }
    Ok(result)
}

fn trim(mut b: &[u8]) -> &[u8] {
    while matches!(b.first(), Some(b' ' | b'\n' | b'\r')) {
        b = &b[1..];
    }
    while matches!(b.last(), Some(b' ' | b'\n' | b'\r')) {
        b = &b[..b.len() - 1];
    }
    b
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn go_plainline_whitespace_and_fractional_timestamp() {
        assert_eq!(
            parse_line(b" \rmetric 1.5 42.9\n ").unwrap(),
            (
                "metric".into(),
                Point {
                    value: 1.5,
                    timestamp: 42
                }
            )
        );
    }
    #[test]
    fn rejects_nan_and_invalid_field_layout() {
        for line in [
            b"m NaN 1".as_slice(),
            b"m 1 NaN",
            b"m  1",
            b"m\t1 2",
            b"m 1 inf",
            b"m inf 1",
            b"m -Inf 1",
        ] {
            assert!(parse_line(line).is_err(), "{line:?}");
        }
    }
    #[test]
    fn body_requires_newline() {
        assert!(parse_body(b"a 1 1").is_err());
        assert_eq!(parse_body(b"\na 1 1\n").unwrap().len(), 1);
    }
}
