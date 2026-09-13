use carbon_rs::{
    app::{read_dump, write_dump},
    file_list::{self, Entry, Version},
};
use serde_json::{Value, json};
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use whisper_rs::Point;

fn oracle(request: Value) -> Value {
    let executable = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/go-whisper-reference");
    let mut process = Command::new(executable)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("build Go oracle: make -C rust reference");
    writeln!(process.stdin.take().unwrap(), "{request}").unwrap();
    let output = process.wait_with_output().unwrap();
    assert!(output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(response.get("error").is_none(), "{response}");
    response["result"].clone()
}

#[test]
fn cache_dump_is_bidirectionally_compatible() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dump.bin");
    let points = vec![
        Point {
            timestamp: 900,
            value: -2.5,
        },
        Point {
            timestamp: 898,
            value: 123.125,
        },
    ];
    write_dump(File::create(&path).unwrap(), "a.b", &points).unwrap();
    let go = oracle(json!({"op":"dump_read", "path":path}));
    assert_eq!(go[0]["metric"], "a.b");
    assert_eq!(
        serde_json::from_value::<Vec<Point>>(go[0]["points"].clone()).unwrap(),
        points
    );
    oracle(json!({"op":"dump_write", "path":path,"metric":"a.b", "points":points}));
    let mut reader = BufReader::new(File::open(&path).unwrap());
    assert_eq!(
        read_dump(&mut reader).unwrap(),
        Some(("a.b".into(), points))
    );
    assert!(read_dump(&mut reader).unwrap().is_none());
}

#[test]
fn file_list_v1_v2_are_bidirectionally_compatible() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flc");
    let entry = Entry {
        path: "/a/b.wsp".into(),
        logical_size: 1234,
        physical_size: 4096,
        data_points: 100,
        first_seen_at: 90,
    };
    for (version, number) in [(Version::V1, 1), (Version::V2, 2)] {
        file_list::write(&path, version, std::slice::from_ref(&entry)).unwrap();
        let go = oracle(json!({"op":"flc_read", "path":path,"version":number}));
        assert_eq!(go[0]["Path"], entry.path);
        if number == 2 {
            assert_eq!(go[0]["LogicalSize"], 1234);
            assert_eq!(go[0]["PhysicalSize"], 4096);
            assert_eq!(go[0]["DataPoints"], 100);
            assert_eq!(go[0]["FirstSeenAt"], 90);
        }
        oracle(
            json!({"op":"flc_write", "path":path,"version":number,"entries":[{"Path":entry.path,"LogicalSize":1234,"PhysicalSize":4096,"DataPoints":100,"FirstSeenAt":90}]}),
        );
        let (got_version, got) = file_list::read(&path).unwrap();
        assert_eq!(got_version, version);
        assert_eq!(got[0].path, entry.path);
        if number == 2 {
            assert_eq!(got[0], entry);
        }
    }
}
