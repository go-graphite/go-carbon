use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::{Value, json};
use whisper_rs::{Aggregation, Metadata, Options, Point, Retention, Whisper};

fn oracle(request: Value) -> Value {
    let exe = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/go-whisper-reference");
    assert!(
        exe.exists(),
        "missing {}; run `rtk proxy go build -o rust/target/go-whisper-reference ./rust/reference`",
        exe.display()
    );
    let mut child = Command::new(exe)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    writeln!(child.stdin.as_mut().unwrap(), "{request}").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "Go oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        response.get("error").is_none(),
        "Go oracle error: {response}"
    );
    response["result"].clone()
}

fn metadata(method: Aggregation) -> Metadata {
    Metadata {
        aggregation: method,
        x_files_factor: 0.5,
        retentions: vec![
            Retention {
                seconds_per_point: 1,
                points: 20,
            },
            Retention {
                seconds_per_point: 10,
                points: 20,
            },
        ],
        compressed: false,
    }
}
fn rets() -> Value {
    json!([{"seconds_per_point":1,"points":20},{"seconds_per_point":10,"points":20}])
}
fn points() -> Vec<Point> {
    vec![
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
        Point {
            timestamp: 109,
            value: 9.0,
        },
        Point {
            timestamp: 105,
            value: 5.0,
        },
    ]
}
fn json_points(ps: &[Point]) -> Value {
    Value::Array(
        ps.iter()
            .map(|p| json!({"timestamp":p.timestamp,"value":p.value}))
            .collect(),
    )
}
fn comparable_fetch(mut value: Value) -> Value {
    for v in value["values"].as_array_mut().unwrap() {
        if let Some(n) = v.as_f64() {
            *v = json!(n);
        }
    }
    value
}
fn fetch_json(w: &mut Whisper, from: i64, until: i64, now: i64) -> Value {
    let s = w.fetch(from, until, now).unwrap().unwrap();
    comparable_fetch(json!({"from":s.from,"until":s.until,"step":s.step,"values":s.values}))
}
fn integrity(path: &Path, now: i64) {
    oracle(json!({"op":"integrity","path":path.display().to_string(),"now":now}));
}

#[test]
fn go_create_rust_update_go_fetch_matches_all_classic_aggregations() {
    for name in ["average", "sum", "last", "max", "min", "first"] {
        let dir = tempfile::tempdir().unwrap();
        let go = dir.path().join(format!("go-{name}.wsp"));
        let rust_path = dir.path().join(format!("rust-{name}.wsp"));
        oracle(
            json!({"op":"create","path":go.display().to_string(),"now":130,"retentions":rets(),"aggregation":name,"xff":0.5}),
        );
        let mut rust = Whisper::create(
            &rust_path,
            metadata(match name {
                "average" => Aggregation::Average,
                "sum" => Aggregation::Sum,
                "last" => Aggregation::Last,
                "max" => Aggregation::Max,
                "min" => Aggregation::Min,
                _ => Aggregation::First,
            }),
            Options::default(),
        )
        .unwrap();
        let sequence = vec![
            Point {
                timestamp: 110,
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
            Point {
                timestamp: 105,
                value: 5.0,
            },
            Point {
                timestamp: 109,
                value: 9.0,
            },
            Point {
                timestamp: 111,
                value: 7.0,
            },
            Point {
                timestamp: 129,
                value: 2.0,
            },
        ];
        oracle(
            json!({"op":"update","path":go.display().to_string(),"now":130,"points":json_points(&sequence)}),
        );
        rust.update_many(&sequence, 130).unwrap();
        drop(rust);
        for (from, until) in [(100, 120), (100, 110), (110, 130), (109, 111)] {
            let expected = comparable_fetch(oracle(
                json!({"op":"fetch","path":go.display().to_string(),"now":130,"from":from,"until":until}),
            ));
            let actual = comparable_fetch(oracle(
                json!({"op":"fetch","path":rust_path.display().to_string(),"now":130,"from":from,"until":until}),
            ));
            assert_eq!(actual, expected, "{name} {from}..{until}");
        }
    }
}

#[test]
fn rust_create_go_update_rust_fetch_matches_duplicates_and_coarse_retention() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cross.wsp");
    let mut rust =
        Whisper::create(&path, metadata(Aggregation::Average), Options::default()).unwrap();
    rust.update_many(&points(), 110).unwrap();
    drop(rust);
    oracle(
        json!({"op":"update","path":path.display().to_string(),"now":120,"points":json_points(&[Point { timestamp: 102, value: 2.0 }, Point { timestamp: 103, value: 8.0 }])}),
    );
    let mut rust = Whisper::open(&path, Options::default()).unwrap();
    let got = fetch_json(&mut rust, 99, 111, 130);
    let want = oracle(
        json!({"op":"fetch","path":path.display().to_string(),"now":130,"from":99,"until":111}),
    );
    assert_eq!(got, want);
}

#[test]
fn rust_reads_go_created_compressed_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compressed.wsp");
    let rets = json!([{"seconds_per_point":1,"points":200},{"seconds_per_point":10,"points":200}]);
    oracle(
        json!({"op":"create","path":path.display().to_string(),"now":300,"retentions":rets,"aggregation":"average","xff":0.5,"compressed":true}),
    );
    let points: Vec<Point> = (100..=300)
        .map(|timestamp| Point {
            timestamp,
            value: timestamp as f64,
        })
        .collect();
    oracle(
        json!({"op":"update","path":path.display().to_string(),"now":300,"points":json_points(&points)}),
    );
    let expected = comparable_fetch(oracle(
        json!({"op":"fetch","path":path.display().to_string(),"now":300,"from":100,"until":300}),
    ));
    let mut rust = Whisper::open(
        &path,
        Options {
            compressed: true,
            ..Options::default()
        },
    )
    .unwrap();
    assert!(rust.metadata().compressed);
    assert_eq!(fetch_json(&mut rust, 100, 300, 300), expected);
}

#[test]
fn rust_compressed_write_go_read_then_go_append_rust_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust-compressed.wsp");
    let meta = Metadata {
        aggregation: Aggregation::Average,
        x_files_factor: 0.5,
        retentions: vec![
            Retention {
                seconds_per_point: 1,
                points: 200,
            },
            Retention {
                seconds_per_point: 10,
                points: 200,
            },
        ],
        compressed: true,
    };
    let first: Vec<Point> = (100..=300)
        .map(|timestamp| Point {
            timestamp,
            value: timestamp as f64,
        })
        .collect();
    let mut rust = Whisper::create(
        &path,
        meta,
        Options {
            compressed: true,
            ..Options::default()
        },
    )
    .unwrap();
    rust.update_many(&first, 300).unwrap();
    drop(rust);
    integrity(&path, 300);
    let go_before = comparable_fetch(oracle(
        json!({"op":"fetch","path":path.display().to_string(),"now":300,"from":100,"until":300}),
    ));
    let mut rust = Whisper::open(
        &path,
        Options {
            compressed: true,
            ..Options::default()
        },
    )
    .unwrap();
    assert_eq!(fetch_json(&mut rust, 100, 300, 300), go_before);
    drop(rust);
    let append = [
        Point {
            timestamp: 301,
            value: 301.0,
        },
        Point {
            timestamp: 302,
            value: 302.0,
        },
    ];
    oracle(
        json!({"op":"update","path":path.display().to_string(),"now":302,"points":json_points(&append)}),
    );
    integrity(&path, 302);
    let expected = comparable_fetch(oracle(
        json!({"op":"fetch","path":path.display().to_string(),"now":302,"from":290,"until":302}),
    ));
    let mut rust = Whisper::open(
        &path,
        Options {
            compressed: true,
            ..Options::default()
        },
    )
    .unwrap();
    assert_eq!(fetch_json(&mut rust, 290, 302, 302), expected);
}

#[test]
fn compressed_sidecar_main_wins_and_cross_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ooo.wsp");
    let rets = json!([{"seconds_per_point":1,"points":100},{"seconds_per_point":10,"points":100}]);
    oracle(
        json!({"op":"create","path":path.display().to_string(),"now":200,"retentions":rets,"aggregation":"average","xff":0.5,"compressed":true,"out_of_order":true}),
    );
    oracle(
        json!({"op":"update","path":path.display().to_string(),"now":200,"out_of_order":true,"points":json_points(&[Point { timestamp: 180, value: 18.0 }, Point { timestamp: 190, value: 19.0 }])}),
    );
    oracle(
        json!({"op":"update","path":path.display().to_string(),"now":200,"out_of_order":true,"points":json_points(&[Point { timestamp: 185, value: 1.0 }, Point { timestamp: 190, value: 99.0 }])}),
    );
    let expected = comparable_fetch(oracle(
        json!({"op":"fetch","path":path.display().to_string(),"now":200,"from":180,"until":190}),
    ));
    let mut rust = Whisper::open(
        &path,
        Options {
            compressed: true,
            out_of_order: true,
            ..Options::default()
        },
    )
    .unwrap();
    assert_eq!(fetch_json(&mut rust, 180, 190, 200), expected);
    rust.compact_out_of_order(200).unwrap();
    drop(rust);
    integrity(&path, 200);
    let compacted = comparable_fetch(oracle(
        json!({"op":"fetch","path":path.display().to_string(),"now":200,"from":180,"until":190}),
    ));
    let mut rust = Whisper::open(
        &path,
        Options {
            compressed: true,
            out_of_order: true,
            ..Options::default()
        },
    )
    .unwrap();
    assert_eq!(fetch_json(&mut rust, 180, 190, 200), compacted);
}

#[test]
fn go_compacts_rust_created_sidecar_and_main_value_wins() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rust-ooo.wsp");
    let meta = Metadata {
        aggregation: Aggregation::Average,
        x_files_factor: 0.5,
        retentions: vec![
            Retention {
                seconds_per_point: 1,
                points: 200,
            },
            Retention {
                seconds_per_point: 10,
                points: 200,
            },
        ],
        compressed: true,
    };
    let on_time: Vec<Point> = (100..=300)
        .map(|timestamp| Point {
            timestamp,
            value: timestamp as f64,
        })
        .collect();
    let mut rust = Whisper::create(
        &path,
        meta,
        Options {
            compressed: true,
            out_of_order: true,
            ..Options::default()
        },
    )
    .unwrap();
    rust.update_many(&on_time, 300).unwrap();
    rust.update_many(
        &[
            Point {
                timestamp: 180,
                value: -1.0,
            },
            Point {
                timestamp: 181,
                value: -2.0,
            },
        ],
        300,
    )
    .unwrap();
    drop(rust);
    integrity(&path, 300);
    assert!(
        whisper_rs::out_of_order_sidecar_path(&path).exists(),
        "late points did not create a sidecar"
    );
    oracle(json!({"op":"compact","path":path.display().to_string(),"now":300,"out_of_order":true}));
    assert!(!whisper_rs::out_of_order_sidecar_path(&path).exists());
    let expected = comparable_fetch(oracle(
        json!({"op":"fetch","path":path.display().to_string(),"now":300,"from":175,"until":185}),
    ));
    let mut rust = Whisper::open(
        &path,
        Options {
            compressed: true,
            out_of_order: true,
            ..Options::default()
        },
    )
    .unwrap();
    let actual = fetch_json(&mut rust, 175, 185, 300);
    assert_eq!(actual, expected);
    assert_eq!(actual["values"][4], json!(180.0));
}

#[test]
fn rust_compacts_go_sidecar_reaggregates_union_and_preserves_live_buffers() {
    let dir = tempfile::tempdir().unwrap();
    let go = dir.path().join("go.wsp");
    let rust = dir.path().join("rust.wsp");
    let rets = json!([{"seconds_per_point":1,"points":200},{"seconds_per_point":10,"points":200}]);
    oracle(
        json!({"op":"create","path":go,"now":300,"retentions":rets,"aggregation":"average","xff":0.5,"compressed":true,"out_of_order":true}),
    );
    let on_time: Vec<Point> = (100..=300)
        .filter(|t| *t != 180)
        .map(|timestamp| Point {
            timestamp,
            value: timestamp as f64,
        })
        .collect();
    oracle(
        json!({"op":"update","path":go,"now":300,"out_of_order":true,"points":json_points(&on_time)}),
    );
    oracle(
        json!({"op":"update","path":go,"now":300,"out_of_order":true,"points":[{"timestamp":180,"value":180.0},{"timestamp":181,"value":-999.0}]}),
    );
    let sidecar = whisper_rs::out_of_order_sidecar_path(&go);
    assert!(sidecar.exists(), "fixture must exercise a real sidecar");
    std::fs::copy(&go, &rust).unwrap();
    std::fs::copy(&sidecar, whisper_rs::out_of_order_sidecar_path(&rust)).unwrap();
    oracle(json!({"op":"compact","path":go,"now":300,"out_of_order":true}));
    let options = Options {
        compressed: true,
        out_of_order: true,
        ..Options::default()
    };
    let mut w = Whisper::open(&rust, options).unwrap();
    w.compact_out_of_order(300).unwrap();
    drop(w);
    assert!(!whisper_rs::out_of_order_sidecar_path(&rust).exists());
    for path in [&go, &rust] {
        integrity(path, 300);
        oracle(
            json!({"op":"update","path":path,"now":310,"out_of_order":true,"points":[{"timestamp":301,"value":301.0},{"timestamp":310,"value":310.0}]}),
        );
        integrity(path, 310);
    }
    for (from, until, now) in [
        (175, 190, 310),
        (90, 310, 310),
        (170, 200, 400),
        (280, 320, 500),
    ] {
        let expected = comparable_fetch(oracle(
            json!({"op":"fetch","path":go,"now":now,"from":from,"until":until}),
        ));
        let actual = comparable_fetch(oracle(
            json!({"op":"fetch","path":rust,"now":now,"from":from,"until":until}),
        ));
        assert_eq!(actual, expected, "range {from}..{until} at {now}");
        let mut w = Whisper::open(&rust, options).unwrap();
        assert_eq!(fetch_json(&mut w, from, until, now), expected);
    }
}

#[test]
fn independent_compressed_writes_match_go_at_each_retention() {
    let methods = [
        ("average", Aggregation::Average),
        ("sum", Aggregation::Sum),
        ("last", Aggregation::Last),
        ("max", Aggregation::Max),
        ("min", Aggregation::Min),
        ("first", Aggregation::First),
    ];
    let rets = json!([{"seconds_per_point":1,"points":200},{"seconds_per_point":10,"points":100}]);
    let batches: Vec<Vec<Point>> = (0..4)
        .map(|batch| {
            (100 + batch * 51..100 + (batch + 1) * 51)
                .filter(|timestamp| timestamp % 7 != 0)
                .map(|timestamp| Point {
                    timestamp,
                    value: (timestamp as f64 * 0.125) - 17.25,
                })
                .collect()
        })
        .collect();
    for (name, method) in methods {
        let dir = tempfile::tempdir().unwrap();
        let go = dir.path().join("go.wsp");
        let rust_path = dir.path().join("rust.wsp");
        oracle(
            json!({"op":"create","path":go.display().to_string(),"now":300,"retentions":rets,"aggregation":name,"xff":0.5,"compressed":true}),
        );
        let mut rust = Whisper::create(
            &rust_path,
            Metadata {
                aggregation: method,
                x_files_factor: 0.5,
                retentions: vec![
                    Retention {
                        seconds_per_point: 1,
                        points: 200,
                    },
                    Retention {
                        seconds_per_point: 10,
                        points: 100,
                    },
                ],
                compressed: true,
            },
            Options {
                compressed: true,
                ..Options::default()
            },
        )
        .unwrap();
        for batch in &batches {
            oracle(
                json!({"op":"update","path":go.display().to_string(),"now":300,"points":json_points(batch)}),
            );
            rust.update_many(batch, 300).unwrap();
        }
        drop(rust);
        integrity(&rust_path, 300);
        for (from, until) in [(100, 300), (90, 200), (95, 150)] {
            let expected = comparable_fetch(oracle(
                json!({"op":"fetch","path":go.display().to_string(),"now":300,"from":from,"until":until}),
            ));
            let actual = comparable_fetch(oracle(
                json!({"op":"fetch","path":rust_path.display().to_string(),"now":300,"from":from,"until":until}),
            ));
            assert_eq!(actual, expected, "{name} {from}..{until}");
        }
    }
}
