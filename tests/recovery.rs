mod common;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::thread::{sleep, spawn};
use std::time::Duration;

use common::open;
use tempfile::tempdir;

/// the n-th command the test sends. a scrambled n picks the key and whether it's a put or a delete,
/// so over time every key gets written, overwritten, deleted and written again
fn command(n: u64) -> (String, Option<String>) {
    let mut x = n.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;

    let key = format!("key{:05}", x % 20_000);
    if (x >> 32) % 5 == 0 {
        (key, None)
    } else {
        (key, Some(format!("value{n}")))
    }
}

#[test]
fn acknowledged_writes_and_deletes_survive_kill_9() {
    for kill_after_ms in [300, 600, 1000] {
        let dir = tempdir().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_database-engine"))
            .args([
                "--dir",
                dir.path().to_str().unwrap(),
                "--demo",
                "--sync",
                "none",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        let mut stdin = child.stdin.take().unwrap();
        spawn(move || {
            for n in 0.. {
                let line = match command(n) {
                    (key, Some(value)) => format!("put {key} {value}"),
                    (key, None) => format!("del {key}"),
                };
                if writeln!(stdin, "{line}").is_err() {
                    break; // the child was killed
                }
            }
        });
        let stdout = child.stdout.take().unwrap();
        let replies = spawn(move || {
            BufReader::new(stdout)
                .lines()
                .map_while(Result::ok)
                .map(|line| line == "OK")
                .collect::<Vec<bool>>()
        });

        sleep(Duration::from_millis(kill_after_ms));
        child.kill().unwrap();
        child.wait().unwrap();
        let replies = replies.join().unwrap();
        assert!(
            replies.iter().any(|ok| *ok),
            "killed before any command was handled"
        );

        let mut expected: BTreeMap<String, Option<String>> = BTreeMap::new();
        for (n, ok) in replies.iter().enumerate() {
            if *ok {
                let (key, value) = command(n as u64);
                expected.insert(key, value);
            }
        }
        // the command after the last reply may have reached the WAL before the kill, so its key can be in either state
        let (in_flight_key, in_flight_value) = command(replies.len() as u64);

        let db = open(dir.path());
        for (key, value) in &expected {
            let got = db
                .get(key.as_bytes())
                .unwrap()
                .map(|v| String::from_utf8(v).unwrap());
            if *key == in_flight_key {
                assert!(
                    got == *value || got == in_flight_value,
                    "in-flight key {key}: got {got:?}"
                );
            } else {
                assert_eq!(got, *value, "key {key} after a kill at {kill_after_ms}ms");
            }
        }
        db.close().unwrap();
    }
}
