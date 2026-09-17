//! The command line as a person meets it: a real process, a real file, and a real exit code.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The binary under test, built by cargo before this test runs.
const BINARY: &str = env!("CARGO_BIN_EXE_chronoloop");

/// A directory cargo sets aside for this test's files.
///
/// It is used instead of a name drawn from the clock or from entropy, neither of which a simulation
/// or its tests may reach for.
const SCRATCH: &str = env!("CARGO_TARGET_TMPDIR");

/// Runs the binary with `args` and returns what it did.
fn chronoloop(args: &[&str]) -> Output {
    Command::new(BINARY)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("could not run {BINARY}: {e}"))
}

/// Everything the binary wrote to stdout, which is where a history goes.
fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is text")
}

/// Everything the binary wrote to stderr, which is where a complaint goes.
fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr is text")
}

/// Writes `contents` to a file named `name` in this test's scratch directory, returning its path.
fn scratch_file(name: &str, contents: &str) -> PathBuf {
    let path = PathBuf::from(SCRATCH).join(name);
    fs::write(&path, contents)
        .unwrap_or_else(|e| panic!("could not write {}: {e}", path.display()));
    path
}

/// The path of a file as an argument, which has to survive being a `&str`.
fn arg(path: &Path) -> &str {
    path.to_str().expect("the scratch path is text")
}

#[test]
fn a_run_of_one_seed_writes_the_same_bytes_every_time() {
    let first = chronoloop(&["run", "--seed", "20260917"]);
    let second = chronoloop(&["run", "--seed", "20260917"]);

    assert!(first.status.success(), "{}", stderr(&first));
    assert_eq!(first.stdout, second.stdout);
    assert!(
        stdout(&first).starts_with("chronoloop history seed 20260917\n"),
        "the history names its seed: {}",
        stdout(&first)
    );
}

#[test]
fn a_replay_of_a_history_the_binary_wrote_finds_no_divergence() {
    // The whole point of the file: the recorded side and the replayed side come from two separate
    // processes, so this is not a run being compared with itself.
    let run = chronoloop(&["run", "--seed", "7"]);
    assert!(run.status.success(), "{}", stderr(&run));
    let path = scratch_file("faithful.history", &stdout(&run));

    let replay = chronoloop(&["replay", arg(&path)]);

    assert!(replay.status.success(), "{}", stderr(&replay));
    assert_eq!(stdout(&replay), "seed 7: 4 entries replayed identically\n");
}

#[test]
fn a_replay_of_a_tampered_history_names_the_line_that_diverged() {
    let run = chronoloop(&["run", "--seed", "7"]);
    assert!(run.status.success(), "{}", stderr(&run));
    let faithful = stdout(&run);
    let (header, rest) = faithful
        .split_once('\n')
        .expect("a history has a header and entries");
    let (_, entries) = rest.split_once('\n').expect("a history has entries");
    let tampered = format!("{header}\n9.000000000s ping sent\n{entries}");
    let path = scratch_file("tampered.history", &tampered);

    let replay = chronoloop(&["replay", arg(&path)]);

    assert!(!replay.status.success(), "a divergence is a failure");
    let complaint = stderr(&replay);
    assert!(
        complaint.contains("diverged at line 2"),
        "the first entry is on line 2: {complaint}"
    );
    assert!(
        complaint.contains("9.000000000s ping sent"),
        "the complaint quotes what was recorded: {complaint}"
    );
}

#[test]
fn an_invocation_that_cannot_be_carried_out_fails_and_says_why() {
    struct Case {
        name: &'static str,
        args: Vec<String>,
        /// Something the complaint has to mention.
        mentions: &'static str,
    }
    let missing = PathBuf::from(SCRATCH).join("no-such.history");
    let unreadable = scratch_file("not-a-history.history", "some other tool's output\n");
    let cases = [
        Case {
            name: "a history that is not there",
            args: vec!["replay".to_owned(), arg(&missing).to_owned()],
            mentions: "cannot read",
        },
        Case {
            name: "a file that is not a history",
            args: vec!["replay".to_owned(), arg(&unreadable).to_owned()],
            mentions: "chronoloop history seed",
        },
        Case {
            name: "a run with no seed",
            args: vec!["run".to_owned()],
            mentions: "--seed",
        },
        Case {
            name: "no subcommand at all",
            args: vec![],
            mentions: "Usage",
        },
    ];

    for case in cases {
        let args: Vec<&str> = case.args.iter().map(String::as_str).collect();
        let output = chronoloop(&args);

        assert!(!output.status.success(), "{}", case.name);
        let complaint = stderr(&output);
        assert!(
            complaint.contains(case.mentions),
            "{}: expected a complaint mentioning {:?}, got {complaint}",
            case.name,
            case.mentions
        );
    }
}
