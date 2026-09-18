//! Checks whose subject is the world around the binary rather than the binary's own logic.
//!
//! Where a history goes, and what happens when it cannot get there: a reader that has already gone,
//! a device with no room, a redirect to a file. These need a real process in a real environment, so
//! they are gated behind `#[ignore]` and run by `make local-validation` rather than by CI. They
//! still compile under an ordinary `cargo test`, which is what keeps them from quietly rotting.
//!
//! What belongs *here* is the world around the binary: real processes, real pipes, real files. What
//! does not belong here is the command line's own behaviour with a file it can read and write —
//! `tests/cli.rs` covers that, and covering it twice would mean two places to update.
//!
//! Anything else too slow for CI does not have to move into this file. `make local-validation` gates
//! on the `#[ignore]` attribute rather than on this filename, so a check can sit beside the thing it
//! is about — the seed sweep in `tests/determinism.rs` is the first one that does.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

/// The binary under test, built by cargo before this test runs.
const BINARY: &str = env!("CARGO_BIN_EXE_chronoloop");

/// A directory cargo sets aside for this test's files.
const SCRATCH: &str = env!("CARGO_TARGET_TMPDIR");

/// The seed every check here runs, since none of them is about the history itself.
const SEED: &str = "7";

/// Runs `run --seed 7` with its output sent to `stdout`, and returns what the process did.
fn run_writing_to(stdout: Stdio) -> Output {
    Command::new(BINARY)
        .args(["run", "--seed", SEED])
        .stdout(stdout)
        .output()
        .unwrap_or_else(|e| panic!("could not run {BINARY}: {e}"))
}

/// Everything the binary wrote to stderr, which is where a complaint goes.
fn complaint(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
#[ignore = "drives a real pipe; run it with `make local-validation`"]
fn a_history_survives_a_reader_that_is_no_longer_there() {
    // Rust leaves SIGPIPE ignored, so writing down a pipe nobody is reading comes back as an error
    // rather than ending the process, and printing turns that error into a panic. Closing the read
    // end before the child starts makes the case certain; piping to a command that exits early only
    // races for it, and at this size the history wins the race and nothing is proved.
    let (reader, writer) = io::pipe().expect("the system can make a pipe");
    drop(reader);
    let output = run_writing_to(Stdio::from(writer));

    let said = complaint(&output);
    assert!(
        output.status.success(),
        "a reader that has gone is not a failure of the run: {said}"
    );
    assert!(said.is_empty(), "nothing worth saying: {said}");
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "writes to /dev/full; run it with `make local-validation`"]
fn a_write_that_cannot_land_is_reported_rather_than_panicking() {
    // The other half of the same decision: a reader that has gone is not a failure, but a write that
    // genuinely could not land is, and it is reported the way every other failure is.
    let full = fs::File::create("/dev/full").expect("/dev/full takes writes and keeps none");
    let output = run_writing_to(Stdio::from(full));

    let said = complaint(&output);
    assert!(
        !output.status.success(),
        "a history that never landed is a failure: {said}"
    );
    assert!(
        said.contains("cannot write to standard output"),
        "the complaint says where the output could not go: {said}"
    );
    assert!(!said.contains("panicked"), "reported, not panicked: {said}");
}

#[test]
#[ignore = "redirects to a real file; run it with `make local-validation`"]
fn a_history_redirected_to_a_file_lands_whole() {
    // The output is flushed explicitly before the process ends, so a redirect has to hold every byte
    // a terminal would have shown.
    let shown = run_writing_to(Stdio::piped());
    assert!(shown.status.success(), "{}", complaint(&shown));
    assert!(
        !shown.stdout.is_empty(),
        "a run writes a history, so neither side of this comparison is empty"
    );

    let path = PathBuf::from(SCRATCH).join("redirected.history");
    let file = fs::File::create(&path)
        .unwrap_or_else(|e| panic!("could not create {}: {e}", path.display()));
    let redirected = run_writing_to(Stdio::from(file));
    assert!(redirected.status.success(), "{}", complaint(&redirected));

    let landed =
        fs::read(&path).unwrap_or_else(|e| panic!("could not read {}: {e}", path.display()));
    assert_eq!(
        landed, shown.stdout,
        "a redirect holds what a terminal shows"
    );
}
