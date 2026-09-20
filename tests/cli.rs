//! The command line as a person meets it: a real process, a real file, and a real exit code.
//!
//! The expectations for what the trace commands print are **pinned literals recorded from actual
//! runs**, never assembled here from the calls the binary makes — a line built out of the same
//! parts would agree with the binary whatever those parts did. What they hold to account is the
//! whole path: the ring's draws, the state encoding, the store, the walk, and the layout each of
//! the four commands prints.

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
    let reached_twenty = recorded_trace("reached-twenty.trace");
    let already_forked = {
        let forked = chronoloop(&["fork", arg(&reached_twenty), "--at", "7", "--seed", "99"]);
        assert!(forked.status.success(), "{}", stderr(&forked));
        scratch_file("already-forked.trace", &stdout(&forked))
    };
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
        Case {
            name: "a file that is not a trace",
            args: vec![
                "inspect".to_owned(),
                arg(&unreadable).to_owned(),
                "--step".to_owned(),
                "0".to_owned(),
            ],
            mentions: "chronoloop trace seed",
        },
        Case {
            name: "a step the run never reached",
            args: vec![
                "inspect".to_owned(),
                arg(&reached_twenty).to_owned(),
                "--step".to_owned(),
                "20".to_owned(),
            ],
            mentions: "never reached step 20",
        },
        Case {
            name: "a second fork of a trace that already forked",
            args: vec![
                "fork".to_owned(),
                arg(&already_forked).to_owned(),
                "--at".to_owned(),
                "3".to_owned(),
                "--seed".to_owned(),
                "5".to_owned(),
            ],
            mentions: "a run carries one fork",
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

/// The seed every trace case runs, since none of them is about a particular one.
const SEED: &str = "20260919";

/// How many steps that run takes.
const STEPS: usize = 20;

/// Writes a trace of [`SEED`] to a file called `name`, and returns its path.
fn recorded_trace(name: &str) -> PathBuf {
    let run = chronoloop(&["trace", "--seed", SEED]);
    assert!(run.status.success(), "{}", stderr(&run));
    scratch_file(name, &stdout(&run))
}

#[test]
fn a_trace_of_one_seed_writes_the_same_bytes_every_time() {
    let first = chronoloop(&["trace", "--seed", SEED]);
    let second = chronoloop(&["trace", "--seed", SEED]);

    assert!(first.status.success(), "{}", stderr(&first));
    assert_eq!(first.stdout, second.stdout);
    assert_eq!(
        stdout(&first).lines().next(),
        Some("chronoloop trace seed 20260919"),
        "the trace names its seed"
    );
    assert_eq!(
        stdout(&first).lines().count(),
        STEPS + 1,
        "a header and a step apiece"
    );
}

#[test]
fn inspecting_a_step_shows_the_record_and_the_world_behind_it() {
    // A trace names states without holding them, so everything under the first line comes out of a
    // run the binary did again from the seed the file names.
    let path = recorded_trace("inspected.trace");

    let shown = chronoloop(&["inspect", arg(&path), "--step", "0"]);

    assert!(shown.status.success(), "{}", stderr(&shown));
    assert_eq!(
        stdout(&shown),
        "step 0 0.941245535s \
         66edefcab06b5b6ee33404a5dec6d190db5b571b3125fb320af15068d797c3c1 \
         us-east heard from node 1 in round 1\n\
         us-east.heard 0.941245535s\n\
         us-east.peer \"node 1\"\n\
         us-east.round 1\n\
         us-east.waiting false\n"
    );
}

#[test]
fn comparing_two_steps_says_which_fields_moved_and_leaves_the_rest_alone() {
    // Two steps of one node, five steps apart: what moved is the round it is on and when it last
    // heard. The two fields that never move — the neighbour it hears from and the flag it writes —
    // are the reason the ring writes them at all, and they are named nowhere here.
    let path = recorded_trace("compared.trace");

    let shown = chronoloop(&["diff", arg(&path), "6", "7"]);

    assert!(shown.status.success(), "{}", stderr(&shown));
    assert_eq!(
        stdout(&shown),
        "step 6 -> step 7\n\
         ~ eu-east.heard 1.138135063s -> 2.879111427s\n\
         ~ eu-east.round 1 -> 2\n"
    );
}

#[test]
fn forking_writes_a_trace_that_says_what_produces_it_again() {
    // The artifact a fork leaves behind, written by one process and read by the next: the header
    // still names the run it was until the fork, and the fork line names the rest of it.
    let path = recorded_trace("forkable.trace");

    let forked = chronoloop(&["fork", arg(&path), "--at", "7", "--seed", "99"]);
    assert!(forked.status.success(), "{}", stderr(&forked));

    let written: Vec<String> = stdout(&forked).lines().map(str::to_owned).collect();
    assert_eq!(
        written.len(),
        STEPS + 2,
        "a header, a fork, and a step apiece"
    );
    assert_eq!(written[0], "chronoloop trace seed 20260919");
    assert_eq!(written[1], "forked at 2.879111427s to seed 99");

    // And the file it wrote is a file the other commands read: showing its last step runs the fork
    // again from what the two header lines name, and finds the state the fork left behind.
    let elsewhere = scratch_file("forked.trace", &stdout(&forked));
    let shown = chronoloop(&["inspect", arg(&elsewhere), "--step", "19"]);

    assert!(shown.status.success(), "{}", stderr(&shown));
    assert_eq!(
        stdout(&shown).lines().next(),
        Some(
            "step 19 7.673233028s \
             de5263dab4f2e8e6dafca836b75dbfedbff7b8e74899e6bd264c61ff8e757c51 \
             eu-west heard from node 3 in round 5"
        )
    );
}

#[test]
fn a_fork_back_to_a_traces_own_seed_writes_the_trace_back_out() {
    // The cheapest check on the whole mechanism, made across two processes and a file on disk: a
    // fork that cannot reproduce its own run has nothing to say about any other. The recorded side
    // is a file one process wrote and the forked side is what another made of it, which is the
    // argument that made `replay` take a path rather than a seed.
    //
    // What comes back is the file itself with one line inserted, because the fork changes what is
    // drawn after the instant and the seed it changes to is the seed already being drawn from.
    let path = recorded_trace("unforked.trace");
    let text = fs::read_to_string(&path).expect("the trace was just written");

    let forked = chronoloop(&["fork", arg(&path), "--at", "7", "--seed", SEED]);
    assert!(forked.status.success(), "{}", stderr(&forked));

    let shown = stdout(&forked);
    let written: Vec<&str> = shown.lines().collect();
    let recorded: Vec<&str> = text.lines().collect();
    assert_eq!(
        written.len(),
        STEPS + 2,
        "a header, a fork, and a step apiece"
    );
    assert_eq!(
        written.first(),
        recorded.first(),
        "the header still names the run it was until the fork"
    );
    assert_eq!(written[1], "forked at 2.879111427s to seed 20260919");
    assert_eq!(
        &written[2..],
        &recorded[1..],
        "and every step below it is the step the run took"
    );
}

#[test]
fn a_trace_the_run_no_longer_produces_is_refused_rather_than_shown() {
    // The check that makes everything above worth reading: what is shown comes out of a run, and a
    // file the run does not produce is a file about nothing.
    let faithful = recorded_trace("faithful.trace");
    let text = fs::read_to_string(&faithful).expect("the trace was just written");
    let path = scratch_file(
        "tampered.trace",
        &text.replacen("in round 1", "in round 9", 1),
    );

    let shown = chronoloop(&["inspect", arg(&path), "--step", "0"]);

    assert!(
        !shown.status.success(),
        "a trace nothing produces is a failure"
    );
    let complaint = stderr(&shown);
    assert!(
        complaint.contains("diverged at line 2"),
        "the first step is on line 2: {complaint}"
    );
    assert!(
        complaint.contains("in round 9"),
        "the complaint quotes what the file holds: {complaint}"
    );
}
