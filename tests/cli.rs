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

/// An invocation the binary has to turn away, and something its complaint has to mention.
struct Refused {
    name: &'static str,
    args: Vec<String>,
    /// Something the complaint has to mention.
    mentions: &'static str,
}

/// Checks each invocation fails, and that what it said names what was wrong with it.
fn refuses(cases: Vec<Refused>) {
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

#[test]
fn an_invocation_that_cannot_be_carried_out_fails_and_says_why() {
    let missing = PathBuf::from(SCRATCH).join("no-such.history");
    let unreadable = scratch_file("not-a-history.history", "some other tool's output\n");
    let reached_twenty = recorded_trace("reached-twenty.trace");
    let already_forked = {
        let forked = chronoloop(&["fork", arg(&reached_twenty), "--at", "7", "--seed", "99"]);
        assert!(forked.status.success(), "{}", stderr(&forked));
        scratch_file("already-forked.trace", &stdout(&forked))
    };
    let cases = vec![
        Refused {
            name: "a history that is not there",
            args: vec!["replay".to_owned(), arg(&missing).to_owned()],
            mentions: "cannot read",
        },
        Refused {
            name: "a file that is not a history",
            args: vec!["replay".to_owned(), arg(&unreadable).to_owned()],
            mentions: "chronoloop history seed",
        },
        Refused {
            name: "a run with no seed",
            args: vec!["run".to_owned()],
            mentions: "--seed",
        },
        Refused {
            name: "no subcommand at all",
            args: vec![],
            mentions: "Usage",
        },
        Refused {
            name: "a file that is not a trace",
            args: vec![
                "inspect".to_owned(),
                arg(&unreadable).to_owned(),
                "--step".to_owned(),
                "0".to_owned(),
            ],
            mentions: "chronoloop trace seed",
        },
        Refused {
            name: "a step the run never reached",
            args: vec![
                "inspect".to_owned(),
                arg(&reached_twenty).to_owned(),
                "--step".to_owned(),
                "20".to_owned(),
            ],
            mentions: "never reached step 20",
        },
        Refused {
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

    refuses(cases);
}

#[test]
fn a_failing_run_that_cannot_be_asked_about_fails_and_says_why() {
    // The same shape one family further on: a file that is not the file the command wanted, and a
    // command asked about a run that cannot give it what it asked for.
    let missing = PathBuf::from(SCRATCH).join("no-such.faults");
    let unreadable = scratch_file("not-a-schedule.faults", "some other tool's output\n");
    let survivable = scratch_file("nothing-to-reduce.faults", LOSSY);
    let checking = |faults: &Path| {
        vec![
            "check".to_owned(),
            "--seed".to_owned(),
            QUORUM_SEED.to_owned(),
            "--faults".to_owned(),
            arg(faults).to_owned(),
        ]
    };
    let cases = vec![
        Refused {
            name: "faults that are not there",
            args: checking(&missing),
            mentions: "cannot read",
        },
        Refused {
            name: "a file that is not a schedule of faults",
            args: checking(&unreadable),
            mentions: "chronoloop faults",
        },
        Refused {
            name: "a file that is not a repro",
            args: vec!["reproduce".to_owned(), arg(&unreadable).to_owned()],
            mentions: "chronoloop repro seed",
        },
        Refused {
            name: "a sweep on no workers, which would never run anything",
            args: vec![
                "sweep".to_owned(),
                "--seeds".to_owned(),
                SWEPT.to_owned(),
                "--jobs".to_owned(),
                "0".to_owned(),
                "--faults".to_owned(),
                arg(&survivable).to_owned(),
            ],
            mentions: "--jobs",
        },
        Refused {
            name: "faults the run holds up under, which have no failure to reduce",
            args: vec![
                "shrink".to_owned(),
                "--seed".to_owned(),
                HOLDS_UP.to_owned(),
                "--faults".to_owned(),
                arg(&survivable).to_owned(),
            ],
            mentions: "nothing to reduce",
        },
    ];

    refuses(cases);
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

/// The seed the fault cases run, since none of them is about a particular one.
const QUORUM_SEED: &str = "20260921";

/// A seed the reduced lossy faults are not enough to break, found by running them under it.
const HOLDS_UP: &str = "1";

/// Three faults the failure does not need and three that cost round 3 its quorum.
///
/// The same schedule `tests/shrink.rs` and `tests/repro.rs` run, deliberately: what those two reach
/// through the library, these reach through a process and a file, and the two routes have to agree.
const FAULTS: &str = "chronoloop faults\n\
                      partition on node 4 -> node 5 from 0.000000000s until forever\n\
                      partition on node 0 -> node 1 from 0.000000000s until 1.000000000s\n\
                      partition on node 0 -> node 4 from 5.000000000s until 10.000000000s\n\
                      partition on node 0 -> node 1 from 15.000000000s until 20.000000000s\n\
                      partition on node 0 -> node 2 from 15.000000000s until 20.000000000s\n\
                      partition on node 0 -> node 3 from 15.000000000s until 20.000000000s\n";

/// The repro reducing it produces, recorded from an actual run.
const REPRO: &str = "chronoloop repro seed 20260921\n\
                     failed at step 14: round 3 lost quorum\n\
                     chronoloop faults\n\
                     partition on node 0 -> node 1 from 15.000000000s until 15.000000001s\n\
                     partition on node 0 -> node 2 from 15.000000000s until 15.000000001s\n\
                     partition on node 0 -> node 3 from 15.000000000s until 15.000000001s\n";

/// Faults whose failure the run's draws take part in, so one seed breaks under them and another does
/// not.
///
/// Every case above is blind to the engine's entropy: the outage schedule reduces to the same three
/// partitions under every seed, so those cases stay green with the engine cut off from its seed
/// entirely. `tests/repro.rs` records the same limit about the same schedule. This is the pair that
/// feels it.
const LOSSY: &str = "chronoloop faults\n\
                     loss 4 in 4 on node 0 -> node 2 from 10.000000000s until 15.000000001s\n\
                     loss 1 in 2 on node 0 -> node 3 from 15.000000000s until 15.000000001s\n\
                     partition on node 0 -> node 4 from 15.000000000s until 15.000000001s\n";

/// How many seeds the sweep cases cover, which is few enough to run in CI.
const SWEPT: &str = "20";

/// Faults no seed in that range breaks under — a link nobody uses, and losses on one between two
/// replicas, which never speak to each other.
///
/// Not an empty schedule: the coordinator cannot fail without faults, so a sweep under none would
/// be green for a reason that says nothing about the sweep.
const MILD: &str = "chronoloop faults\n\
                    partition on node 4 -> node 5 from 0.000000000s until forever\n\
                    loss 1 in 2 on node 2 -> node 3 from 0.000000000s until forever\n";

#[test]
fn checking_a_run_that_broke_says_how_it_broke_and_fails() {
    // `check` asks whether the run held up, so a run that did not is bad news: the verdict goes where
    // every other complaint goes and the exit code says so, which is what makes it usable from a
    // script hunting for a seed.
    let path = scratch_file("trouble.faults", FAULTS);

    let checked = chronoloop(&["check", "--seed", QUORUM_SEED, "--faults", arg(&path)]);

    assert!(!checked.status.success(), "a run that broke is a failure");
    assert_eq!(
        stderr(&checked),
        "chronoloop: seed 20260921: failed at step 13: round 3 lost quorum\n"
    );
    assert!(
        stdout(&checked).is_empty(),
        "the verdict is the complaint, and it is not said twice"
    );
}

#[test]
fn checking_a_run_that_held_up_says_so_and_succeeds() {
    // The other side of the threshold, and the half that feels the seed: these are faults one seed
    // breaks under and this one does not.
    let path = scratch_file("survivable.faults", LOSSY);

    let checked = chronoloop(&["check", "--seed", HOLDS_UP, "--faults", arg(&path)]);

    assert!(checked.status.success(), "{}", stderr(&checked));
    assert_eq!(stdout(&checked), "seed 1: held up under 3 faults\n");

    let broke = chronoloop(&["check", "--seed", QUORUM_SEED, "--faults", arg(&path)]);
    assert!(
        !broke.status.success(),
        "the same faults under the seed they were reduced from: {}",
        stdout(&broke)
    );
}

#[test]
fn shrinking_a_failing_run_writes_the_repro_it_reduces_to() {
    // The pinned text is `tests/repro.rs`'s, reached here through a process and a file rather than
    // through the library — two routes to one text, which says more about the wiring than anything
    // asserted inside either of them.
    let path = scratch_file("shrinkable.faults", FAULTS);

    let shrunk = chronoloop(&["shrink", "--seed", QUORUM_SEED, "--faults", arg(&path)]);

    assert!(shrunk.status.success(), "{}", stderr(&shrunk));
    assert_eq!(stdout(&shrunk), REPRO);
    assert_eq!(
        stdout(&shrunk).len(),
        295,
        "a failing run of a five-node system, in under a third of a kilobyte"
    );
}

#[test]
fn a_repro_the_binary_wrote_puts_a_later_run_through_the_same_failure() {
    // The whole story end to end, across three processes and two files: a schedule goes in, a repro
    // comes out, and the repro alone is enough to put a run back where it was.
    let faults = scratch_file("reducible.faults", FAULTS);
    let shrunk = chronoloop(&["shrink", "--seed", QUORUM_SEED, "--faults", arg(&faults)]);
    assert!(shrunk.status.success(), "{}", stderr(&shrunk));
    let repro = scratch_file("written.repro", &stdout(&shrunk));

    let reproduced = chronoloop(&["reproduce", arg(&repro)]);

    assert!(reproduced.status.success(), "{}", stderr(&reproduced));
    assert_eq!(
        stdout(&reproduced),
        "seed 20260921: failed at step 14: round 3 lost quorum, as the repro expects\n"
    );
}

#[test]
fn a_repro_naming_a_failure_the_run_does_not_produce_is_refused() {
    // What makes the case above worth reading: the run is held to what the file says, so a file
    // naming a failure nothing produces is turned away with both sides quoted.
    let tampered = scratch_file(
        "tampered.repro",
        &REPRO.replacen("round 3 lost quorum", "round 4 lost quorum", 1),
    );

    let reproduced = chronoloop(&["reproduce", arg(&tampered)]);

    assert!(
        !reproduced.status.success(),
        "a failure that did not come back is a failure"
    );
    let complaint = stderr(&reproduced);
    assert!(
        complaint.contains("round 4 lost quorum"),
        "the complaint quotes what the file expects: {complaint}"
    );
    assert!(
        complaint.contains("round 3 lost quorum"),
        "and what the run actually did: {complaint}"
    );
}

#[test]
fn sweeping_names_every_seed_that_broke_and_fails() {
    // `sweep` is `check` asked of a range, so it answers the same way: a seed that broke is bad
    // news, the verdict goes where every other complaint goes, and the exit code says so. The
    // schedule is the one whose failures the run's draws take part in — half of these twenty seeds
    // break under it — because a sweep asserted against the outage schedule would name every seed
    // whatever the engine drew.
    let path = scratch_file("swept.faults", LOSSY);

    let swept = chronoloop(&[
        "sweep",
        "--seeds",
        SWEPT,
        "--jobs",
        "4",
        "--faults",
        arg(&path),
    ]);

    assert!(!swept.status.success(), "seeds that broke are a failure");
    assert_eq!(
        stderr(&swept),
        "chronoloop: 10 of 20 seeds broke\n  \
         seed 0: failed at step 13: round 3 lost quorum\n  \
         seed 3: failed at step 13: round 3 lost quorum\n  \
         seed 4: failed at step 13: round 3 lost quorum\n  \
         seed 5: failed at step 13: round 3 lost quorum\n  \
         seed 7: failed at step 13: round 3 lost quorum\n  \
         seed 8: failed at step 13: round 3 lost quorum\n  \
         seed 12: failed at step 13: round 3 lost quorum\n  \
         seed 16: failed at step 13: round 3 lost quorum\n  \
         seed 17: failed at step 13: round 3 lost quorum\n  \
         seed 18: failed at step 13: round 3 lost quorum\n"
    );
    assert!(
        stdout(&swept).is_empty(),
        "the verdict is the complaint, and it is not said twice"
    );
}

#[test]
fn sweeping_under_faults_nothing_breaks_under_says_so_and_succeeds() {
    // The half a sweep over a system that has been fixed would print, which is what makes "the
    // sweep went green" a thing a person can read off the exit code.
    let path = scratch_file("swept-clean.faults", MILD);

    let swept = chronoloop(&[
        "sweep",
        "--seeds",
        SWEPT,
        "--jobs",
        "4",
        "--faults",
        arg(&path),
    ]);

    assert!(swept.status.success(), "{}", stderr(&swept));
    assert_eq!(
        stdout(&swept),
        "swept 20 seeds under 2 faults: every one held up\n"
    );
}

#[test]
fn a_sweep_reports_the_same_seeds_however_many_workers_it_is_given() {
    // Across real processes, which is where a count of workers is a count of threads. What holds it
    // is the merge being by seed; `tests/sweep.rs` says so beside the library-side case.
    let path = scratch_file("swept-jobs.faults", LOSSY);
    let sweeping = |jobs: &str| {
        let swept = chronoloop(&[
            "sweep",
            "--seeds",
            SWEPT,
            "--jobs",
            jobs,
            "--faults",
            arg(&path),
        ]);
        assert!(!swept.status.success(), "{jobs}: {}", stdout(&swept));
        stderr(&swept)
    };

    let alone = sweeping("1");
    assert!(
        alone.contains("10 of 20 seeds broke"),
        "one worker found them: {alone}"
    );
    for jobs in ["2", "3", "8", "64"] {
        assert_eq!(sweeping(jobs), alone, "{jobs} workers");
    }
}

#[test]
fn a_seed_a_sweep_found_is_a_seed_the_rest_of_the_family_takes() {
    // The pipeline the command exists for, end to end across four processes and three files: a
    // sweep finds a seed, `shrink` cuts its schedule down to a repro, and the repro alone puts a
    // later run back where it was. The seed is read off what the sweep printed rather than written
    // in here, which is what makes this a test of the sweep's answer and not of a constant.
    let faults = scratch_file("pipeline.faults", LOSSY);
    let swept = chronoloop(&[
        "sweep",
        "--seeds",
        SWEPT,
        "--jobs",
        "4",
        "--faults",
        arg(&faults),
    ]);
    assert!(!swept.status.success(), "{}", stdout(&swept));

    let complaint = stderr(&swept);
    let seed = complaint
        .lines()
        .nth(1)
        .and_then(|line| line.trim().strip_prefix("seed "))
        .and_then(|line| line.split(':').next())
        .unwrap_or_else(|| panic!("a sweep names the seeds it found: {complaint}"));

    let shrunk = chronoloop(&["shrink", "--seed", seed, "--faults", arg(&faults)]);
    assert!(shrunk.status.success(), "{}", stderr(&shrunk));
    let repro = scratch_file("swept.repro", &stdout(&shrunk));

    let reproduced = chronoloop(&["reproduce", arg(&repro)]);
    assert!(reproduced.status.success(), "{}", stderr(&reproduced));
    assert!(
        stdout(&reproduced).starts_with(&format!("seed {seed}: failed at step ")),
        "the repro puts the seed the sweep found back through its failure: {}",
        stdout(&reproduced)
    );
}
