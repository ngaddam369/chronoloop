//! The determinism rules, enforced rather than documented.
//!
//! The engine's guarantee is that a run is a pure function of its seed. That holds today because
//! every line has been written with it in mind, which is not the same as it being true — a rule
//! nothing enforces is a comment. This scan reads every Rust file under `src/` and `tests/` and
//! fails the build when one of them names something that would let the outside world reach a
//! simulation: the machine's clock, the operating system's entropy, a collection that iterates in
//! an order drawn from somewhere else, a second thread, or an address.
//!
//! What it cannot see, said here rather than left for the count of needles to imply: that ties at
//! one instant are broken explicitly. Nothing about `(instant, seq)` is visible in a grep, and the
//! tests that hold that rule up are the ones in `tests/event.rs` and `tests/sleep.rs`. Nor is this
//! a proof of the five rules it does cover — it catches a forbidden thing by name, so a new way of
//! spelling an old mistake goes unnoticed until someone adds it below.
//!
//! Two pieces of scope are deliberate. **`benches/` is not read**: a benchmark measures the engine
//! from outside instead of taking part in a simulation, so the wall clock it reads is the right
//! instrument and the one honest exception. And **this file reads everything but itself** — every
//! forbidden spelling is written out below as a literal, so without the skip the audit would be its
//! own first violation. That is asserted rather than assumed.
//!
//! A match counts wherever it falls, comments included. Stripping them first would need a
//! heuristic that is wrong about `//` inside a string literal, and would give a violation a place
//! to hide; the price is that these names are spelled in one file in the repository, and this is
//! it. Prose elsewhere has to say what it means in words.

use std::fs;
use std::path::{Path, PathBuf};

/// The directories the scan reads, relative to the crate root.
const SCANNED: [&str; 2] = ["src", "tests"];

/// A spelling a simulation must not contain, and what it costs.
struct Rule {
    /// The literal spelling this rule forbids.
    needle: &'static str,
    /// The crown jewel it breaks, named so a failure says what was lost rather than what matched.
    jewel: &'static str,
    /// What the spelling does, and why a simulation must not do it.
    why: &'static str,
}

/// Time comes from the virtual clock only.
const VIRTUAL_TIME: &str = "time comes from the virtual clock only";

/// One entropy source, the run's seed.
const ONE_SEED: &str = "one entropy source, the run's seed";

/// Ordered collections only, so iteration order cannot reach behaviour.
const ORDERED: &str = "ordered collections only";

/// A simulation runs on one thread.
const ONE_THREAD: &str = "a simulation runs on one thread";

/// No address ever reaches state, a hash, or a recorded history.
const NO_ADDRESSES: &str = "no address reaches state, a hash, or a history";

/// Every forbidden spelling, with the jewel it breaks and why.
///
/// A `static` rather than a `const`, because a `const` is inlined at each use and a reference taken
/// to one points at a temporary; the reports below outlive the scan that produced them.
static RULES: [Rule; 18] = [
    Rule {
        needle: "Instant::now",
        jewel: VIRTUAL_TIME,
        why: "reads the machine's monotonic clock, so a history would depend on how long the \
              machine took rather than on what the simulation did",
    },
    Rule {
        needle: "SystemTime::now",
        jewel: VIRTUAL_TIME,
        why: "reads the machine's wall clock, which moves for reasons — a leap second, a clock \
              correction — that no seed accounts for",
    },
    Rule {
        needle: "thread::sleep",
        jewel: VIRTUAL_TIME,
        why: "spends real time; a simulated wait costs an event, not a delay",
    },
    Rule {
        needle: "thread_rng",
        jewel: ONE_SEED,
        why: "a generator seeded from the operating system, so nothing it draws can be replayed",
    },
    Rule {
        needle: "rand::rng",
        jewel: ONE_SEED,
        why: "the entropy-backed generator and the generators beside it; the only one a simulation \
              may draw from is a `ChaCha8Rng` built from the run's seed",
    },
    Rule {
        needle: "OsRng",
        jewel: ONE_SEED,
        why: "the operating system's entropy, named outright",
    },
    Rule {
        needle: "from_os_rng",
        jewel: ONE_SEED,
        why: "seeds a generator from the operating system instead of from the run's seed",
    },
    Rule {
        needle: "getrandom",
        jewel: ONE_SEED,
        why: "the call underneath all of these, and the reason the generator crates are built with \
              their default features off",
    },
    Rule {
        needle: "HashMap",
        jewel: ORDERED,
        why: "iterates in an order drawn from a per-process seed of its own, so anything that \
              order reaches differs between two runs of the same simulation seed",
    },
    Rule {
        needle: "HashSet",
        jewel: ORDERED,
        why: "the same order, from the same per-process seed",
    },
    Rule {
        needle: "thread::spawn",
        jewel: ONE_THREAD,
        why: "a second thread makes the interleaving the operating system's decision rather than \
              the seed's",
    },
    Rule {
        needle: "thread::scope",
        jewel: ONE_THREAD,
        why: "a scope is still threads, and it starts several of them at once",
    },
    Rule {
        needle: "thread::Builder",
        jewel: ONE_THREAD,
        why: "the long way round to the same spawn",
    },
    Rule {
        needle: "thread::{",
        jewel: ONE_THREAD,
        why: "a braced import hides which thread function came in; name one at a time or none",
    },
    Rule {
        needle: "as_ptr",
        jewel: NO_ADDRESSES,
        why: "an address varies between runs of the same binary, so anything derived from one is \
              not a function of the seed",
    },
    Rule {
        needle: "as *const",
        jewel: NO_ADDRESSES,
        why: "the same address, taken by a cast",
    },
    Rule {
        needle: "as *mut",
        jewel: NO_ADDRESSES,
        why: "the same address, taken by a cast that could also write through it",
    },
    Rule {
        needle: "{:p}",
        jewel: NO_ADDRESSES,
        why: "prints an address, which puts one straight into a recorded history",
    },
];

/// A line that breaks exactly one rule, and the needle that has to catch it.
struct Violation {
    /// What the line does, for the assertion that names it.
    name: &'static str,
    /// The line itself.
    source: &'static str,
    /// The needle expected to catch it.
    needle: &'static str,
}

/// One line for every rule above, written out by hand rather than built from the needles.
///
/// A fixture generated from the string it is meant to catch says nothing about that string's
/// spelling — it is the same trap as an assertion whose two sides come from one run. Each of these
/// is a line someone could plausibly write, and each breaks exactly one rule, which is also how a
/// needle broad enough to swallow its neighbours gets caught.
static VIOLATIONS: [Violation; 18] = [
    Violation {
        name: "the machine's monotonic clock",
        source: "    let started = std::time::Instant::now();",
        needle: "Instant::now",
    },
    Violation {
        name: "the machine's wall clock",
        source: "    let stamped = SystemTime::now().duration_since(epoch)?;",
        needle: "SystemTime::now",
    },
    Violation {
        name: "spending real time",
        source: "    std::thread::sleep(Duration::from_millis(5));",
        needle: "thread::sleep",
    },
    Violation {
        name: "the per-thread generator",
        source: "    let mut generator = rand::thread_rng();",
        needle: "thread_rng",
    },
    Violation {
        name: "the entropy-backed generator",
        source: "    let delay: u64 = rand::rng().random_range(1..=100);",
        needle: "rand::rng",
    },
    Violation {
        name: "the operating system's entropy by name",
        source: "    use rand_core::OsRng;",
        needle: "OsRng",
    },
    Violation {
        name: "seeding from the operating system",
        source: "    let mut generator = ChaCha8Rng::from_os_rng();",
        needle: "from_os_rng",
    },
    Violation {
        name: "the call underneath them",
        source: "    getrandom::fill(&mut seed).expect(\"the system has entropy\");",
        needle: "getrandom",
    },
    Violation {
        name: "an unordered map in simulated state",
        source: "    let mut seen: HashMap<NodeId, u64> = HashMap::new();",
        needle: "HashMap",
    },
    Violation {
        name: "an unordered set in simulated state",
        source: "    let delivered: HashSet<WaitId> = waiting.keys().copied().collect();",
        needle: "HashSet",
    },
    Violation {
        name: "a second thread",
        source: "    let worker = std::thread::spawn(move || follower.run());",
        needle: "thread::spawn",
    },
    Violation {
        name: "a scope full of them",
        source: "    std::thread::scope(|region| region.spawn(|| sweep(seeds)));",
        needle: "thread::scope",
    },
    Violation {
        name: "the long way round to a spawn",
        source: "    let handle = thread::Builder::new().name(label).spawn(run)?;",
        needle: "thread::Builder",
    },
    Violation {
        name: "a braced import hiding what came in",
        source: "    use std::thread::{sleep, spawn};",
        needle: "thread::{",
    },
    Violation {
        name: "an identity taken from an address",
        source: "    let identity = Rc::as_ptr(&shared) as usize;",
        needle: "as_ptr",
    },
    Violation {
        name: "an address taken by a cast",
        source: "    let raw = &node as *const NodeId;",
        needle: "as *const",
    },
    Violation {
        name: "an address taken by a cast that can write",
        source: "    let raw = &mut queue as *mut EventQueue;",
        needle: "as *mut",
    },
    Violation {
        name: "an address printed into a history",
        source: "    history.record(clock, format!(\"polling task at {:p}\", &task));",
        needle: "{:p}",
    },
];

/// Every rule broken by `text`, as a one-based line number and the rule it broke.
fn scan(text: &str) -> Vec<(usize, &'static Rule)> {
    let mut broken = Vec::new();
    for (index, line) in text.lines().enumerate() {
        for rule in &RULES {
            if line.contains(rule.needle) {
                broken.push((index + 1, rule));
            }
        }
    }
    broken
}

/// The crate root, which is what every path here is spelled relative to.
fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// This file, so the scan can leave out the one place the forbidden spellings belong.
fn self_path() -> PathBuf {
    root().join(file!())
}

/// `path` as a reader would name it, rather than from the root of the filesystem.
fn relative(path: &Path) -> &Path {
    path.strip_prefix(root()).unwrap_or(path)
}

/// Adds every Rust file at or under `dir` to `found`, leaving out `skip`.
fn collect(dir: &Path, skip: &Path, found: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(dir).unwrap_or_else(|e| panic!("could not read {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("could not read {}: {e}", dir.display()));
        let path = entry.path();
        if path.is_dir() {
            collect(&path, skip, found);
        } else if path.extension().is_some_and(|kind| kind == "rs") && path != skip {
            found.push(path);
        }
    }
}

/// Every Rust file the scan reads, in an order that does not come from the filesystem.
fn sources() -> Vec<PathBuf> {
    let skip = self_path();
    let mut found = Vec::new();
    for dir in SCANNED {
        collect(&root().join(dir), &skip, &mut found);
    }
    // `read_dir` hands entries back in whatever order the filesystem holds them. Sorting is what
    // keeps two machines reporting the same violations in the same order.
    found.sort();
    found
}

/// A broken rule, spelled the way the failure reports it.
fn report(path: &Path, line: usize, rule: &Rule) -> String {
    format!(
        "{}:{line} says `{}` — {}: {}",
        relative(path).display(),
        rule.needle,
        rule.jewel,
        rule.why
    )
}

#[test]
fn nothing_in_the_engine_reaches_past_the_simulation() {
    let mut broken = Vec::new();
    for path in sources() {
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("could not read {}: {e}", path.display()));
        for (line, rule) in scan(&text) {
            broken.push(report(&path, line, rule));
        }
    }

    assert!(
        broken.is_empty(),
        "a simulation must not be able to reach any of these:\n{}",
        broken.join("\n")
    );
}

#[test]
fn every_needle_catches_the_line_it_is_for() {
    for case in &VIOLATIONS {
        let found = scan(case.source);
        assert_eq!(
            found.len(),
            1,
            "{}: one broken rule, not {}",
            case.name,
            found.len()
        );
        assert_eq!(found[0].0, 1, "{}: the line it is on", case.name);
        assert_eq!(found[0].1.needle, case.needle, "{}", case.name);
    }

    // And no rule may sit in the table without a line proving it catches something, which is how a
    // needle added later stays as honest as the eighteen that came with it.
    for rule in &RULES {
        assert!(
            VIOLATIONS.iter().any(|case| case.needle == rule.needle),
            "`{}` has no line showing it catches anything",
            rule.needle
        );
    }
}

#[test]
fn source_with_nothing_to_hide_raises_nothing() {
    // The other half: a needle broad enough to match ordinary engine code would turn the audit into
    // something that fails whatever anyone writes, which is as useless as one that never fires.
    let clean = "\
use std::collections::{BTreeMap, BTreeSet};

use chronoloop::clock::{Clock, VirtualTime};

/// Arms a deadline for every controller in the fleet and hands back when each falls due.
fn arm<C: Clock>(clock: &C, rng: &mut SeededRng) -> BTreeMap<VirtualTime, u64> {
    let now = clock.now();
    let mut due = BTreeMap::new();
    let fleet: BTreeSet<u64> = (0..8).collect();
    for controller in fleet {
        let wait = rng.duration_in(SETTLING);
        due.insert(VirtualTime::from_nanos(now.as_nanos() + wait.as_nanos()), controller);
    }
    due
}
";

    let found = scan(clean);
    assert!(
        found.is_empty(),
        "ordinary engine code breaks nothing: {:?}",
        found
            .iter()
            .map(|&(_, rule)| rule.needle)
            .collect::<Vec<_>>()
    );
}

#[test]
fn the_scan_reads_the_whole_engine() {
    // A walk that quietly found nothing would leave the audit above passing with nothing scanned —
    // green because it read no source at all rather than because every source was clean.
    let found = sources();
    for expected in [
        "src/executor.rs",
        "src/systems/pingpong.rs",
        "tests/determinism.rs",
        "tests/common/mod.rs",
    ] {
        assert!(
            found.contains(&root().join(expected)),
            "the scan reads {expected}, including the files a directory down"
        );
    }
    assert!(
        found.len() >= 20,
        "the whole of the engine and its tests, not a corner of it: {} files",
        found.len()
    );

    for path in &found {
        let named = relative(path);
        assert!(
            SCANNED.iter().any(|dir| named.starts_with(dir)),
            "{} is outside what the scan is scoped to",
            named.display()
        );
    }
}

#[test]
fn the_audit_does_not_read_itself() {
    let me = self_path();
    assert!(
        !sources().contains(&me),
        "{} is the one file where these spellings belong",
        relative(&me).display()
    );

    // And the skip carries weight rather than being a precaution against nothing: every needle is
    // written out above, so reading this file would report a violation for each of them.
    let text =
        fs::read_to_string(&me).unwrap_or_else(|e| panic!("could not read {}: {e}", me.display()));
    assert!(
        !scan(&text).is_empty(),
        "without the skip this file would be the audit's own first failure"
    );
}
