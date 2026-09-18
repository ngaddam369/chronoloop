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
//! In two places this is deliberately stricter than the rule it enforces. A seed sweep is entitled
//! to real threads — each seed is an independent single-threaded run, and nothing about that makes
//! a history depend on how the operating system interleaved them — but the four thread spellings
//! below are forbidden in every file the scan reads, sweeps included. A scan cannot tell a sweep
//! from a simulation; the exemption would have to be a file's name, and a file's name is a poor
//! account of what the code in it does. It is the same argument that forbids `HashMap` in all of
//! `src/` rather than only where a simulation can see it. When a sweep here wants threads, this is
//! the decision to re-open — visibly, in a commit that says which file is being trusted and why,
//! rather than by widening a needle until the build goes quiet.
//!
//! The other is `UNIX_EPOCH`, which is a constant and reads nothing: an instant rendered against it
//! is as deterministic as the instant was. What reaches the machine is an elapsed or a duration
//! measured from it, and a scan cannot tell one from the other, so the name is forbidden outright.
//! A simulation has no call for the wall clock's origin either way — a recorded instant is a
//! `VirtualTime`, which carries its own.
//!
//! A match counts wherever it falls, comments included. Stripping them first would need a
//! heuristic that is wrong about `//` inside a string literal, and would give a violation a place
//! to hide; the price is that these names are spelled in one file in the repository, and this is
//! it. Prose elsewhere has to say what it means in words.

use std::collections::BTreeSet;
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
static RULES: [Rule; 25] = [
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
        needle: "UNIX_EPOCH",
        jewel: VIRTUAL_TIME,
        why: "names the wall clock's own origin, which an elapsed or a duration since it is the \
              machine's answer measured against; forbidden outright rather than only where it is \
              read, because a scan cannot tell the two apart",
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
        needle: "as_ptr_range",
        jewel: NO_ADDRESSES,
        why: "hands out two addresses at once, as the ends of a range; a longer name that the \
              needle above deliberately does not claim, so it has to be named here",
    },
    Rule {
        needle: "as_mut_ptr",
        jewel: NO_ADDRESSES,
        why: "the same address, from the buffer's other accessor",
    },
    Rule {
        needle: "as_mut_ptr_range",
        jewel: NO_ADDRESSES,
        why: "the same range, through pointers that could also write",
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
        needle: "addr_of!",
        jewel: NO_ADDRESSES,
        why: "takes an address in safe code, so forbidding `unsafe` does not keep one out",
    },
    Rule {
        needle: "addr_of_mut!",
        jewel: NO_ADDRESSES,
        why: "the same, through a pointer that could also write",
    },
    Rule {
        needle: "&raw ",
        jewel: NO_ADDRESSES,
        why: "this edition's own spelling of the two above, and so the one an author here would \
              reach for; it covers a shared raw borrow and an exclusive one alike",
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
static VIOLATIONS: [Violation; 25] = [
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
        name: "the wall clock by way of the epoch",
        source: "    let stamped = SystemTime::UNIX_EPOCH.elapsed()?.as_nanos();",
        needle: "UNIX_EPOCH",
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
        source: "    use rand::rngs::OsRng;",
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
        name: "a pair of addresses, taken as a range",
        source: "    let bounds = slice.as_ptr_range();",
        needle: "as_ptr_range",
    },
    Violation {
        name: "an identity taken from an address that can be written through",
        source: "    let identity = buffer.as_mut_ptr() as usize;",
        needle: "as_mut_ptr",
    },
    Violation {
        name: "the same range, through pointers that can write",
        source: "    let bounds = buffer.as_mut_ptr_range();",
        needle: "as_mut_ptr_range",
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
        name: "an address taken without unsafe",
        source: "    let identity = std::ptr::addr_of!(shared) as usize;",
        needle: "addr_of!",
    },
    Violation {
        name: "the same, through a pointer that can write",
        source: "    let raw = std::ptr::addr_of_mut!(queue);",
        needle: "addr_of_mut!",
    },
    Violation {
        name: "an address in the spelling this edition encourages",
        source: "    let identity = &raw const shared as usize;",
        needle: "&raw ",
    },
    Violation {
        name: "an address printed into a history",
        source: "    history.record(clock, format!(\"polling task at {:p}\", &task));",
        needle: "{:p}",
    },
];

/// A line sitting on the boundary between a needle and a longer name that begins with it.
struct Boundary {
    /// What the line is, for the assertion that names it.
    name: &'static str,
    /// The line itself.
    source: &'static str,
    /// Every needle that must fire on it, in the order the table above holds them — and nothing
    /// else, so a case says both what is caught and what is left alone.
    caught: &'static [&'static str],
}

/// The pairs that make the distinction worth drawing: a forbidden spelling, and a longer name it
/// sits inside — at the front of one, where the longer name is something else, and at the end of
/// one, where the longer name is the forbidden thing wearing a prefix.
static BOUNDARIES: [Boundary; 8] = [
    Boundary {
        name: "the generator a run may not have",
        source: "    let delay: u64 = rand::rng().random_range(1..=100);",
        caught: &["rand::rng"],
    },
    Boundary {
        name: "a generator a seed can build, whose path begins with that spelling",
        source: "    let mut generator = rand::rngs::StdRng::seed_from_u64(seed);",
        caught: &[],
    },
    Boundary {
        name: "an unordered map wearing a prefix, which is still that map",
        source: "    let counts: AHashMap<NodeId, u64> = AHashMap::default();",
        caught: &["HashMap"],
    },
    Boundary {
        name: "an address, where the needle is the whole name",
        source: "    let identity = Rc::as_ptr(&shared) as usize;",
        caught: &["as_ptr"],
    },
    Boundary {
        name: "the accessor beside it, which no shorter needle may claim",
        source: "    let identity = buffer.as_mut_ptr() as usize;",
        caught: &["as_mut_ptr"],
    },
    Boundary {
        name: "the range accessor, which is a longer name and so needs one of its own",
        source: "    let bounds = buffer.as_ptr_range();",
        caught: &["as_ptr_range"],
    },
    Boundary {
        name: "a raw borrow through a pointer that can write",
        source: "    let raw = std::ptr::addr_of_mut!(queue);",
        caught: &["addr_of_mut!"],
    },
    Boundary {
        name: "a braced import, which ends in punctuation and so begins no name",
        source: "    use std::thread::{sleep, spawn};",
        caught: &["thread::{"],
    },
];

/// Whether a character continues a name rather than ending one.
fn continues_a_name(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Whether `needle` names something in `line`, rather than beginning a longer name.
///
/// `rand::rng()` is the generator backed by the operating system; `rand::rngs::StdRng` is one a
/// seed can build. A `contains` cannot tell them apart, so it reports the second under the first's
/// jewel — a violation announced against code that broke nothing. A needle ending in a name
/// character therefore counts only where the next character does not continue that name. One
/// ending in punctuation — `thread::{`, `as *mut`, `addr_of!`, `{:p}` — has no name to continue and
/// counts wherever it falls, which is what keeps `use std::thread::{sleep, spawn};` caught.
///
/// Only that end is guarded, and the asymmetry is the point rather than half a job. A needle that
/// begins a longer name is usually a different thing — a path carrying on into somewhere else,
/// which is the case above. A needle that *ends* one is usually the forbidden thing wearing a
/// prefix: `AHashMap` is a map whose order comes from the operating system, and anything named
/// `..._as_ptr` or `..._thread_rng` is a wrapper around the call it is named after. Guarding the
/// leading side would buy one contrived false positive at the cost of missing those, so it is left
/// alone; the case below holds that decision up.
fn names(line: &str, needle: &str) -> bool {
    let bounded = needle.ends_with(continues_a_name);
    line.match_indices(needle).any(|(at, _)| {
        !bounded
            || line[at + needle.len()..]
                .chars()
                .next()
                .is_none_or(|c| !continues_a_name(c))
    })
}

/// Every rule broken by `text`, as a one-based line number and the rule it broke.
fn scan(text: &str) -> Vec<(usize, &'static Rule)> {
    let mut broken = Vec::new();
    for (index, line) in text.lines().enumerate() {
        for rule in &RULES {
            if names(line, rule.needle) {
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
///
/// The last two components of `file!()`, not the whole of it: `file!()` is spelled relative to the
/// crate in a single crate and relative to the workspace root in a workspace, so joining all of it
/// to the crate root would name a file that is not here the day the layout changed. The skip would
/// then stop matching and the audit would report every needle in the table against itself, which
/// says nothing about the engine and points the reader at the wrong file.
fn self_path() -> PathBuf {
    let named = Path::new(file!());
    match (named.parent().and_then(Path::file_name), named.file_name()) {
        (Some(directory), Some(file)) => root().join(directory).join(file),
        _ => root().join(named),
    }
}

/// `path` with every link on it resolved, or `path` itself where it cannot be.
///
/// Falling back rather than failing: a link naming a file that is not there resolves to nothing,
/// and a walk that panics on one would be stopped by a piece of debris in `target/` that has no
/// bearing on what the engine says.
fn resolved(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// `path` as a reader would name it, rather than from the root of the filesystem.
fn relative(path: &Path) -> &Path {
    path.strip_prefix(root()).unwrap_or(path)
}

/// Adds every Rust file at or under `dir` to `found`, leaving out `skip`, which arrives resolved.
///
/// `skip` is compared against what each file *is* rather than against the name the walk arrived by,
/// or a link to it under another name would be read — and the file this scan leaves out is the one
/// every forbidden spelling is written in, so reading it under an alias reports the whole table
/// against the wrong file. `found` keeps the walked name rather than the resolved one, because
/// every path in this file is spelled relative to the crate root, which is itself unresolved: a
/// checkout reached through a link would otherwise report violations from the root of the
/// filesystem instead.
///
/// `seen` holds the directories already walked, under the path each one really is. A symbolic link
/// pointing back up the tree sends the walk round again: measured here, the same source was read
/// forty-one times before the kernel's own limit on resolving a link ended it, and every violation
/// in it would have been reported forty-one times. A deeper tree ends on the stack instead. Declining
/// to follow a link would stop the recursion just as well, and was not chosen: a directory the
/// build compiles is a directory this scan has to read, whatever the filesystem calls it, and a
/// violation with somewhere to hide is the other way to report nothing.
fn collect(dir: &Path, skip: &Path, seen: &mut BTreeSet<PathBuf>, found: &mut Vec<PathBuf>) {
    let real = fs::canonicalize(dir)
        .unwrap_or_else(|e| panic!("could not resolve {}: {e}", dir.display()));
    if !seen.insert(real) {
        return;
    }

    let entries =
        fs::read_dir(dir).unwrap_or_else(|e| panic!("could not read {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("could not read {}: {e}", dir.display()));
        let path = entry.path();
        if path.is_dir() {
            collect(&path, skip, seen, found);
        } else if path.extension().is_some_and(|kind| kind == "rs") && resolved(&path) != *skip {
            found.push(path);
        }
    }
}

/// Every Rust file the scan reads, in an order that does not come from the filesystem.
fn sources() -> Vec<PathBuf> {
    let skip = resolved(&self_path());
    let mut seen = BTreeSet::new();
    let mut found = Vec::new();
    for dir in SCANNED {
        collect(&root().join(dir), &skip, &mut seen, &mut found);
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
    // needle added later stays as honest as the ones that came with it.
    for rule in &RULES {
        assert!(
            VIOLATIONS.iter().any(|case| case.needle == rule.needle),
            "`{}` has no line showing it catches anything",
            rule.needle
        );
    }
}

#[test]
fn a_needle_matches_a_name_rather_than_the_start_of_one() {
    // A needle that merely begins a longer name reports the longer name, and reports it under a
    // jewel it did not break — a generator built from the run's seed announced as the operating
    // system's entropy. That is worse than a miss: the next person to see it learns that the audit
    // cries wolf, and the fix they reach for is a weaker needle.
    for case in &BOUNDARIES {
        let caught: Vec<&str> = scan(case.source)
            .iter()
            .map(|&(_, rule)| rule.needle)
            .collect();
        assert_eq!(caught, case.caught, "{}", case.name);
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

    assert!(
        me.is_file(),
        "{} is not a file here: `file!()` is spelled relative to the crate in a single crate and \
         relative to the workspace root in a workspace, and a skip naming a file that does not \
         exist skips nothing",
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

#[cfg(unix)]
#[test]
#[ignore = "plants real directories and a symbolic link, so `make local-validation` runs it"]
fn the_walk_reads_a_tree_that_points_back_at_itself() {
    use std::os::unix::fs::symlink;

    // Under `target/` rather than a directory the operating system chooses: the path is then the
    // same on every machine and in both profiles, and nothing outside the crate is written to.
    let planted = root().join("target").join("audit-walk-loop");
    let deeper = planted.join("deeper");
    clear(&planted);

    fs::create_dir_all(&deeper)
        .unwrap_or_else(|e| panic!("could not create {}: {e}", deeper.display()));
    let real = deeper.join("controller.rs");
    fs::write(&real, "fn reconcile() {}\n")
        .unwrap_or_else(|e| panic!("could not write {}: {e}", real.display()));
    // A link back to the top of the tree, which is the shape that makes a walk unbounded.
    let upwards = deeper.join("upwards");
    symlink(&planted, &upwards)
        .unwrap_or_else(|e| panic!("could not link {}: {e}", upwards.display()));
    // And a link to a file, which is read: the fix is a walk that cannot go round for ever, not a
    // walk that declines to look at anything a link names.
    let linked = planted.join("linked.rs");
    symlink(&real, &linked).unwrap_or_else(|e| panic!("could not link {}: {e}", linked.display()));
    // And a link to the one file the scan leaves out, under a name that is not the one the skip is
    // spelled as. What a file is has to settle that, rather than what this walk happened to call it
    // — otherwise the audit reads itself under an alias and reports every needle in the table
    // against the file they are all written in.
    let mirror = planted.join("mirror.rs");
    symlink(self_path(), &mirror)
        .unwrap_or_else(|e| panic!("could not link {}: {e}", mirror.display()));

    let mut seen = BTreeSet::new();
    let mut found = Vec::new();
    collect(&planted, &resolved(&self_path()), &mut seen, &mut found);
    found.sort();

    // Cleared before the assertion rather than after it: a failing assertion is exactly when the
    // link pointing back up the tree must not be left lying in `target/`, where the next thing to
    // walk it following links goes round until the kernel stops it.
    clear(&planted);

    assert_eq!(
        found,
        vec![real, linked],
        "the source under the loop is read once, the file a link names is read, and the skipped \
         file stays skipped under a name it does not have"
    );
}

/// Removes `dir` and everything under it, and says so rather than swallowing a failure.
#[cfg(unix)]
fn clear(dir: &Path) {
    match fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("could not clear {}: {e}", dir.display()),
    }
}
