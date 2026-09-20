//! Telling two of a run's states apart.
//!
//! [`crate::world`] names a state, [`crate::store`] keeps a run's states without a copy per step,
//! and [`crate::trace`] puts them in order. None of that yet says what *changed*. A recorded run
//! whose states can only be compared for equality answers "is step 37 the state step 36 was in?",
//! which is the question a hash already answers; what a person asks is which key moved, and to
//! what.
//!
//! [`diff`] is that: two [`StateHash`]es and the store they are in, and back comes every difference
//! between them, each one the path to where it is and what stands there on either side.
//!
//! # Why this costs what it changed
//!
//! A branch hashes over its children's hashes, so two subtrees that answer to one name are one
//! state and the walk below stops the moment it meets a pair of them. A run whose world has a
//! thousand fields and whose step touched one is compared by looking at the root, at the resource
//! that moved, and at the field — the rest of it is one hash comparison apiece and is never read.
//!
//! That is a saving in **cost, not in content**: a walk that descended everywhere would report
//! exactly the same changes, so no assertion below pins the stopping and this paragraph rather than
//! a test is what records it. The same is true of [`crate::store`]'s already-present shortcut, and
//! for the same reason. What *can* fail, and is asserted, is the one place the early return shows:
//! a state compares with itself without the store being asked at all, so it works against a store
//! that has never seen it.
//!
//! # The report is written, never read
//!
//! [`Recording`], [`FaultSchedule`] and [`Trace`] all read back from the form they write, because
//! each of them is an *input* somewhere: a run replays one, a wire consults one, a scrubber indexes
//! one. A comparison is an output. Nothing takes one back in, so [`Changes`] writes itself and
//! stops there — no [`FromStr`], and no strictness rules it would need to earn one.
//!
//! What the form does owe a reader is one change per line, which is why [`crate::world::Name`]
//! refuses a line ending and text is written escaped.
//!
//! ```
//! use chronoloop::diff::diff;
//! use chronoloop::store::StateStore;
//! use chronoloop::world::{Name, Resource, Snapshot, Value, World};
//!
//! let database = |replicas| {
//!     World::new().with_resource(
//!         Name::new("database").expect("a name"),
//!         Resource::new().with_field(Name::new("replicas").expect("a name"), Value::Count(replicas)),
//!     )
//! };
//!
//! let mut store = StateStore::new();
//! let before = store.insert(&database(3).snapshot());
//! let after = store.insert(&database(4).snapshot());
//!
//! let changes = diff(&store, before, after)?;
//! assert_eq!(changes.to_string(), "~ database.replicas 3 -> 4\n");
//! # Ok::<(), chronoloop::diff::MissingState>(())
//! ```
//!
//! [`FaultSchedule`]: crate::fault::FaultSchedule
//! [`FromStr`]: core::str::FromStr
//! [`Recording`]: crate::history::Recording
//! [`StateHash`]: crate::world::StateHash
//! [`Trace`]: crate::trace::Trace

use core::fmt;
use std::collections::BTreeSet;

use crate::store::{Held, StateStore};
use crate::world::{Name, StateHash, Value};

/// What a part that appeared is written with, the way a plan of infrastructure work writes it.
const ADDED: char = '+';

/// What a part that vanished is written with.
const REMOVED: char = '-';

/// What a part that is there on both sides and is not the same is written with.
const CHANGED: char = '~';

/// Where in a state a change is: the names leading down to it from the root.
///
/// Written with [`Name::SEPARATOR`] between one name and the next, which is unambiguous because a
/// name may not hold one. The state as a whole has no name of its own and is written as the
/// separator alone.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Path(Vec<Name>);

impl Path {
    /// The path of the state as a whole, which is no names at all.
    fn root() -> Self {
        Self::default()
    }

    /// Returns this path with `name` on the end of it.
    fn then(&self, name: Name) -> Self {
        let mut names = self.0.clone();
        names.push(name);
        Self(names)
    }

    /// Returns the names leading to what this path names, from the root down.
    pub fn names(&self) -> &[Name] {
        &self.0
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some((first, rest)) = self.0.split_first() else {
            return write!(f, "{}", Name::SEPARATOR);
        };
        write!(f, "{first}")?;
        for name in rest {
            write!(f, "{}{name}", Name::SEPARATOR)?;
        }
        Ok(())
    }
}

/// One side of a change, as much of it as a line of a report can hold.
///
/// A leaf is the bytes it holds, so a report can say what a field went from and to. A branch is the
/// name it answers to and nothing more: a whole subtree appearing or vanishing is one line, and a
/// reader who wants what is under it asks the store for that hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Contents {
    /// A value, as the bytes that stand for it.
    Leaf(Vec<u8>),
    /// Parts, named by the hash they answer to.
    Branch(StateHash),
}

impl fmt::Display for Contents {
    /// Reads a leaf as a [`Value`] and falls back to its bytes.
    ///
    /// A leaf holds whatever the state that wrote it chose, and only a state built out of the
    /// types in [`crate::world`] holds something this can read back. Bytes a system wrote itself
    /// are shown as bytes rather than guessed at.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leaf(bytes) => leaf(f, bytes),
            Self::Branch(hash) => write!(f, "{hash}"),
        }
    }
}

/// Writes a leaf as the value it holds, or as its bytes when it holds something else.
fn leaf(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    if let Ok(value) = Value::try_from(bytes) {
        return write!(f, "{value}");
    }
    write!(f, "0x")?;
    for byte in bytes {
        write!(f, "{byte:02x}")?;
    }
    Ok(())
}

/// One difference between two states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// A part the later state holds and the earlier did not.
    Added {
        /// Where it is.
        path: Path,
        /// What stands there now.
        now: Contents,
    },
    /// A part the earlier state held and the later does not.
    Removed {
        /// Where it was.
        path: Path,
        /// What stood there.
        was: Contents,
    },
    /// A part both states hold, and which is not the same part.
    Changed {
        /// Where it is.
        path: Path,
        /// What stood there.
        was: Contents,
        /// What stands there now.
        now: Contents,
    },
}

impl Change {
    /// Returns where in the state this change is.
    pub fn path(&self) -> &Path {
        match self {
            Self::Added { path, .. } | Self::Removed { path, .. } | Self::Changed { path, .. } => {
                path
            }
        }
    }
}

impl fmt::Display for Change {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Added { path, now } => write!(f, "{ADDED} {path} {now}"),
            Self::Removed { path, was } => write!(f, "{REMOVED} {path} {was}"),
            Self::Changed { path, was, now } => write!(f, "{CHANGED} {path} {was} -> {now}"),
        }
    }
}

/// Every difference between two states, in the order their paths sort in.
///
/// The order is the walk's rather than a sort afterwards: a branch's children are held in a
/// [`BTreeMap`], so descending one visits its children in the order their names sort in, and a
/// depth-first descent of that is path order. Determinism rule three, satisfied by the type.
///
/// [`BTreeMap`]: std::collections::BTreeMap
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Changes(Vec<Change>);

impl Changes {
    /// Returns every change, in the order their paths sort in.
    pub fn iter(&self) -> core::slice::Iter<'_, Change> {
        self.0.iter()
    }

    /// Returns how many differences there are between the two states.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the two states are one state.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'a> IntoIterator for &'a Changes {
    type Item = &'a Change;
    type IntoIter = core::slice::Iter<'a, Change>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl fmt::Display for Changes {
    /// Writes one change per line, and two states that are one state as nothing at all.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for change in &self.0 {
            writeln!(f, "{change}")?;
        }
        Ok(())
    }
}

/// Returned when a comparison is asked for a state the store never saw.
///
/// Only ever one of the two the caller named. A branch in the store cannot name a child that is
/// not, so once the walk is inside a state it is inside all of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissingState {
    /// The state that was not there.
    pub hash: StateHash,
}

impl fmt::Display for MissingState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the store never saw the state called {}", self.hash)
    }
}

impl std::error::Error for MissingState {}

/// Returns every difference between the state called `before` and the state called `after`.
///
/// Two states that answer to one name have no differences, and the store is not asked about them —
/// so a state compares with itself whether or not the store has ever seen it. Otherwise both must
/// be in the store, which is where the walk reads them from.
///
/// # Errors
///
/// Returns [`MissingState`] if either state is one the store never saw.
pub fn diff(
    store: &StateStore,
    before: StateHash,
    after: StateHash,
) -> Result<Changes, MissingState> {
    let mut changes = Vec::new();
    descend(store, &Path::root(), before, after, &mut changes)?;
    Ok(Changes(changes))
}

/// Compares the two states standing at `path` and writes what differs into `changes`.
fn descend(
    store: &StateStore,
    path: &Path,
    before: StateHash,
    after: StateHash,
    changes: &mut Vec<Change>,
) -> Result<(), MissingState> {
    // Two subtrees that answer to one name are one subtree, wherever either of them hangs. This is
    // the whole of what keeps a comparison to the size of what changed.
    if before == after {
        return Ok(());
    }
    let (was, now) = (look(store, before)?, look(store, after)?);
    let (Held::Branch(was), Held::Branch(now)) = (was, now) else {
        // Either two values, or a value where there were parts — and the kind of a node is part of
        // what a state is, so both are one change at this path rather than a descent.
        changes.push(Change::Changed {
            path: path.clone(),
            was: contents(store, before)?,
            now: contents(store, after)?,
        });
        return Ok(());
    };

    let names: BTreeSet<&Name> = was.keys().chain(now.keys()).collect();
    for name in names {
        let path = path.then(name.clone());
        match (was.get(name), now.get(name)) {
            (Some(before), Some(after)) => descend(store, &path, *before, *after, changes)?,
            (Some(before), None) => changes.push(Change::Removed {
                path,
                was: contents(store, *before)?,
            }),
            (None, Some(after)) => changes.push(Change::Added {
                path,
                now: contents(store, *after)?,
            }),
            // Cannot arise: every name here came from one side or the other.
            (None, None) => {}
        }
    }
    Ok(())
}

/// Returns what the store holds under `hash`, or says that it holds nothing under it.
fn look(store: &StateStore, hash: StateHash) -> Result<Held<'_>, MissingState> {
    store.held(hash).ok_or(MissingState { hash })
}

/// Returns as much of the state called `hash` as a line of a report can hold.
fn contents(store: &StateStore, hash: StateHash) -> Result<Contents, MissingState> {
    Ok(match look(store, hash)? {
        Held::Leaf(bytes) => Contents::Leaf(bytes.to_vec()),
        Held::Branch(_) => Contents::Branch(hash),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::clock::VirtualTime;
    use crate::world::{Node, Resource, Snapshot, World};

    /// A name, failing the test rather than returning an error no case expects.
    fn name(text: &str) -> Name {
        Name::new(text).unwrap_or_else(|e| panic!("a test name is a name: {e}"))
    }

    /// The world every case below is a small change away from.
    fn base() -> World {
        World::new()
            .with_resource(
                name("database"),
                Resource::new()
                    .with_field(name("replicas"), Value::Count(3))
                    .with_field(name("primary"), Value::Text("eu-west".into())),
            )
            .with_resource(
                name("cache"),
                Resource::new().with_field(name("warm"), Value::Flag(true)),
            )
    }

    /// Compares two states, keeping both in a store first, and hands back what a reader is shown.
    ///
    /// The lines rather than the values: an expectation written as a `Change` would be built out of
    /// the same calls the walk makes, so the two sides of the assertion would come from one place.
    fn lines(before: &Node, after: &Node) -> Vec<String> {
        let mut store = StateStore::new();
        let before = store.insert(before);
        let after = store.insert(after);
        diff(&store, before, after)
            .unwrap_or_else(|e| panic!("both states were just stored: {e}"))
            .to_string()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn what_changed_is_named_by_the_path_down_to_it() {
        struct Case {
            name: &'static str,
            after: World,
            want: Vec<&'static str>,
        }
        let mut emptied = base();
        let cache = emptied
            .remove(&name("cache"))
            .unwrap_or_else(|| panic!("the world these cases start from holds a cache"));

        let cases = [
            Case {
                name: "nothing at all",
                after: base(),
                want: vec![],
            },
            Case {
                name: "a count raised",
                after: base().with_resource(
                    name("database"),
                    Resource::new()
                        .with_field(name("replicas"), Value::Count(4))
                        .with_field(name("primary"), Value::Text("eu-west".into())),
                ),
                want: vec!["~ database.replicas 3 -> 4"],
            },
            Case {
                name: "two fields of one resource",
                after: base().with_resource(
                    name("database"),
                    Resource::new()
                        .with_field(name("replicas"), Value::Count(4))
                        .with_field(name("primary"), Value::Text("eu-east".into())),
                ),
                want: vec![
                    "~ database.primary \"eu-west\" -> \"eu-east\"",
                    "~ database.replicas 3 -> 4",
                ],
            },
            Case {
                name: "a field added",
                after: base().with_resource(
                    name("cache"),
                    Resource::new()
                        .with_field(name("warm"), Value::Flag(true))
                        .with_field(name("entries"), Value::Count(0)),
                ),
                want: vec!["+ cache.entries 0"],
            },
            Case {
                name: "a field taken away",
                after: base().with_resource(name("cache"), Resource::new()),
                want: vec!["- cache.warm true"],
            },
            Case {
                name: "a resource added",
                after: base().with_resource(
                    name("queue"),
                    Resource::new().with_field(name("depth"), Value::Count(9)),
                ),
                want: vec![
                    "+ queue 6e7a2f15f5c0087a719c41fc09c99df738eb32e0dc55fae77baec08bc650a96f",
                ],
            },
            Case {
                name: "a resource taken away",
                after: emptied.clone(),
                want: vec![
                    "- cache 254019f1c87b64de5eb3bb6653e125ae31a3dff032775232fb930d96627ba637",
                ],
            },
            Case {
                name: "a resource under another name",
                after: emptied.with_resource(name("caches"), cache),
                want: vec![
                    "- cache 254019f1c87b64de5eb3bb6653e125ae31a3dff032775232fb930d96627ba637",
                    "+ caches 254019f1c87b64de5eb3bb6653e125ae31a3dff032775232fb930d96627ba637",
                ],
            },
            Case {
                name: "an instant moved",
                after: base().with_resource(
                    name("queue"),
                    Resource::new().with_field(
                        name("drained"),
                        Value::Instant(VirtualTime::from_nanos(1_500_000_000)),
                    ),
                ),
                want: vec![
                    "+ queue 3125607a274d022f2bdcef7cfa627349d7587900fec061364dba4e4ae1b166e7",
                ],
            },
        ];
        for case in cases {
            assert_eq!(
                lines(&base().snapshot(), &case.after.snapshot()),
                case.want,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_part_that_did_not_change_is_reported_nowhere() {
        // The claim the whole shape of a state exists for: the cache is in both states and is
        // named in neither line, because the two subtrees answer to one name.
        let after = base().with_resource(
            name("database"),
            Resource::new()
                .with_field(name("replicas"), Value::Count(4))
                .with_field(name("primary"), Value::Text("eu-west".into())),
        );
        assert_eq!(
            lines(&base().snapshot(), &after.snapshot()),
            vec!["~ database.replicas 3 -> 4"]
        );
    }

    #[test]
    fn a_state_compares_with_itself_without_the_store_being_asked() {
        // The one place the early return shows. A comparison that looked the states up first would
        // have nothing to look up here and would report the state missing instead.
        let hash = base().state_hash();
        let changes = diff(&StateStore::new(), hash, hash)
            .unwrap_or_else(|e| panic!("a state is itself whoever has seen it: {e}"));
        assert!(changes.is_empty());
        assert_eq!(changes.len(), 0);
        assert_eq!(changes.to_string(), "");
    }

    #[test]
    fn a_state_the_store_never_saw_is_named_rather_than_guessed_at() {
        let mut store = StateStore::new();
        let known = store.insert(&base().snapshot());
        let unknown = World::new().state_hash();

        assert_eq!(
            diff(&store, unknown, known),
            Err(MissingState { hash: unknown }),
            "the state compared against"
        );
        assert_eq!(
            diff(&store, known, unknown),
            Err(MissingState { hash: unknown }),
            "the state compared with"
        );
        assert_eq!(
            MissingState { hash: unknown }.to_string(),
            format!("the store never saw the state called {unknown}")
        );
    }

    #[test]
    fn a_part_that_changed_kind_is_one_change_and_not_a_descent() {
        // A world is always a branch of branches of leaves, so this is reached with nodes built by
        // hand. The kind of a node is the first thing in its encoding, and so part of what the
        // state is: parts where there was a value is not two states that happen to differ
        // underneath, it is one thing standing where another stood.
        let leaf = Node::Leaf(vec![0x01, 7, 0, 0, 0, 0, 0, 0, 0]);
        let branch = Node::Branch(
            [(
                name("depth"),
                Node::Leaf(vec![0x01, 9, 0, 0, 0, 0, 0, 0, 0]),
            )]
            .into_iter()
            .collect(),
        );
        let under = |child: Node| {
            Node::Branch(
                [(name("queue"), child)]
                    .into_iter()
                    .collect::<BTreeMap<_, _>>(),
            )
        };

        assert_eq!(
            lines(&under(leaf.clone()), &under(branch.clone())),
            vec![format!("~ queue 7 -> {}", branch.state_hash())],
            "a value giving way to parts"
        );
        assert_eq!(
            lines(&under(branch.clone()), &under(leaf)),
            vec![format!("~ queue {} -> 7", branch.state_hash())],
            "and parts giving way to a value"
        );
    }

    #[test]
    fn bytes_no_value_was_written_as_are_shown_as_bytes() {
        // A state built somewhere else holds whatever its author chose. A report shows what is
        // there rather than guessing at a kind it does not recognise.
        let under = |bytes: Vec<u8>| {
            Node::Branch(
                [(name("blob"), Node::Leaf(bytes))]
                    .into_iter()
                    .collect::<BTreeMap<_, _>>(),
            )
        };
        assert_eq!(
            lines(&under(vec![0xde, 0xad]), &under(vec![0xbe, 0xef])),
            vec!["~ blob 0xdead -> 0xbeef"]
        );
    }

    #[test]
    fn a_change_is_one_line_however_the_text_in_it_is_written() {
        // A report a reader reads by the line cannot hold a value that breaks the line. A name
        // could not carry one to begin with; text is written escaped so it cannot either.
        let text = |what: &str| {
            World::new().with_resource(
                name("database"),
                Resource::new().with_field(name("note"), Value::Text(what.into())),
            )
        };
        assert_eq!(
            lines(&text("one").snapshot(), &text("two\nlines").snapshot()),
            vec!["~ database.note \"one\" -> \"two\\nlines\""]
        );
    }

    #[test]
    fn the_state_as_a_whole_is_written_as_the_separator_alone() {
        // Reached only by two roots of different kinds, since a root has no name of its own.
        assert_eq!(
            lines(&Node::Leaf(vec![0x00, 1]), &Node::Branch(BTreeMap::new())),
            vec![format!(
                "~ . true -> {}",
                Node::Branch(BTreeMap::new()).state_hash()
            )]
        );
        assert_eq!(Path::root().to_string(), ".");
    }

    #[test]
    fn changes_come_out_in_the_order_their_paths_sort_in() {
        // Not a sort afterwards — the walk descends a `BTreeMap`, so this is the order it visits
        // in. A report a person scrubs through has to read the same way twice.
        let resource = |field: &str| Resource::new().with_field(name(field), Value::Count(1));
        let mut after = World::new();
        for called in ["gamma", "alpha", "beta"] {
            after.insert(name(called), resource(called));
        }
        let before = World::new();

        let changes = lines(&before.snapshot(), &after.snapshot());
        let paths: Vec<_> = changes
            .iter()
            .map(|line| {
                line.split(' ')
                    .nth(1)
                    .unwrap_or_else(|| panic!("a change is a sigil, a path and what stands there"))
                    .to_owned()
            })
            .collect();
        assert_eq!(paths, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn a_comparison_reads_by_the_change_as_well_as_by_the_line() {
        // The values behind the report, for a caller that means to act on them rather than print
        // them — which is what an invariant checking a run will do.
        let mut store = StateStore::new();
        let before = store.insert(&base().snapshot());
        let after = store.insert(
            &base()
                .with_resource(name("cache"), Resource::new())
                .snapshot(),
        );
        let changes = diff(&store, before, after)
            .unwrap_or_else(|e| panic!("both states were just stored: {e}"));

        assert_eq!(changes.len(), 1);
        let change = changes
            .iter()
            .next()
            .unwrap_or_else(|| panic!("one change is there to be read"));
        assert_eq!(
            change.path().names(),
            [name("cache"), name("warm")],
            "the path is the names down to what moved"
        );
        assert_eq!(
            change,
            &Change::Removed {
                path: change.path().clone(),
                was: Contents::Leaf(vec![0x00, 1]),
            },
            "and the flag that was there is the bytes a flag that is on is written as"
        );
        assert_eq!(
            (&changes).into_iter().count(),
            1,
            "a comparison iterates by reference"
        );
    }
}
