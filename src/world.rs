//! The state a run carries, and the name that state answers to.
//!
//! Determinism makes a failing run repeatable. It does not make it *inspectable*: to ask what the
//! world looked like at step 37, what changed between 36 and 37, and what would have happened had
//! the run gone on differently from there, a run has to carry a state that can be named. That name
//! is a [`StateHash`] — the hash of a canonical serialization, so two states are the same state
//! exactly when they hash alike, on any machine, in any build.
//!
//! # The shape, and why it is a tree
//!
//! A [`Snapshot`] hands back a [`Node`], which is either a leaf of bytes or a branch of named
//! children. A branch's hash is taken over its children's **hashes**, never over their bytes, and
//! three things the rest of this depends on fall out of that:
//!
//! - An unchanged subtree keeps its hash wherever it sits, so a store can keep one copy of it
//!   however many steps refer to it — a thousand-step run does not cost a thousand full copies.
//! - A comparison of two states descends only where two child hashes differ, so it can say *which
//!   key* changed rather than only that something did.
//! - A subtree's identity does not depend on where it hangs, so the same resource seen under two
//!   names, or at two steps, is recognisably the same thing.
//!
//! # The encoding
//!
//! Written out, since a change to it changes every state hash the engine has ever produced and is
//! therefore a decision rather than an implementation detail:
//!
//! ```text
//! leaf   0x00 ++ (byte count as u64, little endian) ++ bytes
//! branch 0x01 ++ (child count as u64, little endian)
//!             ++ for each child, in ascending order of name:
//!                  (name length as u64, little endian) ++ name ++ the child's 32-byte hash
//! ```
//!
//! Every piece of it earns its place. The leading byte keeps a leaf of no bytes from hashing like a
//! branch of no children. Children are held in a [`BTreeMap`], so their order is their names' order
//! rather than the order something inserted them: determinism rule three, satisfied by the type
//! rather than by care.
//!
//! The lengths are worth being exact about, because it would be easy to claim more for them than
//! they do. As the encoding stands they are not what keeps two states apart: a child's hash is 32
//! bytes whatever it holds, so each node's encoding has only one run of variable-length bytes in it
//! and there is nothing for a name to run into — two branches could collide only through a name
//! chosen to line up with a neighbour's hash, which is a preimage search rather than an accident.
//! What the lengths buy is that the argument does not have to be made again every time the encoding
//! grows a field. They are the discipline, not the defence.
//!
//! # Reading a leaf back
//!
//! A [`Value`]'s half of the encoding has an inverse, [`Value::try_from`], so a comparison of two
//! states can say a field went from three to four rather than that two hashes differ. It lives here
//! beside what it undoes, because a decoder kept anywhere else is a second copy of the encoding to
//! keep in step — the same reason [`crate::store`] never spells the encoding out again. It is
//! strict where the encoding is exact, so no two runs of bytes read back as one value.
//!
//! What it does **not** do is read a branch. A branch's children are hashes rather than bytes, so
//! what is under one is a question for a store rather than for a decoder, and a report that meets
//! a branch prints the name it answers to and leaves the descent to whoever wants it.

use core::fmt;
use core::str::FromStr;
use std::collections::BTreeMap;
use std::collections::btree_map;

use crate::clock::VirtualTime;

/// How many bytes a hash takes.
const HASH_LEN: usize = 32;

/// How many characters a hash is written as.
const HASH_DIGITS: usize = HASH_LEN * 2;

/// Marks a leaf in the encoding, so a leaf of no bytes cannot hash like a branch of no children.
const LEAF_TAG: u8 = 0x00;

/// Marks a branch in the encoding.
const BRANCH_TAG: u8 = 0x01;

/// The name a state answers to: the hash of its canonical serialization.
///
/// Written as 64 lowercase hexadecimal digits, which is the form a recorded trace keeps it in.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateHash([u8; HASH_LEN]);

impl StateHash {
    /// Returns the hash's bytes, which a branch holding this state as a child hashes over.
    fn as_bytes(&self) -> &[u8; HASH_LEN] {
        &self.0
    }
}

impl fmt::Display for StateHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for StateHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The derived form prints thirty-two numbers, which is unreadable in a failed assertion.
        write!(f, "StateHash({self})")
    }
}

/// Errors returned when reading a [`StateHash`] back from the form [`Display`] writes.
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseStateHashError {
    /// The text is not the length a hash is written as.
    BadLength {
        /// How many characters it held.
        found: usize,
    },
    /// The text is the right length but is not lowercase hexadecimal throughout.
    BadDigit,
}

impl fmt::Display for ParseStateHashError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadLength { found } => write!(
                f,
                "a state hash is {HASH_DIGITS} characters, and this is {found}"
            ),
            Self::BadDigit => write!(
                f,
                "a state hash is written in lowercase hexadecimal digits only"
            ),
        }
    }
}

impl std::error::Error for ParseStateHashError {}

impl FromStr for StateHash {
    type Err = ParseStateHashError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() != HASH_DIGITS {
            return Err(ParseStateHashError::BadLength { found: text.len() });
        }
        let mut bytes = [0_u8; HASH_LEN];
        // The length is settled above, so every byte of the text falls in a pair and the remainder
        // is empty.
        let (pairs, _) = text.as_bytes().as_chunks::<2>();
        for (byte, digits) in bytes.iter_mut().zip(pairs) {
            // Uppercase is refused rather than accepted, so a state has one written form and a
            // file holding two spellings of one hash cannot arise.
            if digits.iter().any(u8::is_ascii_uppercase) {
                return Err(ParseStateHashError::BadDigit);
            }
            let pair = core::str::from_utf8(digits).map_err(|_| ParseStateHashError::BadDigit)?;
            *byte = u8::from_str_radix(pair, 16).map_err(|_| ParseStateHashError::BadDigit)?;
        }
        Ok(Self(bytes))
    }
}

/// Returned when a name could not be used as one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NameError {
    /// The name is empty, so nothing could refer to what it names.
    Empty,
    /// The name carries a line ending.
    NotOneLine {
        /// The name that was refused.
        name: String,
    },
    /// The name carries the character that divides one name from the next in a path.
    NotOnePart {
        /// The name that was refused.
        name: String,
    },
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "a name cannot be empty"),
            // The name is written in its escaped form, so a line ending shows up as one.
            Self::NotOneLine { name } => {
                write!(f, "a name must be a single line: {name:?}")
            }
            Self::NotOnePart { name } => {
                write!(f, "a name must be one part of a path: {name:?}")
            }
        }
    }
}

impl std::error::Error for NameError {}

/// What one part of a state is called.
///
/// A name is a key, and a key is what a comparison of two states prints as the path to what
/// changed. Two spellings would break that report, so both are settled here rather than left to
/// whoever writes it: a name holding a line ending would break the line it is printed on, the same
/// way a two-line message would break a recorded history, and a name holding [`Name::SEPARATOR`]
/// would read as two names once the path around it was written out. A path is the only thing that
/// tells a reader *where* a state changed, so a path that reads as something it is not is worse
/// than no path at all.
///
/// Nothing about this reaches the encoding — a name is written there with its length in front of
/// it, so what a name may hold is a question about the report and never about the hash.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Name(String);

impl Name {
    /// What divides one name from the next where a path is written out.
    pub const SEPARATOR: char = '.';

    /// Creates a name.
    ///
    /// # Errors
    ///
    /// Returns [`NameError`] if `name` is empty, carries a line ending, or carries
    /// [`Name::SEPARATOR`].
    pub fn new(name: impl Into<String>) -> Result<Self, NameError> {
        let name = name.into();
        if name.is_empty() {
            return Err(NameError::Empty);
        }
        if name.contains(['\n', '\r']) {
            return Err(NameError::NotOneLine { name });
        }
        if name.contains(Self::SEPARATOR) {
            return Err(NameError::NotOnePart { name });
        }
        Ok(Self(name))
    }

    /// Returns the name as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Name {
    type Err = NameError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::new(text)
    }
}

/// Marks a flag in a value's encoding, so two values of different kinds cannot hash alike.
const FLAG_TAG: u8 = 0x00;

/// Marks a count in a value's encoding.
const COUNT_TAG: u8 = 0x01;

/// Marks text in a value's encoding.
const TEXT_TAG: u8 = 0x02;

/// Marks an instant in a value's encoding.
const INSTANT_TAG: u8 = 0x03;

/// What one field of a resource holds.
///
/// Each kind carries a tag of its own into the encoding, so a count of nothing, a flag that is off,
/// empty text and the start of the simulation are four states rather than one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Something a resource either is or is not.
    Flag(bool),
    /// How many of something there are.
    Count(u64),
    /// What something is called, or says.
    Text(String),
    /// An instant of the run's simulated time.
    Instant(VirtualTime),
}

impl fmt::Display for Value {
    /// Writes a value the way a comparison of two states shows one.
    ///
    /// Text is written in its escaped form and in quotes: escaped because a report is read by the
    /// line and text holding a line ending would break the line it is shown on, and quoted so that
    /// empty text shows as something and so a count is never read as the text of its digits.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Flag(flag) => write!(f, "{flag}"),
            Self::Count(count) => write!(f, "{count}"),
            Self::Text(text) => write!(f, "{text:?}"),
            Self::Instant(at) => write!(f, "{at}"),
        }
    }
}

/// Errors returned when reading a [`Value`] back from the bytes a leaf holds.
///
/// A leaf's bytes are whatever the state that wrote them chose, so these say that the bytes are not
/// a value *this* module wrote rather than that something is broken. A report reading a state built
/// somewhere else falls back to showing the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeValueError {
    /// There are no bytes, so they name no kind.
    Empty,
    /// The leading byte is not a kind the encoding writes.
    UnknownKind {
        /// The byte that was found in place of a kind.
        tag: u8,
    },
    /// The bytes after the kind are not as many as that kind is written with.
    BadLength {
        /// How many the kind is written with.
        expected: usize,
        /// How many there were.
        found: usize,
    },
    /// A flag is written as a byte that is zero or one, and this is neither.
    BadFlag {
        /// The byte that was found in place of a flag.
        found: u8,
    },
    /// Text is written as UTF-8, and this is not.
    BadText,
}

impl fmt::Display for DecodeValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "a value opens with the kind it is, and this is empty"),
            Self::UnknownKind { tag } => write!(f, "no value is written with a kind of {tag:#04x}"),
            Self::BadLength { expected, found } => write!(
                f,
                "this kind of value is written in {expected} bytes, and this is {found}"
            ),
            Self::BadFlag { found } => {
                write!(f, "a flag is written as 0 or 1, and this is {found}")
            }
            Self::BadText => write!(f, "text is written as UTF-8, and this is not"),
        }
    }
}

impl std::error::Error for DecodeValueError {}

/// Reads the eight bytes a count and an instant are each written as.
fn eight(bytes: &[u8]) -> Result<[u8; 8], DecodeValueError> {
    bytes.try_into().map_err(|_| DecodeValueError::BadLength {
        expected: 8,
        found: bytes.len(),
    })
}

impl TryFrom<&[u8]> for Value {
    type Error = DecodeValueError;

    /// Reads back what [`Snapshot::snapshot`] wrote, and refuses anything else.
    ///
    /// Strict in the places the encoding is exact, so no two runs of bytes read back as one value:
    /// a flag is one byte that is zero or one, a count and an instant are eight bytes each, and a
    /// kind the encoding does not write is not guessed at.
    ///
    /// ```
    /// use chronoloop::world::{Node, Snapshot, Value};
    ///
    /// let Node::Leaf(bytes) = Value::Count(3).snapshot() else {
    ///     unreachable!("a value is a leaf")
    /// };
    /// assert_eq!(Value::try_from(bytes.as_slice()), Ok(Value::Count(3)));
    ///
    /// // Bytes a system wrote itself are not a value this module knows.
    /// assert!(Value::try_from(b"anything".as_slice()).is_err());
    /// ```
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let (tag, rest) = bytes.split_first().ok_or(DecodeValueError::Empty)?;
        match *tag {
            FLAG_TAG => match rest {
                [0] => Ok(Self::Flag(false)),
                [1] => Ok(Self::Flag(true)),
                [found] => Err(DecodeValueError::BadFlag { found: *found }),
                _ => Err(DecodeValueError::BadLength {
                    expected: 1,
                    found: rest.len(),
                }),
            },
            COUNT_TAG => Ok(Self::Count(u64::from_le_bytes(eight(rest)?))),
            TEXT_TAG => core::str::from_utf8(rest)
                .map(|text| Self::Text(text.to_owned()))
                .map_err(|_| DecodeValueError::BadText),
            INSTANT_TAG => Ok(Self::Instant(VirtualTime::from_nanos(u64::from_le_bytes(
                eight(rest)?,
            )))),
            tag => Err(DecodeValueError::UnknownKind { tag }),
        }
    }
}

impl Snapshot for Value {
    fn snapshot(&self) -> Node {
        let mut bytes = Vec::new();
        match self {
            Self::Flag(flag) => {
                bytes.push(FLAG_TAG);
                bytes.push(u8::from(*flag));
            }
            Self::Count(count) => {
                bytes.push(COUNT_TAG);
                bytes.extend_from_slice(&count.to_le_bytes());
            }
            Self::Text(text) => {
                bytes.push(TEXT_TAG);
                bytes.extend_from_slice(text.as_bytes());
            }
            Self::Instant(at) => {
                bytes.push(INSTANT_TAG);
                bytes.extend_from_slice(&at.as_nanos().to_le_bytes());
            }
        }
        Node::Leaf(bytes)
    }
}

/// One thing a world holds, as the fields it is observed to have.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resource {
    fields: BTreeMap<Name, Value>,
}

impl Resource {
    /// Creates a resource with no fields.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns this resource with `field` set to `value`, which is how one is built up.
    #[must_use]
    pub fn with_field(mut self, field: Name, value: Value) -> Self {
        self.insert(field, value);
        self
    }

    /// Sets `field` to `value`, returning what it held before.
    pub fn insert(&mut self, field: Name, value: Value) -> Option<Value> {
        self.fields.insert(field, value)
    }

    /// Takes `field` away, returning what it held.
    pub fn remove(&mut self, field: &Name) -> Option<Value> {
        self.fields.remove(field)
    }

    /// Returns what `field` holds, if the resource has one.
    pub fn get(&self, field: &Name) -> Option<&Value> {
        self.fields.get(field)
    }

    /// Returns every field, in the order their names sort in.
    pub fn fields(&self) -> btree_map::Iter<'_, Name, Value> {
        self.fields.iter()
    }
}

impl Snapshot for Resource {
    fn snapshot(&self) -> Node {
        Node::Branch(
            self.fields
                .iter()
                .map(|(field, value)| (field.clone(), value.snapshot()))
                .collect(),
        )
    }
}

/// Everything a run is observed to hold at one instant.
///
/// The named resources of a world are the first level of its snapshot and each resource's fields
/// are the second, which is what lets one resource change while every other keeps the hash it had.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct World {
    resources: BTreeMap<Name, Resource>,
}

impl World {
    /// Creates a world holding nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns this world with `name` holding `resource`, which is how one is built up.
    #[must_use]
    pub fn with_resource(mut self, name: Name, resource: Resource) -> Self {
        self.insert(name, resource);
        self
    }

    /// Puts `resource` in the world under `name`, returning what was there before.
    pub fn insert(&mut self, name: Name, resource: Resource) -> Option<Resource> {
        self.resources.insert(name, resource)
    }

    /// Takes the resource called `name` out of the world, returning it.
    pub fn remove(&mut self, name: &Name) -> Option<Resource> {
        self.resources.remove(name)
    }

    /// Returns the resource called `name`, if the world holds one.
    pub fn get(&self, name: &Name) -> Option<&Resource> {
        self.resources.get(name)
    }

    /// Returns every resource, in the order their names sort in.
    pub fn resources(&self) -> btree_map::Iter<'_, Name, Resource> {
        self.resources.iter()
    }

    /// Returns how many resources the world holds.
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    /// Returns `true` if the world holds nothing.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

impl Snapshot for World {
    fn snapshot(&self) -> Node {
        Node::Branch(
            self.resources
                .iter()
                .map(|(name, resource)| (name.clone(), resource.snapshot()))
                .collect(),
        )
    }
}

/// A state, in the shape it is hashed in: a leaf of bytes, or a branch of named children.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    /// A value, as the bytes that stand for it.
    Leaf(Vec<u8>),
    /// Named parts, held in the order their names sort in.
    Branch(BTreeMap<Name, Node>),
}

impl Node {
    /// Returns the name this node answers to.
    ///
    /// A branch hashes over its children's hashes rather than their bytes, so a subtree that has
    /// not changed keeps the hash it had wherever it sits.
    pub fn state_hash(&self) -> StateHash {
        let mut hasher = blake3::Hasher::new();
        match self {
            Self::Leaf(bytes) => {
                hasher.update(&[LEAF_TAG]);
                hasher.update(&length(bytes.len()));
                hasher.update(bytes);
            }
            Self::Branch(children) => {
                hasher.update(&[BRANCH_TAG]);
                hasher.update(&length(children.len()));
                for (name, child) in children {
                    let name = name.as_str().as_bytes();
                    hasher.update(&length(name.len()));
                    hasher.update(name);
                    hasher.update(child.state_hash().as_bytes());
                }
            }
        }
        StateHash(*hasher.finalize().as_bytes())
    }
}

/// Writes a length the one way the encoding writes one.
///
/// Saturating rather than wrapping: a count past the end of `u64` cannot arise on any machine this
/// runs on, and a wrap would let two different states share an encoding if it ever did.
fn length(count: usize) -> [u8; 8] {
    u64::try_from(count).unwrap_or(u64::MAX).to_le_bytes()
}

/// A state that can be named by a hash of itself.
///
/// A system under simulation implements this for whatever it wants a recorded history to be able to
/// show, scrub through and compare. What it hands back is a tree, so the parts of it that did not
/// change between two steps are recognisably the same parts:
///
/// ```
/// use chronoloop::world::{Name, Resource, Snapshot, Value, World};
///
/// let database = Resource::new().with_field(Name::new("replicas")?, Value::Count(3));
/// let world = World::new().with_resource(Name::new("database")?, database);
///
/// // The same state always answers to the same name.
/// assert_eq!(world.state_hash(), world.clone().state_hash());
///
/// // A state that differs anywhere answers to another.
/// let scaled = World::new().with_resource(
///     Name::new("database")?,
///     Resource::new().with_field(Name::new("replicas")?, Value::Count(4)),
/// );
/// assert_ne!(world.state_hash(), scaled.state_hash());
/// # Ok::<(), chronoloop::world::NameError>(())
/// ```
pub trait Snapshot {
    /// Returns this state as the tree it is hashed in.
    fn snapshot(&self) -> Node;

    /// Returns the name this state answers to.
    fn state_hash(&self) -> StateHash {
        self.snapshot().state_hash()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

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

    #[test]
    fn the_same_state_hashes_the_same_way() {
        // Nothing about the order two resources were put in may reach the hash, or a run would
        // stop being a function of its seed the moment a system inserted in a different order.
        //
        // What holds this up is the type rather than this case: a world and a branch both keep
        // their children in a `BTreeMap`, so an insertion order is not something either of them
        // can remember. Said here rather than left for the assertion to imply — the case is a
        // guard against that choice being traded away for a `Vec`, not a test that could fail
        // while it stands.
        let built_one_way = World::new()
            .with_resource(name("alpha"), Resource::new())
            .with_resource(name("beta"), Resource::new())
            .with_resource(name("gamma"), Resource::new());
        let built_the_other = World::new()
            .with_resource(name("gamma"), Resource::new())
            .with_resource(name("beta"), Resource::new())
            .with_resource(name("alpha"), Resource::new());

        assert_eq!(built_one_way.state_hash(), built_the_other.state_hash());
    }

    #[test]
    fn states_that_differ_hash_differently() {
        struct Case {
            name: &'static str,
            world: World,
        }
        let mut removed = base();
        let cache = removed
            .remove(&name("cache"))
            .unwrap_or_else(|| panic!("the world these cases start from holds a cache"));
        let mut renamed = removed.clone();
        renamed.insert(name("caches"), cache);

        let cases = [
            Case {
                name: "the world it started as",
                world: base(),
            },
            Case {
                name: "a resource added",
                world: base().with_resource(name("queue"), Resource::new()),
            },
            Case {
                name: "a resource taken away",
                world: removed,
            },
            Case {
                name: "a resource under another name",
                world: renamed,
            },
            Case {
                name: "a field added",
                world: base().with_resource(
                    name("cache"),
                    Resource::new()
                        .with_field(name("warm"), Value::Flag(true))
                        .with_field(name("entries"), Value::Count(0)),
                ),
            },
            Case {
                name: "a field under another name",
                world: base().with_resource(
                    name("cache"),
                    Resource::new().with_field(name("hot"), Value::Flag(true)),
                ),
            },
            Case {
                name: "a count changed by one",
                world: base().with_resource(
                    name("database"),
                    Resource::new()
                        .with_field(name("replicas"), Value::Count(4))
                        .with_field(name("primary"), Value::Text("eu-west".into())),
                ),
            },
            Case {
                name: "a flag turned off",
                world: base().with_resource(
                    name("cache"),
                    Resource::new().with_field(name("warm"), Value::Flag(false)),
                ),
            },
            Case {
                name: "one byte of text changed",
                world: base().with_resource(
                    name("database"),
                    Resource::new()
                        .with_field(name("replicas"), Value::Count(3))
                        .with_field(name("primary"), Value::Text("eu-east".into())),
                ),
            },
        ];

        let mut seen = BTreeSet::new();
        for case in cases {
            assert!(
                seen.insert(case.world.state_hash()),
                "{} hashes like a state it is not",
                case.name
            );
        }
    }

    #[test]
    fn a_value_of_one_kind_never_hashes_like_a_value_of_another() {
        // Without a tag of its own per kind, a count of nothing and a flag that is off are the
        // same run of zero bytes, and a world that has just been emptied looks like one that has
        // just been switched off.
        struct Case {
            name: &'static str,
            value: Value,
        }
        let cases = [
            Case {
                name: "a flag that is off",
                value: Value::Flag(false),
            },
            Case {
                name: "a flag that is on",
                value: Value::Flag(true),
            },
            Case {
                name: "a count of nothing",
                value: Value::Count(0),
            },
            Case {
                name: "a count of one",
                value: Value::Count(1),
            },
            Case {
                name: "empty text",
                value: Value::Text(String::new()),
            },
            Case {
                name: "the start of the simulation",
                value: Value::Instant(VirtualTime::ZERO),
            },
            Case {
                name: "the end of virtual time",
                value: Value::Instant(VirtualTime::from_nanos(u64::MAX)),
            },
        ];

        let mut seen = BTreeSet::new();
        for case in cases {
            assert!(
                seen.insert(case.value.state_hash()),
                "{} hashes like a value it is not",
                case.name
            );
        }
    }

    #[test]
    fn a_leaf_of_nothing_and_a_branch_of_nothing_are_not_one_state() {
        // The kind of a node is the first thing in its encoding, so an empty resource and an empty
        // value are different states even though neither holds a byte.
        assert_ne!(
            Node::Leaf(Vec::new()).state_hash(),
            Node::Branch(BTreeMap::new()).state_hash()
        );
    }

    #[test]
    fn a_subtree_is_the_same_subtree_wherever_it_sits() {
        // The hash a branch gives a child is the hash that child answers to on its own. That is
        // what lets a store keep one copy of a resource however many steps refer to it, and what
        // lets a comparison of two states stop descending where two children agree. A branch that
        // folded anything of its own — a name, a position — into a child's hash would give the
        // same resource two names depending on where it hung, and neither would hold.
        /// The hash a world's own snapshot gives the resource called `resource`.
        fn inside(world: &World, resource: &Name) -> StateHash {
            match world.snapshot() {
                Node::Branch(children) => children
                    .get(resource)
                    .unwrap_or_else(|| panic!("the world holds a {resource}"))
                    .state_hash(),
                Node::Leaf(_) => panic!("a world is a branch"),
            }
        }
        /// The hash that resource answers to on its own.
        fn alone(world: &World, resource: &Name) -> StateHash {
            world
                .get(resource)
                .unwrap_or_else(|| panic!("the world holds a {resource}"))
                .state_hash()
        }

        let before = base();
        let after = base().with_resource(
            name("cache"),
            Resource::new().with_field(name("warm"), Value::Flag(false)),
        );

        for resource in [name("database"), name("cache")] {
            assert_eq!(
                inside(&before, &resource),
                alone(&before, &resource),
                "{resource} is named the same inside the world as out of it"
            );
        }
        assert_eq!(
            inside(&before, &name("database")),
            inside(&after, &name("database")),
            "the database did not change"
        );
        assert_ne!(
            inside(&before, &name("cache")),
            inside(&after, &name("cache")),
            "the cache did"
        );
        assert_ne!(before.state_hash(), after.state_hash());
    }

    #[test]
    fn a_node_hashes_the_bytes_the_encoding_documents() {
        // The expectation is assembled here from the encoding written down in the module's docs,
        // rather than taken from what the code produced, so the two sides of the assertion do not
        // come from one place. A change to the encoding fails this and says which part moved.
        //
        // This is also the only thing holding the lengths up. A case built out of worlds cannot:
        // dropping either length still leaves every pair of states this module can express hashing
        // differently, for the reason the module's docs give. So the framing is asserted directly,
        // byte for byte, rather than through a consequence it does not have.
        let leaf = {
            let mut bytes = vec![LEAF_TAG];
            bytes.extend_from_slice(&3_u64.to_le_bytes());
            bytes.extend_from_slice(&[7, 8, 9]);
            bytes
        };
        assert_eq!(
            Node::Leaf(vec![7, 8, 9]).state_hash().to_string(),
            blake3::hash(&leaf).to_hex().as_str(),
            "a leaf is its tag, its length and its bytes"
        );

        let empty_leaf = {
            let mut bytes = vec![LEAF_TAG];
            bytes.extend_from_slice(&0_u64.to_le_bytes());
            bytes
        };
        let branch = {
            let mut bytes = vec![BRANCH_TAG];
            bytes.extend_from_slice(&1_u64.to_le_bytes());
            bytes.extend_from_slice(&2_u64.to_le_bytes());
            bytes.extend_from_slice(b"on");
            bytes.extend_from_slice(blake3::hash(&empty_leaf).as_bytes());
            bytes
        };
        let children = [(name("on"), Node::Leaf(Vec::new()))].into_iter().collect();
        assert_eq!(
            Node::Branch(children).state_hash().to_string(),
            blake3::hash(&branch).to_hex().as_str(),
            "a branch is its tag, its count, and each child's name and hash"
        );
    }

    #[test]
    fn a_hash_round_trips_through_its_written_form() {
        struct Case {
            name: &'static str,
            node: Node,
        }
        let cases = [
            Case {
                name: "a leaf of no bytes",
                node: Node::Leaf(Vec::new()),
            },
            Case {
                name: "a branch of no children",
                node: Node::Branch(BTreeMap::new()),
            },
            Case {
                name: "a world of two resources",
                node: base().snapshot(),
            },
        ];
        for case in cases {
            let hash = case.node.state_hash();
            let written = hash.to_string();
            assert_eq!(written.len(), HASH_DIGITS, "{}", case.name);
            assert_eq!(written.parse(), Ok(hash), "{}", case.name);
        }
    }

    #[test]
    fn reading_a_hash_rejects_text_it_could_not_have_written() {
        struct Case {
            name: &'static str,
            text: String,
            want: ParseStateHashError,
        }
        let written = base().state_hash().to_string();
        let cases = [
            Case {
                name: "empty",
                text: String::new(),
                want: ParseStateHashError::BadLength { found: 0 },
            },
            Case {
                name: "one digit short",
                text: written[1..].to_string(),
                want: ParseStateHashError::BadLength { found: 63 },
            },
            Case {
                name: "one digit long",
                text: format!("{written}0"),
                want: ParseStateHashError::BadLength { found: 65 },
            },
            Case {
                name: "written the way a number is",
                text: format!("0x{}", &written[2..]),
                want: ParseStateHashError::BadDigit,
            },
            Case {
                name: "in uppercase",
                text: written.to_uppercase(),
                want: ParseStateHashError::BadDigit,
            },
            Case {
                name: "not hexadecimal at all",
                text: "z".repeat(HASH_DIGITS),
                want: ParseStateHashError::BadDigit,
            },
            Case {
                name: "the right length in characters but not in bytes",
                text: "é".repeat(HASH_DIGITS / 2),
                want: ParseStateHashError::BadDigit,
            },
        ];
        for case in cases {
            assert_eq!(
                case.text.parse::<StateHash>(),
                Err(case.want),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_name_that_could_not_be_printed_as_a_path_is_refused() {
        struct Case {
            name: &'static str,
            text: &'static str,
            usable: bool,
        }
        // A name is what a comparison of two states prints as the path to what changed, so a name
        // that would break that report is turned away where it is made rather than where it shows.
        let cases = [
            Case {
                name: "an ordinary name",
                text: "database",
                usable: true,
            },
            Case {
                name: "a name carrying a space",
                text: "database primary",
                usable: true,
            },
            Case {
                name: "empty",
                text: "",
                usable: false,
            },
            Case {
                name: "a newline of its own",
                text: "\n",
                usable: false,
            },
            Case {
                name: "two lines",
                text: "database\nprimary",
                usable: false,
            },
            Case {
                name: "a carriage return",
                text: "database\r",
                usable: false,
            },
            Case {
                name: "a name carrying the separator",
                text: "database.primary",
                usable: false,
            },
            Case {
                name: "a name that is nothing but the separator",
                text: ".",
                usable: false,
            },
        ];
        for case in cases {
            assert_eq!(Name::new(case.text).is_ok(), case.usable, "{}", case.name);
        }
    }

    #[test]
    fn refusing_a_name_says_what_was_wrong_with_it() {
        assert_eq!(
            Name::new("")
                .expect_err("an empty name is refused")
                .to_string(),
            "a name cannot be empty"
        );
        assert_eq!(
            Name::new("database\nprimary")
                .expect_err("a name of two lines is refused")
                .to_string(),
            "a name must be a single line: \"database\\nprimary\""
        );
        assert_eq!(
            Name::new("database.primary")
                .expect_err("a name holding the separator is refused")
                .to_string(),
            "a name must be one part of a path: \"database.primary\""
        );
    }

    #[test]
    fn a_value_reads_back_from_the_bytes_the_encoding_documents() {
        // The bytes are written here as literals rather than taken from `snapshot`, so the two
        // sides of the assertion do not come from one place. A round trip through both halves
        // would stay green with a tag moved, because the encoder and the decoder would move
        // together; this goes red and says which kind stopped agreeing with the documentation.
        struct Case {
            name: &'static str,
            bytes: Vec<u8>,
            value: Value,
        }
        let cases = [
            Case {
                name: "a flag that is off",
                bytes: vec![0x00, 0],
                value: Value::Flag(false),
            },
            Case {
                name: "a flag that is on",
                bytes: vec![0x00, 1],
                value: Value::Flag(true),
            },
            Case {
                name: "a count of nothing",
                bytes: vec![0x01, 0, 0, 0, 0, 0, 0, 0, 0],
                value: Value::Count(0),
            },
            Case {
                name: "a count written least significant byte first",
                bytes: vec![0x01, 1, 2, 0, 0, 0, 0, 0, 0],
                value: Value::Count(513),
            },
            Case {
                name: "the largest count there is",
                bytes: vec![0x01, 255, 255, 255, 255, 255, 255, 255, 255],
                value: Value::Count(u64::MAX),
            },
            Case {
                name: "empty text",
                bytes: vec![0x02],
                value: Value::Text(String::new()),
            },
            Case {
                name: "text running to the end of the bytes",
                bytes: vec![0x02, b'e', b'u', b'-', b'w', b'e', b's', b't'],
                value: Value::Text("eu-west".into()),
            },
            Case {
                name: "text that is not ASCII",
                bytes: vec![0x02, 0xc3, 0xa9],
                value: Value::Text("é".into()),
            },
            Case {
                name: "the start of the simulation",
                bytes: vec![0x03, 0, 0, 0, 0, 0, 0, 0, 0],
                value: Value::Instant(VirtualTime::ZERO),
            },
            Case {
                name: "the end of virtual time",
                bytes: vec![0x03, 255, 255, 255, 255, 255, 255, 255, 255],
                value: Value::Instant(VirtualTime::from_nanos(u64::MAX)),
            },
        ];
        for case in cases {
            assert_eq!(
                Value::try_from(case.bytes.as_slice()),
                Ok(case.value.clone()),
                "{}",
                case.name
            );
            // And the two halves are each other's inverse, which is what keeps a decoded report
            // from naming a value the state it read never held.
            assert_eq!(
                case.value.snapshot(),
                Node::Leaf(case.bytes),
                "{} is written the way it is read",
                case.name
            );
        }
    }

    #[test]
    fn reading_a_value_rejects_bytes_it_could_not_have_written() {
        // Every kind is refused everything but the one spelling it is written in, so no two runs
        // of bytes read back as one value and a report cannot claim a state was something it was
        // not.
        struct Case {
            name: &'static str,
            bytes: Vec<u8>,
            want: DecodeValueError,
        }
        let cases = [
            Case {
                name: "no bytes at all",
                bytes: Vec::new(),
                want: DecodeValueError::Empty,
            },
            Case {
                name: "a kind the encoding does not write",
                bytes: vec![0x04],
                want: DecodeValueError::UnknownKind { tag: 0x04 },
            },
            Case {
                name: "a flag that is neither off nor on",
                bytes: vec![0x00, 2],
                want: DecodeValueError::BadFlag { found: 2 },
            },
            Case {
                name: "a flag of no bytes",
                bytes: vec![0x00],
                want: DecodeValueError::BadLength {
                    expected: 1,
                    found: 0,
                },
            },
            Case {
                name: "a flag of two bytes",
                bytes: vec![0x00, 1, 0],
                want: DecodeValueError::BadLength {
                    expected: 1,
                    found: 2,
                },
            },
            Case {
                name: "a count one byte short",
                bytes: vec![0x01, 0, 0, 0, 0, 0, 0, 0],
                want: DecodeValueError::BadLength {
                    expected: 8,
                    found: 7,
                },
            },
            Case {
                name: "a count one byte long",
                bytes: vec![0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                want: DecodeValueError::BadLength {
                    expected: 8,
                    found: 9,
                },
            },
            Case {
                name: "an instant one byte short",
                bytes: vec![0x03, 0, 0, 0, 0, 0, 0, 0],
                want: DecodeValueError::BadLength {
                    expected: 8,
                    found: 7,
                },
            },
            Case {
                name: "text that is not UTF-8",
                bytes: vec![0x02, 0xff],
                want: DecodeValueError::BadText,
            },
        ];
        for case in cases {
            assert_eq!(
                Value::try_from(case.bytes.as_slice()),
                Err(case.want),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_world_hands_back_what_was_put_in_it() {
        let mut world = base();
        assert_eq!(world.len(), 2);
        assert!(!world.is_empty());
        assert_eq!(
            world
                .get(&name("database"))
                .and_then(|resource| resource.get(&name("replicas"))),
            Some(&Value::Count(3))
        );
        assert_eq!(
            world
                .resources()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec!["cache", "database"],
            "resources come back in the order their names sort in"
        );

        assert!(world.remove(&name("cache")).is_some());
        assert!(world.remove(&name("cache")).is_none());
        assert_eq!(world.len(), 1);
        assert_eq!(World::new().len(), 0);
        assert!(World::new().is_empty());
    }
}
