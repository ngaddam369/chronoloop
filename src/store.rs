//! Where the states a run passed through are kept, and what keeps a long run from costing a copy
//! of the world per step.
//!
//! A [`Snapshot`] gives a state a name and says nothing about keeping it. Keeping every step of a
//! thousand-step run as the tree it came back as would cost a thousand full copies of a world that
//! usually changed in one field — and the shape of that tree was chosen so it need not. A branch
//! hashes over its children's *hashes*, so a subtree that did not change answers to the name it
//! already had, and a store that keeps nodes under their names keeps it once however many steps
//! reach it.
//!
//! [`StateStore`] is that store: a map from a [`StateHash`] to one node's own contents, where a
//! branch's children are **named rather than held**. That is where the sharing lives. Two states
//! that differ in one field are two roots, two resources and two leaves; everything else they have
//! in common is one entry each, pointed at from both.
//!
//! # What the store guarantees
//!
//! **A hash the store holds brings everything it names with it.** [`StateStore::insert`] stores a
//! node's children before the node itself, and it is the only way in, so a branch in the store can
//! never name a child that is not. Two things rest on that: [`StateStore::get`] can rebuild a whole
//! state from its root without the tree it came from, and a walk of two states can descend a child
//! at a time, stopping wherever two hashes agree, which is what a comparison of two steps does —
//! see [`StateStore::held`], the one-node-deep view that walk reads the store through.
//!
//! `insert` also reads the invariant back out: a node already in the store is a subtree already in
//! the store, so it returns at once rather than walking what it would only find again. Re-storing
//! an unchanged subtree therefore costs its hash and nothing else — no clone of its bytes, no walk
//! of its children. That is a saving in **cost, not in content**: a store that walked and overwrote
//! anyway would hold exactly the same nodes under exactly the same names, so no assertion here pins
//! the shortcut, and this paragraph rather than a test is what records it.
//!
//! # Where the encoding lives
//!
//! Nowhere near here. A node's name comes from [`Node::state_hash`] and the bytes behind it are
//! written down in [`crate::world`], because a second copy of that encoding would be a second thing
//! to keep in step with every hash the engine has ever produced. What the store pays for that is
//! hashing a node once per level above it as it descends; the trees are worlds of resources of
//! fields, and the claim this module makes is about what a run costs to keep rather than what it
//! costs to hash.
//!
//! [`Snapshot`]: crate::world::Snapshot

use std::collections::BTreeMap;

use crate::world::{Name, Node, StateHash};

/// One node as the store keeps it: its own contents, with its children named rather than held.
///
/// This is the whole of the structural sharing. A [`Node::Branch`] holds its children, so a copy of
/// one is a copy of everything under it; a `Stored::Branch` holds their names, so a copy of one is
/// a handful of hashes and the children stay where they are.
#[derive(Debug, Clone)]
enum Stored {
    /// A value, as the bytes that stand for it.
    Leaf(Vec<u8>),
    /// Named parts, each one a state the store holds in its own right.
    Branch(BTreeMap<Name, StateHash>),
}

/// What the store holds under one name: the node's own contents, its children named rather than
/// held.
///
/// This is [`Stored`] as a reader sees it, and it is the store's claim made public. [`StateStore::get`]
/// rebuilds everything under a node, which is the wrong instrument for a walk that means to stop as
/// soon as two children agree — that walk wants one level at a time, and a child's name is the hash
/// it answers to, so descending is a second lookup rather than a second copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Held<'a> {
    /// A value, as the bytes that stand for it.
    Leaf(&'a [u8]),
    /// Named parts, each one a state the store holds in its own right.
    Branch(&'a BTreeMap<Name, StateHash>),
}

/// The states a run passed through, each node kept once under the name it answers to.
///
/// ```
/// use chronoloop::store::StateStore;
/// use chronoloop::world::{Name, Resource, Snapshot, Value, World};
///
/// let database = Resource::new().with_field(Name::new("replicas")?, Value::Count(3));
/// let cache = Resource::new().with_field(Name::new("warm")?, Value::Flag(true));
/// let before = World::new()
///     .with_resource(Name::new("database")?, database)
///     .with_resource(Name::new("cache")?, cache);
///
/// let mut store = StateStore::new();
/// let first = store.insert(&before.snapshot());
///
/// // The world, a resource apiece, and a leaf per field.
/// assert_eq!(store.len(), 5);
///
/// // A step that scales the database costs the field, the resource and the world — the cache it
/// // did not touch is the entry that is already there.
/// let after = before.clone().with_resource(
///     Name::new("database")?,
///     Resource::new().with_field(Name::new("replicas")?, Value::Count(4)),
/// );
/// let second = store.insert(&after.snapshot());
/// assert_eq!(store.len(), 8);
///
/// // And either step comes back whole from the name it answers to.
/// assert_eq!(store.get(first), Some(before.snapshot()));
/// assert_eq!(store.get(second), Some(after.snapshot()));
/// # Ok::<(), chronoloop::world::NameError>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct StateStore {
    nodes: BTreeMap<StateHash, Stored>,
}

impl StateStore {
    /// Creates a store holding nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Puts `node` and every part of it in the store, returning the name `node` answers to.
    ///
    /// A part already held is left where it is, so what a state costs is what it changed. Children
    /// go in before the node naming them, which is what makes a hash in the store a state the store
    /// can hand back whole.
    pub fn insert(&mut self, node: &Node) -> StateHash {
        let hash = node.state_hash();
        // Already here means all of it is already here, so there is nothing below to walk.
        if self.nodes.contains_key(&hash) {
            return hash;
        }
        let stored = match node {
            Node::Leaf(bytes) => Stored::Leaf(bytes.clone()),
            Node::Branch(children) => Stored::Branch(
                children
                    .iter()
                    .map(|(name, child)| (name.clone(), self.insert(child)))
                    .collect(),
            ),
        };
        self.nodes.insert(hash, stored);
        hash
    }

    /// Returns the state `hash` names, rebuilt from the parts the store holds.
    ///
    /// `None` if the store never saw it. A state it did see comes back whole: a branch in the store
    /// cannot name a child that is not, so the descent below cannot stop halfway through a state
    /// the store reported having.
    pub fn get(&self, hash: StateHash) -> Option<Node> {
        match self.nodes.get(&hash)? {
            Stored::Leaf(bytes) => Some(Node::Leaf(bytes.clone())),
            Stored::Branch(children) => {
                let mut rebuilt = BTreeMap::new();
                for (name, child) in children {
                    rebuilt.insert(name.clone(), self.get(*child)?);
                }
                Some(Node::Branch(rebuilt))
            }
        }
    }

    /// Returns what the store holds under `hash`, one node deep.
    ///
    /// `None` if the store never saw it. Where [`StateStore::get`] rebuilds a whole state, this
    /// hands back the node alone with its children named, which is what lets a comparison of two
    /// states descend only where their hashes differ.
    ///
    /// ```
    /// use chronoloop::store::{Held, StateStore};
    /// use chronoloop::world::{Name, Resource, Snapshot, Value, World};
    ///
    /// let cache = Resource::new().with_field(Name::new("warm")?, Value::Flag(true));
    /// let world = World::new().with_resource(Name::new("cache")?, cache.clone());
    ///
    /// let mut store = StateStore::new();
    /// let hash = store.insert(&world.snapshot());
    ///
    /// let Some(Held::Branch(resources)) = store.held(hash) else {
    ///     unreachable!("a world is a branch")
    /// };
    /// // The world names the cache by the hash the cache answers to on its own.
    /// assert_eq!(resources.get(&Name::new("cache")?), Some(&cache.state_hash()));
    /// # Ok::<(), chronoloop::world::NameError>(())
    /// ```
    pub fn held(&self, hash: StateHash) -> Option<Held<'_>> {
        Some(match self.nodes.get(&hash)? {
            Stored::Leaf(bytes) => Held::Leaf(bytes),
            Stored::Branch(children) => Held::Branch(children),
        })
    }

    /// Returns how many distinct nodes the store holds.
    ///
    /// This is what the sharing is worth: a run's states cost this many nodes between them rather
    /// than the sum of what each one is made of.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if the store holds nothing.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::world::{Name, Resource, Snapshot, Value, World};

    /// A name, failing the test rather than returning an error no case expects.
    fn name(text: &str) -> Name {
        Name::new(text).unwrap_or_else(|e| panic!("a test name is a name: {e}"))
    }

    /// The world the cases below are a small change away from.
    ///
    /// Six nodes: the world itself, a resource apiece, and a leaf per field — counted here by hand
    /// from the tree rather than read off what the store did with it, since what the store did with
    /// it is the thing under test.
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

    /// How many nodes the world above is made of.
    const BASE_NODES: usize = 6;

    #[test]
    fn a_state_comes_back_the_way_it_went_in() {
        let world = base();
        let snapshot = world.snapshot();
        let mut store = StateStore::new();
        let hash = store.insert(&snapshot);

        assert_eq!(hash, world.state_hash(), "a state is stored under its name");
        assert_eq!(
            store.get(hash),
            Some(snapshot),
            "and comes back as the tree that went in"
        );
    }

    #[test]
    fn an_unchanged_subtree_is_stored_once() {
        // The whole point of the store: a step that changed one field costs that field, the
        // resource holding it and the world holding that — three nodes — rather than a second copy
        // of everything the run was carrying.
        let mut store = StateStore::new();
        store.insert(&base().snapshot());
        assert_eq!(store.len(), BASE_NODES, "a world of two resources");

        store.insert(&base().snapshot());
        assert_eq!(
            store.len(),
            BASE_NODES,
            "the same state again is the same state"
        );

        let changed = base().with_resource(
            name("cache"),
            Resource::new().with_field(name("warm"), Value::Flag(false)),
        );
        store.insert(&changed.snapshot());
        assert_eq!(
            store.len(),
            BASE_NODES + 3,
            "a flag, the resource holding it and the world holding that — the database is untouched"
        );
    }

    #[test]
    fn a_part_shared_between_two_states_is_stored_where_neither_owns_it() {
        // A subtree's name does not depend on where it hangs, so two worlds that happen to hold the
        // same resource under different names hold one copy of it between them.
        let resource = Resource::new()
            .with_field(name("replicas"), Value::Count(3))
            .with_field(name("primary"), Value::Text("eu-west".into()));
        let mine = World::new().with_resource(name("mine"), resource.clone());
        let yours = World::new().with_resource(name("yours"), resource);

        let mut store = StateStore::new();
        store.insert(&mine.snapshot());
        assert_eq!(store.len(), 4, "a world, a resource and two fields");

        store.insert(&yours.snapshot());
        assert_eq!(
            store.len(),
            5,
            "the second world is the only new thing about it"
        );
    }

    #[test]
    fn every_hash_a_stored_state_names_is_in_the_store() {
        /// Asserts that `node` and everything under it comes back out of `store`.
        fn reachable(store: &StateStore, node: &Node) {
            let hash = node.state_hash();
            assert_eq!(
                store.get(hash).as_ref(),
                Some(node),
                "the store holds {hash} whole"
            );
            if let Node::Branch(children) = node {
                for child in children.values() {
                    reachable(store, child);
                }
            }
        }

        let mut store = StateStore::new();
        let snapshot = base().snapshot();
        store.insert(&snapshot);
        // A state that is in the store brings everything it names with it, which is what lets a
        // comparison of two states descend into one of them a child at a time.
        reachable(&store, &snapshot);
    }

    #[test]
    fn what_the_store_holds_under_a_name_comes_back_without_the_rest_of_it() {
        // The shallow view a comparison of two states needs. `get` rebuilds everything under a
        // node, which is the wrong instrument for a walk that means to stop as soon as two
        // children agree, so the store also answers for one node at a time: a branch's children
        // by name and hash, and a leaf's bytes.
        let world = base();
        let mut store = StateStore::new();
        store.insert(&world.snapshot());

        let Some(Held::Branch(resources)) = store.held(world.state_hash()) else {
            panic!("a world is a branch the store holds")
        };
        assert_eq!(
            resources.keys().map(Name::as_str).collect::<Vec<_>>(),
            vec!["cache", "database"],
            "a branch names its children in the order their names sort in"
        );

        let cache = world
            .get(&name("cache"))
            .unwrap_or_else(|| panic!("the world holds a cache"));
        assert_eq!(
            resources.get(&name("cache")),
            Some(&cache.state_hash()),
            "and names each one by the hash it answers to on its own"
        );

        let Some(Held::Branch(fields)) = store.held(cache.state_hash()) else {
            panic!("a resource is a branch the store holds")
        };
        let warm = fields
            .get(&name("warm"))
            .unwrap_or_else(|| panic!("the cache holds a warm flag"));
        let Node::Leaf(bytes) = Value::Flag(true).snapshot() else {
            panic!("a value is a leaf")
        };
        assert_eq!(
            store.held(*warm),
            Some(Held::Leaf(&bytes)),
            "a leaf is the bytes it holds"
        );

        assert_eq!(
            store.held(World::new().state_hash()),
            None,
            "and a state the store never saw is not there to be looked at"
        );
    }

    #[test]
    fn a_state_never_stored_is_not_found() {
        let mut store = StateStore::new();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert_eq!(store.get(base().state_hash()), None);

        store.insert(&base().snapshot());
        assert!(!store.is_empty());
        assert_eq!(
            store.get(World::new().state_hash()),
            None,
            "an empty world is a state of its own, and this run never reached it"
        );
    }
}
