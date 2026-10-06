//! A level-triggered control loop keeping replicated databases placed across regions.
//!
//! The systems before this one are protocols: an exchange, a ring, a quorum. This is the first
//! **control loop**, which is the thing chronoloop exists to run — an operator that is told what
//! should exist and keeps making it so.
//!
//! # The world it runs in
//!
//! What is asked for is a [`Placement`] for each database: one region holding its **primary**, and
//! a set of other regions each holding a **standby** that follows it. What exists is a set of
//! replicas, one for each database in each region it has been placed in, and a replica is not ready
//! the moment it is asked for. Each one goes through [`Phase`]s on a clock of its own — a primary is
//! provisioned and then serves, and a standby is provisioned, then spends a while catching up with
//! its primary, and only then is in sync.
//!
//! That is what makes the order of things matter. A standby that is still catching up holds less
//! than its primary does, so taking a primary away while none of its standbys has caught up
//! **destroys the database's data**, and the world records that as a fact about the database rather
//! than as something the next pass can undo. The other moves are safe whenever they land: a replica
//! is created, a standby that has caught up is promoted — and the primary it replaces steps down to
//! a standby that is, by then, in sync — and a replica nobody wants is taken away.
//!
//! # Level-triggered, and what that buys
//!
//! The decision is [`reconcile`], and it reads exactly two things: what is [`Desired`] and what is
//! [`Observed`]. It keeps nothing between calls, so it cannot be told *what changed* — only what
//! is. That is what "level-triggered" means, and two properties fall out of it rather than being
//! built in:
//!
//! - **A lost action is retried without anything deciding to retry it.** The next pass looks again,
//!   finds the world still short of what was asked for, and asks for the same thing again. A
//!   controller that acted on *changes* would have seen nothing change and done nothing, forever.
//! - **A controller can start from nothing.** There is no memory to lose, so a controller restarted
//!   halfway through, or one that never saw the beginning, reaches the same decisions as one that
//!   was there all along.
//!
//! Both depend on the other half of the contract: [`Observed::apply`] is idempotent. Creating a
//! replica that already exists, promoting one that is not a standby in sync, or deleting one that is
//! not there, changes nothing — so an action decided on a listing that has since gone stale does
//! nothing the next pass cannot put right.
//!
//! The decision asks for every difference it sees at once, and says nothing about the order they
//! should land in. That is safe for every move but one, and the timeline this module runs reaches
//! that one: when a database's primary is moved out of a region that is wanted for nothing, the
//! same pass asks for the old primary to be deleted and the new one promoted, and if the delete
//! lands while the standby to be promoted is still catching up, the data goes with it. Whether it
//! is still catching up is a matter of how long it took to be created and how long it is taking to
//! catch up, so what reaches the hazard is a run's draws together with whatever held it up.
//!
//! # The run
//!
//! [`run`] starts two tasks over the simulated network. The **API server** holds what is desired and
//! what exists. What is desired changes on a fixed timeline — two databases are placed, one gains a
//! standby in a third region and then loses the standby it started with, and the other is moved out
//! of the region holding its primary, into the region holding its standby — and the server applies
//! whatever it is asked to, and moves every replica on through its phases as their time comes. The
//! **controller** wakes on a fixed resync period, asks for a listing, works out what to do and sends
//! it. Every change the server makes, whether asked for or the passage of time, is a step of the
//! trace.
//!
//! The wire is **dependable**, for the reason `quorum` gives: it takes time and nothing else, so an
//! injected [`FaultSchedule`] is the only thing that can keep a request from arriving, and a failure
//! is the schedule's doing. It is slow and uneven — a message can take anything from ten
//! milliseconds to most of a second — so the actions of one pass routinely land in another order
//! than they were sent in. The seed reaches a run twice over: through how long each message spends
//! on the wire, and through how long each replica takes to be provisioned and to catch up, which the
//! server draws from a generator of its own. The network's generator is drawn from the run's seed
//! first and the server's second, and that order is part of every history this module records.
//!
//! A run breaks if a database that was still wanted lost its data, at the step that destroyed it.
//! Otherwise it holds up when, once the loop has finished, every replica that was asked for exists
//! in the role asked of it and is ready, and nothing else does.

use core::cell::RefCell;
use core::fmt;
use core::ops::RangeInclusive;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet, btree_map};
use std::rc::Rc;

use crate::clock::{Clock, VirtualTime};
use crate::executor::Executor;
use crate::fault::{Fault, FaultSchedule, Window};
use crate::history::Entry;
use crate::net::{Link, Network, NodeId, Odds, VirtualNetwork};
use crate::outcome::{Outcome, Reason, ReasonError};
use crate::rng::{Rng, SeededRng};
use crate::store::StateStore;
use crate::systems::RunError;
use crate::trace::{Step, Trace};
use crate::world::{Name, NameError, Resource, Snapshot, Value, World};

/// How long apart the controller's passes are. Pass `n` opens at `n` of these after the start.
const PERIOD: Duration = Duration::from_secs(5);

/// How many passes the controller makes before it stops.
const PASSES: u64 = 8;

/// How long the controller waits for a listing before giving the pass up.
///
/// Comfortably longer than a request across and a listing back, each as slow as the wire can be,
/// and comfortably shorter than [`PERIOD`], so no pass outlives its own.
const PATIENCE: Duration = Duration::from_secs(2);

/// How long the server waits for a request, once nothing more is going to be asked of it and no
/// replica has anywhere left to go, before deciding nothing more is coming.
const QUIET: Duration = Duration::from_secs(30);

/// What the dependable wire takes to carry a message: slow, and uneven enough that the actions of
/// one pass are routinely carried out in another order than they were sent in.
const LATENCY: RangeInclusive<Duration> = Duration::from_millis(10)..=Duration::from_millis(800);

/// How long a replica takes to be provisioned.
const PROVISIONING: RangeInclusive<Duration> = Duration::from_secs(1)..=Duration::from_secs(4);

/// How long a standby takes to catch up with its primary once it has been provisioned.
const CATCHING_UP: RangeInclusive<Duration> = Duration::from_secs(2)..=Duration::from_secs(12);

/// What is asked for over the run: at a whole number of seconds, a database, the region its primary
/// is wanted in, and the regions its standbys are wanted in.
///
/// Each change falls between two passes, so the pass after it is the first to see it. The last one
/// evacuates west: `users`' primary is wanted in east, where its standby is, and west is wanted for
/// nothing. It falls late enough that with nothing in the way the standby in east has always caught
/// up by the time the pass after it acts: asked for on the first pass, it exists by 7.4s at the
/// latest and is in sync by 23.4s, and that pass opens at 25s. So only a run held up on its way
/// there can reach the unsafe delete, which `tests/reconciler.rs` checks over a sweep of seeds
/// rather than leaving to this arithmetic.
const TIMELINE: &[(u64, &str, &str, &[&str])] = &[
    (0, "orders", "east", &["west"]),
    (0, "users", "west", &["east"]),
    (12, "orders", "east", &["south", "west"]),
    (22, "orders", "east", &["south"]),
    (23, "users", "east", &["south"]),
];

/// How many faults [`drawn_faults`] draws for a seed.
const TROUBLE: usize = 6;

/// How long a drawn fault lasts, before it is cut short at [`HEALED_BY`].
const TROUBLE_LASTS: RangeInclusive<Duration> =
    Duration::from_millis(100)..=Duration::from_secs(10);

/// The odds a drawn loss drops a message at are some number of these, never none and never all.
const TENTHS: u32 = 10;

/// How many of the controller's last passes a drawn schedule leaves alone.
///
/// A controller owes convergence once the trouble stops and not before, so a schedule that is still
/// cutting the way to the server on the last pass is asking for a failure nobody could avoid. Three
/// clear passes is a choice about the **passes** — enough for a pass to look, a pass to act on what
/// it saw and one more to spare — and nothing about it knows what the timeline asks for or when.
const CLEAR_PASSES: u64 = 3;

/// The field holding whether a database's data has been destroyed.
const LOST: &str = "lost";

/// What follows a region in the field holding the role a replica there is wanted in.
const WANTED: &str = "wanted";

/// What follows a region in the field holding the role a replica there has.
const ROLE: &str = "role";

/// What follows a region in the field holding the phase a replica there is in.
const PHASE: &str = "phase";

/// A place a replica can be put.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Region(Name);

impl Region {
    /// The region called `name`.
    pub fn new(name: Name) -> Self {
        Self(name)
    }

    /// What the region is called.
    pub fn name(&self) -> &Name {
        &self.0
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What a replica does for its database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    /// The replica the database is served from, and which holds everything it has.
    Primary,
    /// A replica following the primary, which holds what the primary holds once it has caught up.
    Standby,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Primary => "primary",
            Self::Standby => "standby",
        })
    }
}

/// How far along a replica is.
///
/// A primary goes from [`Phase::Provisioning`] straight to [`Phase::Ready`]; a standby spends a
/// while [`Phase::CatchingUp`] in between, and only once it is ready does it hold everything its
/// primary does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    /// Being brought into being, and holding nothing yet.
    Provisioning,
    /// A standby copying what its primary holds, and holding only part of it so far.
    CatchingUp,
    /// A primary serving, or a standby in sync with its primary.
    Ready,
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Provisioning => "provisioning",
            Self::CatchingUp => "catching up",
            Self::Ready => "ready",
        })
    }
}

/// Errors returned when a placement asks for something no database can be.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PlacementError {
    /// The region asked to hold the primary was also asked to hold a standby, and a database has
    /// one replica in a region.
    PrimaryAlsoStandby {
        /// The region asked for twice.
        region: Region,
    },
}

impl fmt::Display for PlacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimaryAlsoStandby { region } => {
                write!(
                    f,
                    "{region} is asked to hold both the primary and a standby"
                )
            }
        }
    }
}

impl std::error::Error for PlacementError {}

/// Where a database is wanted: one region holding its primary, and a standby in each of the others.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    primary: Region,
    standbys: BTreeSet<Region>,
}

impl Placement {
    /// A primary in `primary`, and a standby in each of `standbys`.
    ///
    /// # Errors
    ///
    /// Returns [`PlacementError::PrimaryAlsoStandby`] if `primary` is among `standbys`.
    pub fn new(
        primary: Region,
        standbys: impl IntoIterator<Item = Region>,
    ) -> Result<Self, PlacementError> {
        let standbys: BTreeSet<Region> = standbys.into_iter().collect();
        if standbys.contains(&primary) {
            return Err(PlacementError::PrimaryAlsoStandby { region: primary });
        }
        Ok(Self { primary, standbys })
    }

    /// The region the primary is wanted in.
    pub fn primary(&self) -> &Region {
        &self.primary
    }

    /// The regions a standby is wanted in, in name order.
    pub fn standbys(&self) -> impl Iterator<Item = &Region> {
        self.standbys.iter()
    }

    /// Every region a replica is wanted in, with the role wanted of it.
    fn replicas(&self) -> impl Iterator<Item = (&Region, Role)> {
        core::iter::once((&self.primary, Role::Primary))
            .chain(self.standbys.iter().map(|region| (region, Role::Standby)))
    }
}

impl fmt::Display for Placement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "primary {}", self.primary)?;
        let mut standbys = self.standbys.iter();
        match standbys.next() {
            None => f.write_str(" and no standbys"),
            Some(first) => {
                write!(f, " and standbys {first}")?;
                for standby in standbys {
                    write!(f, ", {standby}")?;
                }
                Ok(())
            }
        }
    }
}

/// What should exist: every database that is wanted, and where.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Desired {
    databases: BTreeMap<Name, Placement>,
}

impl Desired {
    /// Wants nothing at all.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wants `database` placed as `placement`, returning where it was wanted before.
    pub fn insert(&mut self, database: Name, placement: Placement) -> Option<Placement> {
        self.databases.insert(database, placement)
    }

    /// Every database that is wanted, in name order.
    pub fn databases(&self) -> btree_map::Iter<'_, Name, Placement> {
        self.databases.iter()
    }

    /// Every replica that is wanted, with the role wanted of it.
    fn replicas(&self) -> BTreeMap<(&Name, &Region), Role> {
        self.databases
            .iter()
            .flat_map(|(database, placement)| {
                placement
                    .replicas()
                    .map(move |(region, role)| ((database, region), role))
            })
            .collect()
    }
}

/// One database's copy in one region, as it stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Replica {
    role: Role,
    phase: Phase,
}

impl Replica {
    /// What the replica does for its database.
    pub fn role(self) -> Role {
        self.role
    }

    /// How far along it is.
    pub fn phase(self) -> Phase {
        self.phase
    }
}

/// What does exist: every replica, and every database whose data has been destroyed.
///
/// There is no way to change one from outside other than [`Observed::apply`], which is the way a
/// server changes what it holds; the server also moves replicas on through their phases as their
/// time comes, which is the other way the world changes and the one nobody asks for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Observed {
    replicas: BTreeMap<(Name, Region), Replica>,
    lost: BTreeSet<Name>,
}

impl Observed {
    /// Holds nothing at all.
    pub fn new() -> Self {
        Self::default()
    }

    /// Carries out `action`, returning whether anything changed.
    ///
    /// Idempotent: creating a replica that already exists, promoting one that is not a standby in
    /// sync, and deleting one that is not there, all change nothing. A database has at most one
    /// primary, so a primary is not created where the database has one already. That is what makes
    /// it safe to act on a listing that has gone stale — the worst a stale action can do is nothing,
    /// or something the next pass puts right.
    ///
    /// The one move whose order matters: deleting a database's primary while none of its standbys
    /// is in sync destroys the database's data, and the database is marked lost for good.
    pub fn apply(&mut self, action: &Action) -> bool {
        match action {
            Action::Create {
                database,
                region,
                role,
            } => {
                let key = (database.clone(), region.clone());
                if self.replicas.contains_key(&key)
                    || (*role == Role::Primary && self.has_primary(database))
                {
                    return false;
                }
                self.replicas.insert(
                    key,
                    Replica {
                        role: *role,
                        phase: Phase::Provisioning,
                    },
                );
                true
            }
            Action::Promote { database, region } => {
                let key = (database.clone(), region.clone());
                let promotable = self.replicas.get(&key).is_some_and(|replica| {
                    replica.role == Role::Standby && replica.phase == Phase::Ready
                });
                // `reconcile` only ever asks to promote a standby that is ready, so only a stale
                // action reaches this refusal — and only the stale-action case holds it.
                if !promotable {
                    return false;
                }
                // The standby is in sync, so the primary it replaces holds nothing it does not, and
                // steps down to a standby that is in sync too.
                for ((owner, _), replica) in &mut self.replicas {
                    if owner == database && replica.role == Role::Primary {
                        replica.role = Role::Standby;
                    }
                }
                self.replicas.insert(
                    key,
                    Replica {
                        role: Role::Primary,
                        phase: Phase::Ready,
                    },
                );
                true
            }
            Action::Delete { database, region } => {
                let Some(gone) = self.replicas.remove(&(database.clone(), region.clone())) else {
                    return false;
                };
                if gone.role == Role::Primary && !self.has_standby_in_sync(database) {
                    self.lost.insert(database.clone());
                }
                true
            }
        }
    }

    /// Moves the replica of `database` in `region` on to its next phase, returning whether it had
    /// one to move on to.
    fn advance(&mut self, database: &Name, region: &Region) -> bool {
        let Some(replica) = self.replicas.get_mut(&(database.clone(), region.clone())) else {
            return false;
        };
        replica.phase = match (replica.role, replica.phase) {
            (Role::Standby, Phase::Provisioning) => Phase::CatchingUp,
            (_, Phase::Provisioning | Phase::CatchingUp) => Phase::Ready,
            (_, Phase::Ready) => return false,
        };
        true
    }

    /// The replica of `database` in `region`, if there is one.
    pub fn replica(&self, database: &Name, region: &Region) -> Option<Replica> {
        self.replicas
            .get(&(database.clone(), region.clone()))
            .copied()
    }

    /// Every replica that exists, in the order of its database's name and then its region's.
    pub fn replicas(&self) -> impl Iterator<Item = (&Name, &Region, Replica)> {
        self.replicas
            .iter()
            .map(|((database, region), replica)| (database, region, *replica))
    }

    /// Whether `database`'s data has been destroyed.
    pub fn is_lost(&self, database: &Name) -> bool {
        self.lost.contains(database)
    }

    /// Whether `database` has a primary anywhere, in whatever phase.
    fn has_primary(&self, database: &Name) -> bool {
        self.replicas()
            .any(|(owner, _, replica)| owner == database && replica.role == Role::Primary)
    }

    /// Whether `database` has a standby that holds everything its primary does.
    fn has_standby_in_sync(&self, database: &Name) -> bool {
        self.replicas().any(|(owner, _, replica)| {
            owner == database && replica.role == Role::Standby && replica.phase == Phase::Ready
        })
    }
}

/// One thing the controller asks the server to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Bring a replica that does not exist into being.
    Create {
        /// The database it is a replica of.
        database: Name,
        /// Where it is to be.
        region: Region,
        /// What it is to do.
        role: Role,
    },
    /// Make a standby that is in sync the primary, in place of whichever replica was.
    Promote {
        /// The database it is a replica of.
        database: Name,
        /// Where it is.
        region: Region,
    },
    /// Take a replica that exists away.
    Delete {
        /// The database it is a replica of.
        database: Name,
        /// Where it is.
        region: Region,
    },
}

impl Action {
    /// The database the action is about.
    fn database(&self) -> &Name {
        match self {
            Self::Create { database, .. }
            | Self::Promote { database, .. }
            | Self::Delete { database, .. } => database,
        }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Create {
                database,
                region,
                role,
            } => write!(f, "create {database} in {region} as {role}"),
            Self::Promote { database, region } => write!(f, "promote {database} in {region}"),
            Self::Delete { database, region } => write!(f, "delete {database} in {region}"),
        }
    }
}

/// Works out what to ask for to turn what is `observed` into what is `desired`.
///
/// Reads the two states and nothing else, and keeps nothing for next time: the answer is a function
/// of what *is*, never of what changed. The actions come back in the order of the database's name
/// and then the region's, at most one a replica:
///
/// - a replica that is wanted and missing is **created** — as the primary if that is what is wanted
///   and the database has none yet, and otherwise as a standby, to be promoted once it is in sync;
/// - a standby that is in sync where the primary is wanted is **promoted**;
/// - a replica that is not wanted is **deleted**.
///
/// Nothing else is asked for. A replica on its way through its phases needs only time, and one
/// standing where a standby is wanted steps down by itself when the primary that is wanted is
/// promoted. Every difference is asked for at once, in no order that keeps a database safe.
///
/// ```
/// use chronoloop::systems::reconciler::{Desired, Observed, Placement, Region, reconcile};
/// use chronoloop::world::Name;
///
/// let east = Region::new(Name::new("east")?);
/// let west = Region::new(Name::new("west")?);
/// let mut desired = Desired::new();
/// desired.insert(Name::new("orders")?, Placement::new(east, [west])?);
/// let mut observed = Observed::new();
///
/// let actions = reconcile(&desired, &observed);
/// let shown: Vec<String> = actions.iter().map(ToString::to_string).collect();
/// assert_eq!(
///     shown,
///     ["create orders in east as primary", "create orders in west as standby"]
/// );
///
/// for action in &actions {
///     observed.apply(action);
/// }
/// // Both replicas exist, and what is left is for them to finish getting ready.
/// assert!(reconcile(&desired, &observed).is_empty());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn reconcile(desired: &Desired, observed: &Observed) -> Vec<Action> {
    let wanted = desired.replicas();
    let mut replicas: BTreeSet<(&Name, &Region)> = wanted.keys().copied().collect();
    replicas.extend(
        observed
            .replicas
            .keys()
            .map(|(database, region)| (database, region)),
    );
    replicas
        .into_iter()
        .filter_map(|(database, region)| {
            let action = match (
                wanted.get(&(database, region)),
                observed.replica(database, region),
            ) {
                (Some(&role), None) => Action::Create {
                    database: database.clone(),
                    region: region.clone(),
                    role: if role == Role::Primary && !observed.has_primary(database) {
                        Role::Primary
                    } else {
                        Role::Standby
                    },
                },
                (
                    Some(Role::Primary),
                    Some(Replica {
                        role: Role::Standby,
                        phase: Phase::Ready,
                    }),
                ) => Action::Promote {
                    database: database.clone(),
                    region: region.clone(),
                },
                (None, Some(_)) => Action::Delete {
                    database: database.clone(),
                    region: region.clone(),
                },
                _ => return None,
            };
            Some(action)
        })
        .collect()
}

/// Whether what is `observed` is exactly what is `desired`: every replica asked for exists in the
/// role asked of it and is ready, and nothing else exists.
fn converged(desired: &Desired, observed: &Observed) -> bool {
    let wanted = desired.replicas();
    wanted.len() == observed.replicas.len()
        && observed.replicas().all(|(database, region, replica)| {
            replica.phase == Phase::Ready && wanted.get(&(database, region)) == Some(&replica.role)
        })
}

/// What travels between the controller and the server.
#[derive(Debug, Clone)]
enum Message {
    /// The controller asking what is wanted and what exists.
    List,
    /// The server's answer.
    Listed(Desired, Observed),
    /// The controller asking for one thing to be done.
    Apply(Action),
}

/// Returns the instant pass `pass` opens at.
fn opens(pass: u64) -> VirtualTime {
    VirtualTime::from_nanos(pass.saturating_mul(as_nanos(PERIOD)))
}

/// Returns `duration` as a whole number of nanoseconds, capped at the end of virtual time.
fn as_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Returns how long there is left until `deadline`, which is nothing once it has passed.
fn until(now: VirtualTime, deadline: VirtualTime) -> Duration {
    Duration::from_nanos(deadline.as_nanos().saturating_sub(now.as_nanos()))
}

/// Returns the instant `duration` after `at`, capped at the end of virtual time.
fn after(at: VirtualTime, duration: Duration) -> VirtualTime {
    at.checked_add(duration)
        .unwrap_or(VirtualTime::from_nanos(u64::MAX))
}

/// One change to what is wanted, with its names already made.
struct Change {
    at: VirtualTime,
    database: Name,
    placement: Placement,
}

/// The timeline, with every name and placement made up front.
///
/// Making either is fallible and a task has nowhere to report a failure to, so the one place they
/// could go wrong is put where there is still a caller to tell — the reason `quorum` makes its
/// names before its run starts.
fn timeline() -> Result<Vec<Change>, RunError> {
    let region = |name: &str| Name::new(name).map(Region::new);
    TIMELINE
        .iter()
        .map(|&(seconds, database, primary, standbys)| {
            let standbys = standbys
                .iter()
                .map(|&standby| region(standby))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Change {
                at: after(opens(0), Duration::from_secs(seconds)),
                database: Name::new(database)?,
                placement: Placement::new(region(primary)?, standbys)?,
            })
        })
        .collect()
}

/// Writes what is wanted and what exists as a world: one resource for each database that is wanted
/// or has a replica or lost its data, saying whether it has lost it and, for each region it is
/// wanted in or has a replica in, the role wanted there and the role and phase of the replica there.
///
/// A region's fields are named for it — `west-phase` — so a comparison of two steps reads
/// `orders.west-phase "catching up" -> "ready"`. The three endings are fixed, so a field name says
/// which region it is about however the region is spelled.
fn world(desired: &Desired, observed: &Observed) -> Result<World, NameError> {
    let lost = Name::new(LOST)?;
    let field = |region: &Region, ending: &str| Name::new(format!("{region}-{ending}"));
    let mut world = World::new();
    let mut put = |database: &Name, field: Name, value: Value| {
        let mut resource = world.remove(database).unwrap_or_else(|| {
            Resource::new().with_field(lost.clone(), Value::Flag(observed.is_lost(database)))
        });
        resource.insert(field, value);
        world.insert(database.clone(), resource);
    };
    for (database, placement) in desired.databases() {
        for (region, role) in placement.replicas() {
            put(
                database,
                field(region, WANTED)?,
                Value::Text(role.to_string()),
            );
        }
    }
    for (database, region, replica) in observed.replicas() {
        put(
            database,
            field(region, ROLE)?,
            Value::Text(replica.role().to_string()),
        );
        put(
            database,
            field(region, PHASE)?,
            Value::Text(replica.phase().to_string()),
        );
    }
    for database in &observed.lost {
        if world.get(database).is_none() {
            world.insert(
                database.clone(),
                Resource::new().with_field(lost.clone(), Value::Flag(true)),
            );
        }
    }
    Ok(world)
}

/// Something the server changed, and the state the run was in once it had.
struct Observation {
    at: VirtualTime,
    message: String,
    desired: Desired,
    observed: Observed,
    destroyed: bool,
    converged: bool,
}

/// Runs the controller and the server under `seed`, with `faults` to get in their way.
///
/// Returns what the run passed through, the states it passed through, and how it went. A run that
/// meets no faults ends with every replica where it was asked for:
///
/// ```
/// use chronoloop::fault::FaultSchedule;
/// use chronoloop::outcome::Outcome;
/// use chronoloop::systems::reconciler;
///
/// let (trace, store, outcome) = reconciler::run(20_261_005, &FaultSchedule::default())?;
///
/// assert_eq!(outcome, Outcome::Pass);
/// assert_eq!(trace.seed(), 20_261_005);
/// assert!(store.get(trace.steps()[0].state()).is_some());
/// # Ok::<(), chronoloop::systems::RunError>(())
/// ```
///
/// # Errors
///
/// Returns [`RunError`] if the simulation could not finish, or if the run wrote down something that
/// could not be read back.
pub fn run(seed: u64, faults: &FaultSchedule) -> Result<(Trace, StateStore, Outcome), RunError> {
    let observed = observe(seed, faults)?;
    let outcome = verdict(&observed)?;
    let (steps, store) = collect(observed)?;
    Ok((Trace::new(seed, steps), store, outcome))
}

/// Draws a schedule of trouble for the run of `seed`, knowing nothing about what the run is for.
///
/// This is what lets a sweep look for a failure **blind**. A schedule written by hand is written by
/// someone who knows where the failure is; this one knows only what any run of this system has —
/// two nodes and the link between them, and when the controller's passes are — and draws
/// [`TROUBLE`] faults onto that: each on one direction of the link, each an outage or a loss at some
/// odds, each starting anywhere from the beginning and lasting between a tenth of a second and ten.
/// Every one is over by the time the last [`CLEAR_PASSES`] passes open, because a controller owes
/// convergence once trouble stops and not while it is still going on. Nothing in it is drawn from,
/// or tuned against, the timeline of what is wanted.
///
/// The draws come from a generator of their own: the **third** value the run's root generator
/// yields, after the network's and the server's. A run never draws a third, so no history recorded
/// before this existed moves; and the schedule is not the same stream as anything in the run it is
/// thrown at, which would correlate the trouble with what the trouble lands on. For each fault, in
/// order: which direction, then outage or loss, then the odds if a loss, then when it starts, then
/// how long it lasts. That order is part of every schedule this hands back, the way the order of the
/// draws in a send is part of every history.
///
/// ```
/// use chronoloop::systems::reconciler;
///
/// let faults = reconciler::drawn_faults(31);
///
/// assert_eq!(faults.len(), 6);
/// assert_eq!(faults, reconciler::drawn_faults(31));
/// ```
pub fn drawn_faults(seed: u64) -> FaultSchedule {
    let mut seeds = SeededRng::from_seed(seed);
    // The network's and the server's, in the order `observe` takes them.
    seeds.next_u64();
    seeds.next_u64();
    let mut rng = SeededRng::from_seed(seeds.next_u64());

    let links = [
        (NodeId::from_index(0), NodeId::from_index(1)),
        (NodeId::from_index(1), NodeId::from_index(0)),
    ];
    let healed = opens(PASSES + 1 - CLEAR_PASSES).as_nanos();
    let last = u64::try_from(links.len() - 1).unwrap_or(0);
    let faults = (0..TROUBLE)
        .map(|_| {
            // The index is drawn below the array's length, so it is always in range; the fallback is
            // capped by the line it is on rather than trusted.
            let (from, to) = usize::try_from(rng.range(0..=last))
                .ok()
                .and_then(|index| links.get(index).copied())
                .unwrap_or(links[0]);
            let loss = if rng.chance(1, 2) {
                // Drawn strictly between none and all, so `Odds::new` has nothing to refuse.
                let numerator = u32::try_from(rng.range(1..=u64::from(TENTHS - 1))).unwrap_or(1);
                Some(Odds::new(numerator, TENTHS).unwrap_or(Odds::always()))
            } else {
                None
            };
            let start = rng.range(0..=healed - 1);
            let end = start
                .saturating_add(as_nanos(rng.duration_in(TROUBLE_LASTS)))
                .min(healed);
            let during = Window::new(VirtualTime::from_nanos(start), VirtualTime::from_nanos(end))
                // `end` is at least `start`: it is `start` plus a length, capped at an instant
                // `start` was drawn below.
                .unwrap_or(Window::forever_from(VirtualTime::from_nanos(start)));
            match loss {
                None => Fault::Partition { from, to, during },
                Some(odds) => Fault::Loss {
                    from,
                    to,
                    during,
                    odds,
                },
            }
        })
        .collect();
    FaultSchedule::new(faults)
}

/// Runs the whole thing and returns what the server wrote down.
fn observe(seed: u64, faults: &FaultSchedule) -> Result<Vec<Observation>, RunError> {
    let changes = timeline()?;
    let mut executor = Executor::new();
    let mut seeds = SeededRng::from_seed(seed);

    // The network's generator first and the server's second. Both orders are part of every history
    // this module records: swap them and every run lands somewhere else.
    let network: VirtualNetwork<Message, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()))
            .with_default_link(Link::new(LATENCY));
    let rng = SeededRng::from_seed(seeds.next_u64());

    // The controller is added first, so it is node 0 and the server is node 1.
    let controller = network.add_node();
    let server = network.add_node();
    let address = server.id();
    network.set_faults(faults.clone());

    let log = Rc::new(RefCell::new(Vec::new()));
    let clock = executor.handle();
    let observations = Rc::clone(&log);
    executor.spawn(async move {
        serve(&clock, &server, rng, changes, &observations).await;
    });

    let clock = executor.handle();
    executor.spawn(async move {
        control(&clock, &controller, address).await;
    });

    executor.run()?;
    // Every task is finished and nothing is polling, so nothing else holds the log open.
    Ok(core::mem::take(&mut *log.borrow_mut()))
}

/// The controller: on every pass, look, decide, and ask.
///
/// Written against the capabilities alone, and holding nothing from one pass to the next — the
/// listing a pass acts on is the one it asked for itself. A pass whose listing does not come back
/// in time does nothing, and the next pass looks again.
async fn control<C, N>(clock: &C, endpoint: &N, server: NodeId)
where
    C: Clock,
    N: Network<Message = Message>,
{
    for pass in 1..=PASSES {
        // A pass opens on the period rather than whenever the last one finished, so which pass a
        // window of simulated time falls in is not something the run's draws can move.
        clock.sleep_until(opens(pass)).await;
        endpoint.send(server, Message::List);

        let deadline = after(opens(pass), PATIENCE);
        // Only the server talks to the controller and it only ever sends a listing, so the first
        // thing to arrive is this pass's. A listing from a pass gone by could not still be on the
        // wire — a pass gives up well inside its period — and acting on one would do no harm
        // anyway, which is the point of deciding on what is rather than on what changed.
        let Ok(delivery) = clock
            .timeout(until(clock.now(), deadline), endpoint.recv())
            .await
        else {
            continue;
        };
        let Message::Listed(desired, observed) = delivery.into_message() else {
            continue;
        };
        for action in reconcile(&desired, &observed) {
            endpoint.send(server, Message::Apply(action));
        }
    }
}

/// What the server holds, and the replicas it has still to move on.
struct Server<'a, R> {
    rng: R,
    desired: Desired,
    observed: Observed,
    /// Each replica's next phase change, keyed by when it falls and then by the order it was
    /// armed in, so two that fall at one instant happen in a fixed order rather than a lucky one.
    due: BTreeMap<(VirtualTime, u64), (Name, Region)>,
    armed: u64,
    observations: &'a RefCell<Vec<Observation>>,
}

impl<R: Rng> Server<'_, R> {
    /// The instant the next replica moves on, if any has anywhere left to go.
    fn next_due(&self) -> Option<VirtualTime> {
        self.due.keys().next().map(|&(at, _)| at)
    }

    /// Arms the replica of `database` in `region` to move on after a draw from `bounds`.
    fn arm(
        &mut self,
        now: VirtualTime,
        database: &Name,
        region: &Region,
        bounds: RangeInclusive<Duration>,
    ) {
        let at = after(now, self.rng.duration_in(bounds));
        self.due
            .insert((at, self.armed), (database.clone(), region.clone()));
        self.armed += 1;
    }

    /// Makes a change to what is wanted.
    fn change(&mut self, now: VirtualTime, change: Change) {
        let message = format!("{} wanted with {}", change.database, change.placement);
        self.desired.insert(change.database, change.placement);
        self.write(now, message, false);
    }

    /// Moves on the next replica whose time has come, if one has.
    fn advance(&mut self, now: VirtualTime) {
        let Some(entry) = self.due.first_entry() else {
            return;
        };
        if entry.key().0 > now {
            return;
        }
        let (database, region) = entry.remove();
        if !self.observed.advance(&database, &region) {
            return;
        }
        let Some(replica) = self.observed.replica(&database, &region) else {
            return;
        };
        if replica.phase() == Phase::CatchingUp {
            self.arm(now, &database, &region, CATCHING_UP);
        }
        self.write(
            now,
            format!("{database} in {region} {}", replica.phase()),
            false,
        );
    }

    /// Carries out what the controller asked for.
    fn apply(&mut self, now: VirtualTime, action: &Action) {
        let database = action.database();
        let was_lost = self.observed.is_lost(database);
        if !self.observed.apply(action) {
            return;
        }
        match action {
            Action::Create {
                database, region, ..
            } => self.arm(now, database, region, PROVISIONING),
            // A replica taken away takes its next phase change with it, so nothing moves on a
            // replica made later in the same place before that one's own time has come.
            Action::Delete { database, region } => {
                self.due
                    .retain(|_, (owner, place)| owner != database || place != region);
            }
            Action::Promote { .. } => {}
        }
        let destroyed = !was_lost
            && self.observed.is_lost(database)
            && self.desired.databases.contains_key(database);
        self.write(now, action.to_string(), destroyed);
    }

    /// Writes down a change the server has just made.
    ///
    /// Nothing here waits, so the borrow is given up before the server goes round again and is never
    /// held across a poll.
    fn write(&self, at: VirtualTime, message: String, destroyed: bool) {
        self.observations.borrow_mut().push(Observation {
            at,
            message,
            desired: self.desired.clone(),
            observed: self.observed.clone(),
            destroyed,
            converged: converged(&self.desired, &self.observed),
        });
    }
}

/// The server: change what is wanted on the timeline, move replicas on as their time comes, and do
/// whatever it is asked.
///
/// It waits for whichever comes first — the next change to what is wanted, the next replica moving
/// on, or the next request — selecting between them rather than spawning a task for each. Of a
/// change and a replica due at one instant, the change goes first. Once every change is made and
/// every replica is where it is going, it stops when it has been quiet for long enough. Every wait
/// is bounded and the controller's passes by a count, so no seed and no schedule can leave the run
/// waiting for something that is never coming.
async fn serve<C, N, R>(
    clock: &C,
    endpoint: &N,
    rng: R,
    changes: Vec<Change>,
    observations: &RefCell<Vec<Observation>>,
) where
    C: Clock,
    N: Network<Message = Message>,
    R: Rng,
{
    let mut server = Server {
        rng,
        desired: Desired::new(),
        observed: Observed::new(),
        due: BTreeMap::new(),
        armed: 0,
        observations,
    };
    let mut changes = changes.into_iter().peekable();
    loop {
        let next = changes
            .peek()
            .map(|change| change.at)
            .into_iter()
            .chain(server.next_due())
            .min();
        let wait = next.map_or(QUIET, |at| until(clock.now(), at));
        let Ok(delivery) = clock.timeout(wait, endpoint.recv()).await else {
            let now = clock.now();
            if let Some(change) = changes.next_if(|change| change.at <= now) {
                server.change(now, change);
            } else if next.is_some() {
                server.advance(now);
            } else {
                break;
            }
            continue;
        };
        let sender = delivery.sender();
        match delivery.into_message() {
            Message::List => {
                endpoint.send(
                    sender,
                    Message::Listed(server.desired.clone(), server.observed.clone()),
                );
            }
            Message::Apply(action) => server.apply(clock.now(), &action),
            // Nothing sends the server a listing; one arriving is not a request for anything.
            Message::Listed(..) => {}
        }
    }
}

/// Reads the run's verdict off what the server wrote down.
///
/// A database that was still wanted losing its data is the worse failure and has a step of its own
/// to be named at — the one that destroyed it — so it is looked for first. Otherwise the last change
/// the server made is the state the run ended in, and the run held up exactly when that state is
/// what was asked for; a run that did not get there names that last step, since there is no one
/// earlier step a failure to get somewhere can be said to have happened at.
fn verdict(observed: &[Observation]) -> Result<Outcome, ReasonError> {
    if let Some(step) = observed.iter().position(|seen| seen.destroyed) {
        return Ok(Outcome::Fail {
            reason: Reason::new("lost data")?,
            step,
        });
    }
    match observed.iter().enumerate().next_back() {
        Some((step, last)) if !last.converged => Ok(Outcome::Fail {
            reason: Reason::new("did not converge")?,
            step,
        }),
        _ => Ok(Outcome::Pass),
    }
}

/// Turns what the server observed into steps, keeping every state it passed through in a store.
fn collect(observed: Vec<Observation>) -> Result<(Vec<Step>, StateStore), RunError> {
    let mut store = StateStore::new();
    let mut steps = Vec::with_capacity(observed.len());
    for step in observed {
        let event = Entry::new(step.at, step.message)?;
        let world = world(&step.desired, &step.observed)?;
        steps.push(Step::new(event, store.insert(&world.snapshot())));
    }
    Ok((steps, store))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A name, failing the test if it is not one.
    fn name(name: &str) -> Name {
        Name::new(name).unwrap_or_else(|e| panic!("{name:?} is a name: {e}"))
    }

    /// A region, failing the test if its name is not one.
    fn region(region: &str) -> Region {
        Region::new(name(region))
    }

    /// A placement, failing the test if it is not one.
    fn placement(primary: &str, standbys: &[&str]) -> Placement {
        Placement::new(
            region(primary),
            standbys.iter().map(|&standby| region(standby)),
        )
        .unwrap_or_else(|e| panic!("a placement a test writes is a placement: {e}"))
    }

    /// What is wanted: `orders` placed as `primary` with `standbys`.
    fn desired(primary: &str, standbys: &[&str]) -> Desired {
        let mut desired = Desired::new();
        desired.insert(name("orders"), placement(primary, standbys));
        desired
    }

    /// What exists, built the only ways a server builds it: by creating each replica of `orders` and
    /// then letting its time come until it reaches the phase given.
    fn observed(replicas: &[(&str, Role, Phase)]) -> Observed {
        let mut observed = Observed::new();
        for &(place, role, phase) in replicas {
            assert!(
                observed.apply(&Action::Create {
                    database: name("orders"),
                    region: region(place),
                    role,
                }),
                "{place} is created"
            );
            while observed
                .replica(&name("orders"), &region(place))
                .is_some_and(|replica| replica.phase() != phase)
            {
                assert!(
                    observed.advance(&name("orders"), &region(place)),
                    "{place} reaches {phase}"
                );
            }
        }
        observed
    }

    /// Lets every replica's time come until none has anywhere left to go.
    fn settle(observed: &mut Observed) {
        let replicas: Vec<(Name, Region)> = observed.replicas.keys().cloned().collect();
        for (database, region) in replicas {
            while observed.advance(&database, &region) {}
        }
    }

    /// The actions as a person is shown them.
    fn shown(actions: &[Action]) -> Vec<String> {
        actions.iter().map(ToString::to_string).collect()
    }

    /// What exists, as a person is shown it.
    fn listed(observed: &Observed) -> Vec<String> {
        observed
            .replicas()
            .map(|(database, region, replica)| {
                format!(
                    "{database} in {region}: {} {}",
                    replica.role(),
                    replica.phase()
                )
            })
            .collect()
    }

    use Phase::{CatchingUp, Provisioning, Ready};
    use Role::{Primary, Standby};

    #[test]
    fn reconcile_asks_for_exactly_the_difference() {
        struct Case {
            name: &'static str,
            desired: Desired,
            observed: &'static [(&'static str, Role, Phase)],
            expected: &'static [&'static str],
        }
        let cases = [
            Case {
                name: "nothing wanted and nothing there",
                desired: Desired::new(),
                observed: &[],
                expected: &[],
            },
            Case {
                name: "everything already as wanted",
                desired: desired("east", &["west"]),
                observed: &[("east", Primary, Ready), ("west", Standby, Ready)],
                expected: &[],
            },
            Case {
                name: "replicas on their way need only time",
                desired: desired("east", &["west"]),
                observed: &[
                    ("east", Primary, Provisioning),
                    ("west", Standby, CatchingUp),
                ],
                expected: &[],
            },
            Case {
                name: "wanted and missing, with no primary anywhere",
                desired: desired("east", &["west"]),
                observed: &[],
                expected: &[
                    "create orders in east as primary",
                    "create orders in west as standby",
                ],
            },
            Case {
                name: "a primary wanted where the database already has one is made a standby first",
                desired: desired("west", &[]),
                observed: &[("east", Primary, Ready)],
                expected: &["delete orders in east", "create orders in west as standby"],
            },
            Case {
                name: "a standby in sync where the primary is wanted is promoted",
                desired: desired("west", &["east"]),
                observed: &[("east", Primary, Ready), ("west", Standby, Ready)],
                expected: &["promote orders in west"],
            },
            Case {
                name: "a standby still catching up is not promoted yet",
                desired: desired("west", &["east"]),
                observed: &[("east", Primary, Ready), ("west", Standby, CatchingUp)],
                expected: &[],
            },
            Case {
                name: "there and not wanted",
                desired: desired("east", &[]),
                observed: &[("east", Primary, Ready), ("west", Standby, Ready)],
                expected: &["delete orders in west"],
            },
        ];
        for case in cases {
            assert_eq!(
                shown(&reconcile(&case.desired, &observed(case.observed))),
                case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn from_every_state_the_loop_settles_where_it_was_asked() {
        // The level-triggered claim, asked of every state a controller could find one database in
        // across three regions: nothing there, a standby in each phase, or a primary in each phase,
        // with at most one primary since the world allows no more. A controller that restarts finds
        // one of these with no memory of how it got there. Passes alternate with the passage of
        // time, and three are enough from anywhere: one to create, one to promote what was created,
        // and one to find nothing left to do.
        //
        // What this does not ask is whether the world got there *safely*. Some of these starts hold
        // a primary in the region nobody wants, and the decision deletes it in the same pass as it
        // creates the standby that will replace it — so from those, the data is destroyed on the way.
        // Getting there is the decision's claim; the order is not one it makes.
        let want = desired("east", &["west"]);
        let options: [Option<(Role, Phase)>; 6] = [
            None,
            Some((Standby, Provisioning)),
            Some((Standby, CatchingUp)),
            Some((Standby, Ready)),
            Some((Primary, Provisioning)),
            Some((Primary, Ready)),
        ];
        let regions = ["east", "south", "west"];
        let mut tried = 0;
        for code in 0..options.len().pow(3) {
            let mut rest = code;
            let chosen: Vec<(&str, Role, Phase)> = regions
                .iter()
                .filter_map(|&place| {
                    let option = options[rest % options.len()];
                    rest /= options.len();
                    option.map(|(role, phase)| (place, role, phase))
                })
                .collect();
            if chosen
                .iter()
                .filter(|&&(_, role, _)| role == Primary)
                .count()
                > 1
            {
                continue;
            }
            let mut world = observed(&chosen);
            let mut passes = 0;
            while !converged(&want, &world) {
                assert!(passes < 3, "from {chosen:?}, still at {:?}", listed(&world));
                for action in reconcile(&want, &world) {
                    world.apply(&action);
                }
                settle(&mut world);
                passes += 1;
            }
            assert!(
                reconcile(&want, &world).is_empty(),
                "from {chosen:?}, nothing left to ask"
            );
            tried += 1;
        }
        assert_eq!(
            tried, 160,
            "every one of the states with at most one primary, not a corner of them"
        );
    }

    #[test]
    fn applying_an_action_twice_is_applying_it_once() {
        struct Case {
            name: &'static str,
            before: &'static [(&'static str, Role, Phase)],
            action: Action,
            after: &'static [&'static str],
        }
        let cases = [
            Case {
                name: "a create",
                before: &[],
                action: Action::Create {
                    database: name("orders"),
                    region: region("east"),
                    role: Primary,
                },
                after: &["orders in east: primary provisioning"],
            },
            Case {
                name: "a promotion",
                before: &[("east", Primary, Ready), ("west", Standby, Ready)],
                action: Action::Promote {
                    database: name("orders"),
                    region: region("west"),
                },
                after: &[
                    "orders in east: standby ready",
                    "orders in west: primary ready",
                ],
            },
            Case {
                name: "a delete",
                before: &[("east", Primary, Ready), ("west", Standby, Ready)],
                action: Action::Delete {
                    database: name("orders"),
                    region: region("west"),
                },
                after: &["orders in east: primary ready"],
            },
        ];
        for case in cases {
            let mut world = observed(case.before);
            assert!(
                world.apply(&case.action),
                "{}: the first one changes",
                case.name
            );
            let once = world.clone();
            assert!(
                !world.apply(&case.action),
                "{}: the second one does not",
                case.name
            );
            assert_eq!(world, once, "{}", case.name);
            assert_eq!(listed(&world), case.after, "{}", case.name);
        }
    }

    #[test]
    fn an_action_on_a_world_it_no_longer_fits_changes_nothing() {
        // What makes a stale listing harmless: an action decided against a world that has moved on
        // since is either already done or no longer possible, and either way does nothing.
        struct Case {
            name: &'static str,
            before: &'static [(&'static str, Role, Phase)],
            action: Action,
        }
        let cases = [
            Case {
                name: "creating what is already there, even in another role",
                before: &[("east", Standby, Ready)],
                action: Action::Create {
                    database: name("orders"),
                    region: region("east"),
                    role: Primary,
                },
            },
            Case {
                name: "creating a second primary",
                before: &[("east", Primary, Provisioning)],
                action: Action::Create {
                    database: name("orders"),
                    region: region("west"),
                    role: Primary,
                },
            },
            Case {
                name: "promoting a standby still catching up",
                before: &[("east", Primary, Ready), ("west", Standby, CatchingUp)],
                action: Action::Promote {
                    database: name("orders"),
                    region: region("west"),
                },
            },
            Case {
                name: "promoting a standby still being provisioned",
                before: &[("east", Primary, Ready), ("west", Standby, Provisioning)],
                action: Action::Promote {
                    database: name("orders"),
                    region: region("west"),
                },
            },
            Case {
                name: "promoting the primary",
                before: &[("east", Primary, Ready)],
                action: Action::Promote {
                    database: name("orders"),
                    region: region("east"),
                },
            },
            Case {
                name: "promoting what is not there",
                before: &[("east", Primary, Ready)],
                action: Action::Promote {
                    database: name("orders"),
                    region: region("west"),
                },
            },
            Case {
                name: "deleting what is not there",
                before: &[("east", Primary, Ready)],
                action: Action::Delete {
                    database: name("orders"),
                    region: region("west"),
                },
            },
        ];
        for case in cases {
            let mut world = observed(case.before);
            assert!(!world.apply(&case.action), "{}", case.name);
            assert_eq!(world, observed(case.before), "{}", case.name);
        }
    }

    #[test]
    fn deleting_a_primary_destroys_the_data_unless_a_standby_has_caught_up() {
        // The one move whose order matters, and the fact the world keeps about it. A standby that has
        // not caught up holds less than its primary did, so it is no copy of what was lost.
        struct Case {
            name: &'static str,
            before: &'static [(&'static str, Role, Phase)],
            deleted: &'static str,
            lost: bool,
        }
        let cases = [
            Case {
                name: "the primary, with a standby in sync",
                before: &[("east", Primary, Ready), ("west", Standby, Ready)],
                deleted: "east",
                lost: false,
            },
            Case {
                name: "the primary, with its one standby catching up",
                before: &[("east", Primary, Ready), ("west", Standby, CatchingUp)],
                deleted: "east",
                lost: true,
            },
            Case {
                name: "the primary, with its one standby being provisioned",
                before: &[("east", Primary, Ready), ("west", Standby, Provisioning)],
                deleted: "east",
                lost: true,
            },
            Case {
                name: "the primary, with one standby behind and another in sync",
                before: &[
                    ("east", Primary, Ready),
                    ("south", Standby, CatchingUp),
                    ("west", Standby, Ready),
                ],
                deleted: "east",
                lost: false,
            },
            Case {
                name: "the primary, with no standby at all",
                before: &[("east", Primary, Ready)],
                deleted: "east",
                lost: true,
            },
            Case {
                name: "a standby catching up, with the primary left standing",
                before: &[("east", Primary, Ready), ("west", Standby, CatchingUp)],
                deleted: "west",
                lost: false,
            },
        ];
        for case in cases {
            let mut world = observed(case.before);
            assert!(
                world.apply(&Action::Delete {
                    database: name("orders"),
                    region: region(case.deleted),
                }),
                "{}: the replica goes",
                case.name
            );
            assert_eq!(world.is_lost(&name("orders")), case.lost, "{}", case.name);
        }
    }

    #[test]
    fn a_database_once_lost_stays_lost() {
        // Bringing the replicas back gives the database somewhere to live again, not what it held.
        let mut world = observed(&[("east", Primary, Ready), ("west", Standby, CatchingUp)]);
        world.apply(&Action::Delete {
            database: name("orders"),
            region: region("east"),
        });
        settle(&mut world);
        for action in reconcile(&desired("east", &["west"]), &world) {
            world.apply(&action);
        }
        settle(&mut world);

        assert!(converged(&desired("east", &["west"]), &world));
        assert!(world.is_lost(&name("orders")));
    }

    #[test]
    fn a_replica_moves_through_its_phases_in_order() {
        struct Case {
            name: &'static str,
            role: Role,
            expected: &'static [Phase],
        }
        let cases = [
            Case {
                name: "a primary is provisioned and then serves",
                role: Primary,
                expected: &[Provisioning, Ready],
            },
            Case {
                name: "a standby catches up in between",
                role: Standby,
                expected: &[Provisioning, CatchingUp, Ready],
            },
        ];
        for case in cases {
            let mut world = Observed::new();
            world.apply(&Action::Create {
                database: name("orders"),
                region: region("east"),
                role: case.role,
            });
            let mut phases = Vec::new();
            loop {
                let replica = world
                    .replica(&name("orders"), &region("east"))
                    .unwrap_or_else(|| panic!("{}: the replica is there", case.name));
                phases.push(replica.phase());
                if !world.advance(&name("orders"), &region("east")) {
                    break;
                }
            }
            assert_eq!(phases, case.expected, "{}", case.name);
        }
    }

    #[test]
    fn a_placement_cannot_ask_one_region_for_two_replicas() {
        assert_eq!(
            Placement::new(region("east"), [region("west"), region("east")]),
            Err(PlacementError::PrimaryAlsoStandby {
                region: region("east")
            })
        );
        assert_eq!(
            placement("east", &["west", "south", "west"]).to_string(),
            "primary east and standbys south, west",
            "a standby asked for twice is one standby, and they are listed in name order"
        );
        assert_eq!(
            placement("east", &[]).to_string(),
            "primary east and no standbys"
        );
    }

    #[test]
    fn a_state_names_each_field_after_the_region_it_is_about() {
        let world = world(
            &desired("east", &["west"]),
            &observed(&[("east", Primary, Ready), ("west", Standby, CatchingUp)]),
        )
        .unwrap_or_else(|e| panic!("the fields are names: {e}"));
        let orders = world
            .get(&name("orders"))
            .unwrap_or_else(|| panic!("orders is in the world"));
        let fields: Vec<String> = orders
            .fields()
            .map(|(field, value)| format!("{field} {value}"))
            .collect();

        assert_eq!(
            fields,
            [
                "east-phase \"ready\"",
                "east-role \"primary\"",
                "east-wanted \"primary\"",
                "lost false",
                "west-phase \"catching up\"",
                "west-role \"standby\"",
                "west-wanted \"standby\"",
            ]
        );
    }

    #[test]
    fn the_verdict_names_the_step_that_destroyed_data_before_anything_else() {
        struct Case {
            name: &'static str,
            observed: Vec<Observation>,
            expected: Option<(&'static str, usize)>,
        }
        let seen = |destroyed: bool, converged: bool| Observation {
            at: VirtualTime::from_nanos(0),
            message: String::from("something"),
            desired: Desired::new(),
            observed: Observed::new(),
            destroyed,
            converged,
        };
        let cases = [
            Case {
                name: "nothing happened at all",
                observed: vec![],
                expected: None,
            },
            Case {
                name: "ended where it was asked",
                observed: vec![seen(false, false), seen(false, true)],
                expected: None,
            },
            Case {
                name: "ended short",
                observed: vec![seen(false, true), seen(false, false)],
                expected: Some(("did not converge", 1)),
            },
            Case {
                name: "destroyed data and then ended short",
                observed: vec![seen(false, true), seen(true, false), seen(false, false)],
                expected: Some(("lost data", 1)),
            },
            Case {
                name: "destroyed data and then ended where it was asked anyway",
                observed: vec![seen(true, false), seen(false, true)],
                expected: Some(("lost data", 0)),
            },
        ];
        for case in cases {
            let expected = match case.expected {
                None => Outcome::Pass,
                Some((reason, step)) => Outcome::Fail {
                    reason: Reason::new(reason).unwrap_or_else(|e| panic!("a reason: {e}")),
                    step,
                },
            };
            assert_eq!(
                verdict(&case.observed).unwrap_or_else(|e| panic!("a verdict: {e}")),
                expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_run_that_meets_no_faults_ends_with_what_was_asked_for() {
        let (trace, _, outcome) = run(20_261_005, &FaultSchedule::default())
            .unwrap_or_else(|e| panic!("the run finishes: {e}"));

        assert_eq!(outcome, Outcome::Pass);
        let messages: Vec<&str> = trace
            .steps()
            .iter()
            .map(|step| step.event().message())
            .collect();
        for expected in [
            "create orders in east as primary",
            "orders in west catching up",
            "create orders in south as standby",
            "delete orders in west",
        ] {
            assert!(messages.contains(&expected), "{expected} in {messages:?}");
        }
    }

    /// The faults drawn for seed 5, recorded from an actual draw.
    ///
    /// Pinned rather than compared with a second draw, which would be the same function asked
    /// twice: this is what holds the order of the draws — direction, kind, odds, start, length — and
    /// which of the run's root values the schedule is drawn from.
    const DRAWN_FOR_5: &str = "chronoloop faults\n\
loss 9 in 10 on node 1 -> node 0 from 23.548411241s until 28.168587270s\n\
loss 2 in 10 on node 1 -> node 0 from 7.462983351s until 10.234732660s\n\
partition on node 0 -> node 1 from 10.466233633s until 17.164792136s\n\
partition on node 0 -> node 1 from 25.751580658s until 30.000000000s\n\
loss 8 in 10 on node 0 -> node 1 from 5.307097372s until 11.168506268s\n\
partition on node 0 -> node 1 from 1.121021899s until 9.976940227s\n";

    #[test]
    fn a_seed_draws_the_faults_it_was_recorded_drawing() {
        assert_eq!(drawn_faults(5).to_string(), DRAWN_FOR_5);
    }

    #[test]
    fn different_seeds_draw_different_trouble() {
        // Compared as faults rather than as text, though a schedule's text names no seed: the habit
        // costs nothing and the day it does name one is not the day to remember.
        let drawn: BTreeSet<String> = (0..50)
            .map(|seed| format!("{:?}", drawn_faults(seed).faults()))
            .collect();
        assert_eq!(drawn.len(), 50);
    }

    #[test]
    fn drawn_trouble_stays_on_the_link_and_is_over_before_the_last_passes() {
        // What a controller is owed: trouble only where trouble can be, and none once the last
        // passes open. Five hundred seeds of drawing, which costs no runs at all.
        let healed = opens(PASSES + 1 - CLEAR_PASSES);
        assert_eq!(healed, VirtualTime::from_nanos(30_000_000_000));
        let link = [
            (NodeId::from_index(0), NodeId::from_index(1)),
            (NodeId::from_index(1), NodeId::from_index(0)),
        ];
        let mut kinds = BTreeSet::new();
        for seed in 0..500 {
            let drawn = drawn_faults(seed);
            assert_eq!(drawn.len(), TROUBLE, "seed {seed}");
            for fault in drawn.faults() {
                let (from, to, during) = match fault {
                    Fault::Partition { from, to, during } => {
                        kinds.insert("partition");
                        (*from, *to, *during)
                    }
                    Fault::Loss {
                        from, to, during, ..
                    } => {
                        kinds.insert("loss");
                        (*from, *to, *during)
                    }
                };
                assert!(link.contains(&(from, to)), "seed {seed}: {fault}");
                assert!(during.end() <= healed, "seed {seed}: {fault}");
                assert!(during.start() < during.end(), "seed {seed}: {fault}");
            }
        }
        assert_eq!(kinds.len(), 2, "both kinds are drawn");
    }
}
