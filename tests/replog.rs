//! Five replicas keeping a log over the simulated network, with three clients sending them commands.
//!
//! The unit tests beside [`chronoloop::systems::replog`] hold the protocol to account one message
//! at a time, on replicas a case builds by hand. This one asks what those cannot: does a **run** —
//! timeouts drawn, messages landing in whatever order the wire gives them — elect a leader and
//! commit every command in an order its clients could have seen, and what becomes of the log when
//! one replica spends a partition alone?
//!
//! Schedules are read back from the text a schedule is written in, for the reason
//! `tests/outcome.rs` gives. The clients are nodes 0 to 2 and the replicas nodes 3 to 7.
//!
//! What these cases can and cannot feel: the clients' commands open on a fixed period, so the seed
//! reaches a run through how long each message spends on the wire and through each replica's
//! election timeouts. The recorded trace feels both. The cases under faults run one seed, [`SEED`],
//! and are about a shape of trouble rather than about the draws: each asserts the shape it was
//! built for — who led when, who asked — so a change of timings that moves the run out from under
//! its schedule goes red rather than quiet.
//!
//! A replica asks whether it could stand before it stands, and both the asking and the vote are
//! refused to a replica whose log is behind. Under a vote that asked nothing of the candidate's log,
//! seed 0 lost four committed entries and 475 of the gated sweep's 500 seeds lost some under the
//! isolation; that vote is in git history at `2ff3f46`. Which cases hold which part of the rule, by
//! breaking each part and watching what goes red:
//!
//! - **asking before standing**: the unit cases on asking, and every run whose shape depends on
//!   it — the pinned trace, the isolation, the replica that cannot hear, the stranded leader, the
//!   order the clients saw. Not the gated sweep: without it, a deaf replica only slows a run down.
//! - **the asked-about term not being taken**: thirteen cases, the gated sweep among them.
//! - **a replica following a leader saying no**: the pre-vote table and the stranded leader's case.
//! - **the log asked of the pre-vote**: the pre-vote table and the stranded leader's case, where the
//!   asker holds more entries than anyone and its last is from a term gone by.
//! - **the log asked of the vote itself**: the unit cases beside the module alone. Every stale
//!   replica in these runs is turned away at the asking, so none ever reaches a vote; the rule is
//!   there because a pre-vote promises nothing, and nothing here makes a run that needs it.
//! - **the later last term winning over length**: both tables and the stranded leader's case.
//! - **length deciding between two equal last terms**: both tables alone.
//! - **a refusal leaving the vote unspent**, **an answer to an earlier question counting for
//!   nothing**, and **hearing a leader ending the asking**: the unit cases alone.
//! - **the asker naming its last entry's term rather than its own**: the unit cases on asking and
//!   the stranded leader's case.
//!
//! The order the log applied the clients' commands in is checked here a second time, by a route the
//! module's own check does not take: rebuilt from what each step says — which command a leader took
//! as which entry, and when a client sent it and heard it was done — rather than from the fields of
//! the world it recorded. Nothing here makes a run break that order, since nothing in the module
//! does; the unit cases beside it hold the check to account on worlds built to break it.

use core::ops::Range;
use std::collections::{BTreeMap, BTreeSet};

use chronoloop::clock::VirtualTime;
use chronoloop::fault::FaultSchedule;
use chronoloop::outcome::Outcome;
use chronoloop::store::StateStore;
use chronoloop::systems::replog;
use chronoloop::trace::Trace;
use chronoloop::world::{Name, Value, World};

#[path = "common/survey.rs"]
mod survey;

use survey::survey;

/// The seed the recorded case runs, since it is not about a particular one.
const SEED: u64 = 20_261_006;

/// Node 7, the last replica, cut off from every other node, both ways, from one second to nine.
///
/// It is gone before any replica's first timeout can run out, so it never leads the first term and
/// misses the first four rounds of commands; and it is back with twenty seconds of the run still to
/// go.
const ISOLATED: &str = "chronoloop faults\n\
                        partition on node 7 -> node 0 from 1.000000000s until 9.000000000s\n\
                        partition on node 0 -> node 7 from 1.000000000s until 9.000000000s\n\
                        partition on node 7 -> node 1 from 1.000000000s until 9.000000000s\n\
                        partition on node 1 -> node 7 from 1.000000000s until 9.000000000s\n\
                        partition on node 7 -> node 2 from 1.000000000s until 9.000000000s\n\
                        partition on node 2 -> node 7 from 1.000000000s until 9.000000000s\n\
                        partition on node 7 -> node 3 from 1.000000000s until 9.000000000s\n\
                        partition on node 3 -> node 7 from 1.000000000s until 9.000000000s\n\
                        partition on node 7 -> node 4 from 1.000000000s until 9.000000000s\n\
                        partition on node 4 -> node 7 from 1.000000000s until 9.000000000s\n\
                        partition on node 7 -> node 5 from 1.000000000s until 9.000000000s\n\
                        partition on node 5 -> node 7 from 1.000000000s until 9.000000000s\n\
                        partition on node 7 -> node 6 from 1.000000000s until 9.000000000s\n\
                        partition on node 6 -> node 7 from 1.000000000s until 9.000000000s\n";

/// The instant [`ISOLATED`] heals at, in nanoseconds.
const HEALED: u64 = 9_000_000_000;

/// How many seeds the gated sweep covers under each schedule — the same five hundred the vote that
/// asked nothing of the candidate's log was measured on.
const SWEEP: u64 = 500;

/// The run `SEED` produces with nothing in its way, recorded from an actual run.
///
/// Node 5's timeout runs out first, at 1.96s. It asks whether it could stand, a majority says it
/// could, and it stands at 2.07s and every other replica votes for it. Its no-op is entry 1, and
/// each command after it is taken, copied, committed by the leader once three replicas hold it, and
/// answered — the followers hearing of the commit on the next heartbeat, half a second later. The
/// command entries are 2 to 16, and no one stands against node 5 again. Each round's three commands
/// are taken in whatever order they reach the leader, which is not the order they are numbered in:
/// command 2 is entry 2 and command 1 entry 3.
const RECORDED: &str = "chronoloop trace seed 20261006\n\
step 0 0.000000000s e23fb45b2b8f55d00f3a20685b1ae4d7c00b527473d7da131866432c70ff2134 node-3 started as a follower in term 0\n\
step 1 0.000000000s 5dfbfc08e04d1c91fa8210f129f2eea098490466f288ba9b8b808272f088a248 node-4 started as a follower in term 0\n\
step 2 0.000000000s 90bf7b15cd7d923ed3ad594e398c8fde3b9ffb0f262ca65a64116bd2219ef125 node-5 started as a follower in term 0\n\
step 3 0.000000000s 4b246fdd9e58a9b79991a7a8e21282489a0398436a9adc1268ae2456d26c5209 node-6 started as a follower in term 0\n\
step 4 0.000000000s b3c5fa999971079f393e1d76e425d857ec0c15a2d39fdabb8ea0369175655de0 node-7 started as a follower in term 0\n\
step 5 1.964643210s b3c5fa999971079f393e1d76e425d857ec0c15a2d39fdabb8ea0369175655de0 node-5 asked whether it could stand for term 1\n\
step 6 2.000000000s 39de5cda8a588498fc7a44385315c4b268dcb7cf7dff3006ff786f85574e0745 client-0 sent command 1\n\
step 7 2.000000000s 132a7e4156c4016b1f8dcef30094d33ea0c21f7db5601bfd986d806f822b228b client-1 sent command 2\n\
step 8 2.000000000s e786b125d3d27d8ebbce33360dbbbd976c6a7e9128d05617ab4a9257d099331b client-2 sent command 3\n\
step 9 2.069399162s 90eea5be6d1d654e62e3a56fb29f9963ef1c50be928b2f20bc535bc8cf4ff754 node-5 became candidate for term 1\n\
step 10 2.100764061s f89f83457083d5e5cba010cf89c343bff695b5149ceafff958f08047cd072756 node-7 voted for node-5 in term 1\n\
step 11 2.112691893s fc141c1efb6e1f3f7a408c04fd467fad4f56c09de54ea287bcedd304486f280e node-6 voted for node-5 in term 1\n\
step 12 2.115240061s 052417f04a294e1ae9d87a9e8b83d62d277870aca6883caa75c46a25d30e649e node-3 voted for node-5 in term 1\n\
step 13 2.132018736s bb126518e89b26c0de1c38b74fb62868f76542a4a118d7bb329076f3d8412022 node-4 voted for node-5 in term 1\n\
step 14 2.155305214s 18cca6b47598417e76c6e3d3ed500b48a4540708cb34d5833d628b020e2c6c0e node-5 became leader of term 1\n\
step 15 2.169915435s 205dcfc921585dadc9a12e82f90688bffcea2b4e8e93548f53d68e8b16d3a2bb node-4 appended entry 1\n\
step 16 2.205821454s 3f5375baeb9ee466f133443328d3736a5075eeaeee84a283f54c1f6ff2157db5 node-6 appended entry 1\n\
step 17 2.234101108s 929387a176a291c7385e77aa69699415ad3161830064597727f72c831a420839 node-7 appended entry 1\n\
step 18 2.251228961s 2ae4615861c7c517ed296eab8bde713e007cc08ce9ed473480efd9534ec19715 node-5 took command 2 as entry 2\n\
step 19 2.253617880s 924b3b9100cabeae83b83267b34022b493944c3e1baf840ba071aa2b9ee8daab node-3 appended entry 1\n\
step 20 2.267071879s 0e13ddebf5177adf8a77836ebd8534706c2e5dea8e08d3ee7673593ae7125889 node-5 committed through 1\n\
step 21 2.271667997s 7e88905343db0f6fb87b02834c8f20db5d7cfc693acdaa5a376ebcf7e806ea39 node-3 appended entry 2\n\
step 22 2.281972162s 0e4aacc76efe3492b0c82e59cd4eb8c7b7cd6585c966c8961580dd412f6fed1e node-4 appended entry 2\n\
step 23 2.281972162s 9e7dee2ebd4b7ec178b61d71ff6fd15e71662f892bd45f5638b33de196264789 node-4 committed through 1\n\
step 24 2.302133752s 3a04f789511062ad1d8bb7e3e66126c9e2a31781abfc91fced5496b174fc3b05 node-5 took command 1 as entry 3\n\
step 25 2.321140972s 784fea71dd917a04c131999334a75a1552bbfefd27b6e13dad2b9346be8eeab0 node-4 appended entry 3\n\
step 26 2.323605650s df44f4cb5d448273a9f6e92b404cc88748e403f4c8f51a8764b06ffdb2d4e77a node-5 committed through 2\n\
step 27 2.337152531s 4e5cec0f3d4322fcde48888971205131b4f7fdd523ce4b760aced6258793604c node-6 appended entry 2\n\
step 28 2.341288385s a62b44ed68e00ece16649019d3f0a7bda2d17fcfb05e9f3c94a11ab180213830 node-7 appended entry 2\n\
step 29 2.341791725s f15514d3456fbcd04cb178d34cb15410b984880831c2fc95fb5c87a5e9b9c631 node-3 appended entry 3\n\
step 30 2.341791725s fbd682747c7f538bcd3d875cf0d51a46ad3d07d6dbe9c892ca92606bfe2b248c node-3 committed through 1\n\
step 31 2.354270780s a0df55ee7ecd98899969fadf35056b14f4438133ab75cdd52c07fb08de214d84 node-3 committed through 2\n\
step 32 2.367640455s d4b068bd0c8f20d0b1648d0d7f8ad2aa32b683f9f19a15119c730bd60aa483be node-7 committed through 1\n\
step 33 2.368632763s 7820e2b960190a9d4d0232836797f85133c549694e7bdb070a6b088755b394c0 node-6 appended entry 3\n\
step 34 2.368632763s 652b9680dd266e65b89062e47ef7fca20cb3a6f9c59a73482d8b07c84e92b330 node-6 committed through 1\n\
step 35 2.377187333s 28392f3976cf0a4f0577fba898873c197cdd4fb4c103ed8a90f2d6981d9a0b3a node-5 committed through 3\n\
step 36 2.384265542s 11ffae39acba823dddc82c733590224617732b948d5ea9fb4a465025690e41bf node-7 appended entry 3\n\
step 37 2.384265542s 726154360640c9133451ed8777d808f51f62f09fd13f7516f568bd645485acfd node-7 committed through 2\n\
step 38 2.401380648s c71d92d7bcf2687d81eebba123097906f3b5fcb249255fb7f2e01fd6bd1c114d client-1 heard command 2 is done\n\
step 39 2.435380250s a5177affbb0ae9daf02dafbdb0212b558a781a3c6ec2b3ff0eb127be66ae9a08 client-0 heard command 1 is done\n\
step 40 2.439454171s 370f5fd30217cb8a528700a8cc5e992829c67b0be89c2ea01fcac857099968e0 node-6 committed through 3\n\
step 41 2.439520596s 1dd1ab1a4a92149c33a96bc9117cfe1c8ec8bab4a074258a378f537574f670cc node-5 took command 3 as entry 4\n\
step 42 2.452089044s 36c751c02ad83dedd585509d13abffc20f2f18491f59add056c8385440718b20 node-6 appended entry 4\n\
step 43 2.458574066s 89842452fa40fa57b371a7ccfe6a4b08cfbe9d757a4aa2d9c415155b965eb5a5 node-3 appended entry 4\n\
step 44 2.458574066s 745f9197f1be304298662889748f2beaddc7364f2ceeebf3a721d474b512e6ad node-3 committed through 3\n\
step 45 2.460165431s d071ed59bcf7e4f220c816a48ecf08e53d846712d144f31bc4fe731c921c04b3 node-4 committed through 2\n\
step 46 2.463074655s 821a18b3fde386a14e96d04ab625fec203081baabeb3212df48ed741402ec3f3 node-4 appended entry 4\n\
step 47 2.463074655s 1edc9d3fea6d94d0a45aab82e4f855e38f1fbcd6dd4b256cc3815c0393b9e201 node-4 committed through 3\n\
step 48 2.485542990s 80049aff6fb4aaad6a214fee4a3f5036100a81b6a78b944dd77b91f6de94a440 node-7 appended entry 4\n\
step 49 2.485542990s 28ad73396f58fff653a3faf52cc249ac321e5802adef74bd004b6d7fd9321cd7 node-7 committed through 3\n\
step 50 2.535631326s 611fcc951b7344b61c634b260a3972e9fe82fb06689bae1c436a9bf871f17cc9 node-5 committed through 4\n\
step 51 2.605766881s 2909a06b2f6a54f3692bfb69a1d1489307172b2e309156641c13218292fc2762 client-2 heard command 3 is done\n\
step 52 2.675947991s f7a622338aed33c235cc37eac3d494419e6dfc14d8254af537e10f81ee673af7 node-4 committed through 4\n\
step 53 2.676996859s 0b146e69c2121c638ac1ef3c0f9c5a885abf2519619f4409c8c0dbe9ef96733e node-7 committed through 4\n\
step 54 2.721963611s 389c36814f4cf0b90ce3b3e8c97c706119e42656063f70a8c6fe6b4890a7480f node-6 committed through 4\n\
step 55 2.743986053s c8f349d75b53b2e5293000a5c9bac4ac0e9769c7d062e0828cb2f5a25c73f6a2 node-3 committed through 4\n\
step 56 4.000000000s c4bdf316d217d72636a64559cf54d9bb3ddfccdc4c570adec12a7f5e59526a3e client-1 sent command 5\n\
step 57 4.000000000s c966eec96eec544098ec0eb11acd83d351c2d85845cefc0b53b0ff0c7861099a client-0 sent command 4\n\
step 58 4.000000000s b24fd0fdd7fe3cd4439ac33a4b18fb45ff8468adb9cc2452875efb47240f010c client-2 sent command 6\n\
step 59 4.049871800s 59a4acc183b5dcb5c70ebd2e882fe351a31b353420aeda105f53ecc3936fc916 node-5 took command 4 as entry 5\n\
step 60 4.060653899s 748bfd434e5d9ec0c37abc205cda15d7e92b7a33b15a096d689304a315100675 node-5 took command 5 as entry 6\n\
step 61 4.062491445s 8ff51c4372cc80e80bcaeb8062668b03ca22ab60e77bf22c03d5092c13c06834 node-5 took command 6 as entry 7\n\
step 62 4.075797272s 7aaab266a5b972151e61d77640f6b4c9392441e4897c1c54793fcb095579b664 node-3 appended entry 5\n\
step 63 4.078960330s 74e393f7c2b4c873adc1fe9c452459fa53671c7cb80516eba119e84dc31f431d node-4 appended entries 5 to 6\n\
step 64 4.095180208s 5f9c9b352a8baa6c4dc6a7d5527c20152a1d052430897e748f21596e1d33ca19 node-7 appended entry 5\n\
step 65 4.099471189s 321a6419b47761b63131b368eaaffb9df38bb518727fb8dd51528d381c8ae5ea node-7 appended entries 6 to 7\n\
step 66 4.109718470s c47ca0dbd750348b6d96af1056e44d682f094295061f931d4b65ef778b93b8a6 node-6 appended entry 5\n\
step 67 4.123875707s c1056da2a958a06ef7b3b839a03381b242a00f2441c03dac09a686f6bdd1554b node-5 committed through 5\n\
step 68 4.124676338s 39380aaa92f7cd70daf5992d98e951dc4e430551f9d6b2bf45c3bea32ef2bae9 node-6 appended entry 6\n\
step 69 4.137691597s 81c8f5feb9544dfc381c44fb8b01205d4ac14c9194b9a9486ace41188833c774 node-4 appended entry 7\n\
step 70 4.140102753s 95e550fbf0ae7e8190041d36b48316f86b6cdf5508475596dab38e29b9e57079 node-5 committed through 6\n\
step 71 4.145911681s a59817af08a524354ad5ce5d1747ac38d5d0ce4841162593fdbaf2f22b208637 node-6 appended entry 7\n\
step 72 4.145911681s 86cf2ebe295e7d30f76dc8f7be5f7e9fd16949ebd796eda062cdb48362d5cd8e node-6 committed through 5\n\
step 73 4.146791476s 24f8edcaf4a60dab9e1aa25101da8229fb161b34eb07c24fea3cbf996cf7275e node-3 appended entries 6 to 7\n\
step 74 4.157736382s 8a9b6cf0fb6d95c06d9798058f045324ec3e41457bef663ca7e5b49ec0a5034e node-3 committed through 5\n\
step 75 4.167022419s f5243eccc143485a257aa0e59dc466c02993531f233fc94563a0cc5c0b8ced19 node-6 committed through 6\n\
step 76 4.172550218s 37f4f954784635312a179f8832e9c65efdbd000c2d35be6f67fb66a88cbfa1c0 node-3 committed through 6\n\
step 77 4.173701332s 28966ed96ef6efcac9801d2e229de86bfc7fb9e5041bbe19aa2b0181f19021cb node-5 committed through 7\n\
step 78 4.178252558s b66019794c82212c071c4158c89ff9b02b60684db4147f038eb36ef21d878ddc client-0 heard command 4 is done\n\
step 79 4.193514383s bdd3e1bf9cf1a44db671103933276ba7cb18883754d3ee35105262a484d35728 client-1 heard command 5 is done\n\
step 80 4.199120384s 445d1c469b372cd6015b4a5095bc978e6fd72904af49c2191af5a0b2a6d90bba node-4 committed through 6\n\
step 81 4.205889555s bf5c2ee9ace8881bafde14cfb0f9077b9e8623c9631e67f5555f823b7d3cb79c node-7 committed through 6\n\
step 82 4.258134366s de5b510fa07413d0733dfe70385b317bc85a0479b47857fccdee29bf5bb529ab client-2 heard command 6 is done\n\
step 83 4.707128832s 5a37ced7e4e9615b4c532ae6df423cf2589bc7f1fea6864f70fad1f7cd67bf5d node-4 committed through 7\n\
step 84 4.711339706s 592341e74f8eab787b842e8aa3864e38528cd4936b9344db873016c03c9658df node-6 committed through 7\n\
step 85 4.729018705s 3e11dd858ab025922936d1639428f989b9524c8bd30b41989814b5ebbcece3d7 node-7 committed through 7\n\
step 86 4.750118505s 00ee9d32f399ffe69afa03ac06f9a1db40eee3d973eae95a3559c850eb26b556 node-3 committed through 7\n\
step 87 6.000000000s f66d1083bf4d0c59fd458e1fc926b7710ffd4b8ea5e839678692d10f53123677 client-0 sent command 7\n\
step 88 6.000000000s bbbda651c541505c3a9c59e02967ebd27124ad0632234a90eb01e756844773b4 client-1 sent command 8\n\
step 89 6.000000000s 86ade3784195ba2d96f9d5d067197bc1f671dec9af5e2f0d2feeae2667dd4ada client-2 sent command 9\n\
step 90 6.039828945s c75bb228c68309e99f7829a5c59dda6d0a695627cfc2e380cbe02039c3baae32 node-5 took command 7 as entry 8\n\
step 91 6.050826838s 890cc3693643429a971c6506fafc2f73e41db8971494edaa57de5f209a3fbf04 node-6 appended entry 8\n\
step 92 6.052488904s 6c14c1132e3a85376d56b4ec41e853cac911656042fd132cdb90b3035bc298cd node-7 appended entry 8\n\
step 93 6.065031313s e8f36c2f34bd4fdca69d2953a5d283b8725277b734fbd1ecc95e552577cb3e70 node-4 appended entry 8\n\
step 94 6.073857105s a69e421475794a87ae5b2c142e38c8a2bd4aa0d2ce3427b77002b268089db995 node-3 appended entry 8\n\
step 95 6.077040353s 5a3b858fb0814b4a348c822d534f8861a060a379d28045f66cf72b3c4784a0f9 node-5 took command 9 as entry 9\n\
step 96 6.083566503s a84e060775e676a0a1c28426e9872fd6000e4e709e91dc5b148813ce4b764829 node-5 took command 8 as entry 10\n\
step 97 6.097957453s 3674b3c72b2dd490b968d579cad3961a9c9a3bbe6679c77e05859a51891ac90f node-4 appended entries 9 to 10\n\
step 98 6.099163056s e905fa29dde469237ac793e2e33571d4f4168dace156c926a954efa6c53a798a node-6 appended entries 9 to 10\n\
step 99 6.130357240s 837a0540271b6ac8c7ec24ac638f174e6aa4512e6661c03edf7b78e91831a23c node-3 appended entries 9 to 10\n\
step 100 6.135541307s 90a9ce8294f6c41109be8a5c4d99a4a6cc32b07490855f3d0ec02fb4962c7b65 node-5 committed through 8\n\
step 101 6.147503014s 3a1541d18a84edfb0c7510f7c311bf27dbd283621dfe090d8a842549c3e0900a node-7 appended entry 9\n\
step 102 6.154265849s f67e748e3c7635074cd51ba396824050e3e998ff71a256418c26205014dae1da node-7 appended entry 10\n\
step 103 6.163268245s c54cea121a919a822d66cc37c4848f010108a6bac8c9f59bf250566497b13fd3 node-5 committed through 10\n\
step 104 6.174444513s 425a739ab82e6b6747079b7544c8c8ab79a9ddc2ed1173400f563b5b8feae778 node-7 committed through 8\n\
step 105 6.183464164s 24a5dfeb00442cb102948126e6115a6ffef2ea84ff4ddb4be325b511d34847b5 node-6 committed through 8\n\
step 106 6.195461086s fe1e70e246080ea5b9b96ce5900cb787fd0805c5ceeb84b01fd5873ab99da57b client-0 heard command 7 is done\n\
step 107 6.195495355s b03b40287f9f2e1e2f0a594ea0b300fe75e3e0a3b7874f33949bb53df7b10a5d client-2 heard command 9 is done\n\
step 108 6.197673238s 95ed29d0fb927d2002dd9199377babc1797f3a9b5b582d38bab8ed99e2c368cc client-1 heard command 8 is done\n\
step 109 6.210583068s f8c22daec3afd6948b9f067569638a98f8b2649c54ee278f57cbe1383858c0c2 node-3 committed through 8\n\
step 110 6.227852046s 88a36a426f3e432edd38fc89f8b4a950c98a61eec31d9cdc6270fb5a7cebaf1e node-4 committed through 8\n\
step 111 6.685659631s 3e28de167a4f88f3ee292edb803956813701e8ec8e18fdb96c88b3d4b62dc397 node-6 committed through 10\n\
step 112 6.694083475s ecb33e62c52900ba3c828a37fd961b58c31fe54d4967006548fd701c46bd5436 node-7 committed through 10\n\
step 113 6.705077614s 6a299cc012979710787234a8ae6fd6bc03fb70cc20c78748590da90ae6798016 node-4 committed through 10\n\
step 114 6.708227058s bb53ca4343f3e92e1bf1df3b3d4aad8b275b6278943ca77913d8311c4f5f42a4 node-3 committed through 10\n\
step 115 8.000000000s 1e7715be20821560f2df9b22103aa6cbfe850f6191832bbb71d8a0a573f933c8 client-0 sent command 10\n\
step 116 8.000000000s 153ed6819c18d9974ad9d852e2634cf0fb5c7dde7411281e2957f45d1f0747a3 client-2 sent command 12\n\
step 117 8.000000000s d01855968b68e2ada54184eed7279f2adcba720e32c24c9fec5520cc282eccd5 client-1 sent command 11\n\
step 118 8.027628507s a40b8a4ba69313c315cf608aaa1ed591bc883e84e7d563874de0ae519680202c node-5 took command 12 as entry 11\n\
step 119 8.044403319s 3107c31413de99cc4ba296c15753c2f05ffb440bf6e1adcd392bf3cf93e689c6 node-6 appended entry 11\n\
step 120 8.053222964s 55ca0772c5c1064d55742090049d53bbd8405713a4cf603e52c8247b485f8048 node-5 took command 10 as entry 12\n\
step 121 8.053264899s 91d37e43f4359dde10dca821bce78b5a3d88e3ed5e6ca1805807cc581bb50dc6 node-4 appended entry 11\n\
step 122 8.085819264s 32b11e62bfb4828eeae15df9e5f2ae4e131ca8cb6a4aacdace5d341ea21ce485 node-5 took command 11 as entry 13\n\
step 123 8.098614855s 63b0b7ddc49ee459417dd956585ae0bc0a5a4239bae050e7378bef921ac99b1a node-4 appended entry 12\n\
step 124 8.099527575s a616c0c9cfe30d169b2ffb1ea4264dd22f064d70e2617a74c6ff730514309611 node-3 appended entries 11 to 13\n\
step 125 8.120456732s b41f9eeb756b8f29bb70d8e000875f1f423049c02dbdd52e6078e71602b789dd node-6 appended entries 12 to 13\n\
step 126 8.126538719s 94e04999782561a3a7a05da77e3ff5790cea51e6d301b164d6cc9341ceb3d81a node-7 appended entry 11\n\
step 127 8.135010673s 59556e909a00d156f88b0bc70c87ee80f471e2e2c7cd7f25f79433e0a8622e11 node-5 committed through 11\n\
step 128 8.137319927s aadb7ed99313de42fecb1d38c646a18dad0648d36b44b93a0f804e0f076d6910 node-4 appended entry 13\n\
step 129 8.144738455s 701d6bf38a1892dba65768c06d8455e03a6b14076ba12dc533975e2a24e62215 node-7 appended entries 12 to 13\n\
step 130 8.166193370s c73ce644d2883f34a6808b7ad0c66fef5a83488b6bca7012ebe7d5b3654dcda7 node-3 committed through 11\n\
step 131 8.181110102s a772a30c6b26b236f3f8a13cc598a07df8b120611909a19a4ec0d6eb672d47ef node-5 committed through 13\n\
step 132 8.203486477s 121a3103060b981fa2ea5498323208f2ef345aa33cbd563520f398c211732cf9 node-6 committed through 11\n\
step 133 8.206373775s b5535e5e411ec46ed8aaf1951e5aea324fcc4a44f7368fd2c4733c7fbc1f3b6b client-2 heard command 12 is done\n\
step 134 8.208399577s 54f45dd4de1924c72720819d2cd5349b183656c73bbcb94396b2b0feaab0f173 node-4 committed through 11\n\
step 135 8.217225199s 18d562a752ee0a5daa85384326411713a29f4372a934611a3b124d63900a550a node-4 committed through 13\n\
step 136 8.219155645s eeab36d2d4fa4c0d25037e3be92440a5d069d56f76b29bca7aa922f73c464ec1 node-7 committed through 11\n\
step 137 8.224173803s 1dd6bfa885a92bc5eecfa1f5042a96fc6385c6a2f874d783a4cf048ba17b9918 client-1 heard command 11 is done\n\
step 138 8.228735571s a01525d194454720ee6ead3556ace2160220f4215dcf7e031efdba8d76125b58 client-0 heard command 10 is done\n\
step 139 8.275844382s 3f91c8f09a53302b07eb29e89da659136cc198db898bd4d9053f7849e832f2b4 node-7 committed through 13\n\
step 140 8.685751357s 701a7667f6be929d49195d9f9d91fc7472e4b3f17056caa1b727a8fe5f42798f node-6 committed through 13\n\
step 141 8.708963215s 63920150c811891ddfee75b00dc4d3d4d4d60d016eed805d33db3bb7fb3c8e77 node-3 committed through 13\n\
step 142 10.000000000s 1a98c013f6c4be54c8ace2b066a5d9d1b353a3122d92efa2ee473066456de0b1 client-2 sent command 15\n\
step 143 10.000000000s cfdb48a9f9fdcb3d122091da9a7bf581b048386a8a621d1a8f4d94c388efcde4 client-1 sent command 14\n\
step 144 10.000000000s 5d33a9177f2e18b75330f8707cff8a78e293bb2c21beacc274eaa1f67798f1b6 client-0 sent command 13\n\
step 145 10.024332961s 328f33efe1b457ed93096eddbcc8f61fb6e4c36d1e73ddba871ebc32df4dd1da node-5 took command 14 as entry 14\n\
step 146 10.050187193s fa82897ff7292b2d9e673035ad4caea547661689ae2fcd7a9180c502a4052759 node-5 took command 13 as entry 15\n\
step 147 10.051650420s f902fb6be97fabc5ba50fc246a5f3390205a1686d76f5aa6d872df5ddc25734a node-6 appended entry 14\n\
step 148 10.052773140s bb8d5b2381e5a4de34f21f12d18c7fa20567c25f739b56df200467ea7178a079 node-7 appended entry 14\n\
step 149 10.071721161s 04759f91cefb4e79101a316270befec70ea0a6f38651c2a816a99ac2966c2b67 node-5 took command 15 as entry 16\n\
step 150 10.090595293s e0671ecf463f07c4b08af69213ca8541463aab8b18febd212d7197ce2a3f3558 node-4 appended entries 14 to 15\n\
step 151 10.096279727s 6495e1dbb7ec9134fb03c22b9c64951998c856388211ad36317047b2b675d3a6 node-3 appended entries 14 to 16\n\
step 152 10.098607831s 40e98edf67b42fddea2c7023b614c62fa2d5a670901be4e7d683191bd101f38a node-7 appended entry 15\n\
step 153 10.113430892s 7c4a3bddf06fadbf384813a6bd8eae57201ce92b5d7cb2e601e408a9d24f38d7 node-7 appended entry 16\n\
step 154 10.118033251s 08a631f228b44f7e0aea35211fb3f8d76e8c5cb36f0fcc2e8c61cc871ff542f7 node-4 appended entry 16\n\
step 155 10.128127790s b8bd298eb59c4acb7a0364225507b33295dbf0c5d8f44c41596a95ab7e49be1a node-5 committed through 14\n\
step 156 10.142014821s 11049f68b115ea4f169da9dfaf05aaa781060a91fc6eb3f89fa002f25315d17f node-6 appended entry 15\n\
step 157 10.150550990s cc1579c51f5a5ce14f2c0aa0a8c1c7c5049101dfb001068653c09d4603392a79 node-5 committed through 15\n\
step 158 10.163892607s 7b5cb845baa09d980cbdbdf2105254d32e4d961b444b1db39e084d985ef0ad42 node-6 appended entry 16\n\
step 159 10.177421928s 834aa6f77593d545cbb7ba079eddbfe74950654083f92ddb30f93d3b567e7bf2 node-4 committed through 15\n\
step 160 10.191153341s 57eb39e8a2f24748919dad036a22273d5c1a60654e0403f3f32eb03cbb272f43 node-7 committed through 15\n\
step 161 10.194286146s 4f4eba3ff21d0bb04c1b6ffd40ae64533b6cb4d48c693f52350d18446d184cdf node-5 committed through 16\n\
step 162 10.196519406s 2096bea5b7f7915ce33e29a4e20fe854ea72831279b6ea3b887525801ca67bd8 node-3 committed through 15\n\
step 163 10.221017882s 670328c6df8fca51b40401fc30a97852ab4782fab41ab6caf448ba9fa2c07dae client-1 heard command 14 is done\n\
step 164 10.239005536s 66e29f25569424acdddd64e5fb1323ce8992e985af9f6708f0d9999e281a4ac1 node-6 committed through 15\n\
step 165 10.242716340s cdb5e8c9dd25fd8e3639c2af4058b025ac0eab092078f85137ca33db5006e9d6 client-0 heard command 13 is done\n\
step 166 10.289060803s 6024318ef236bbe9723fdb94549706afe5bb089a4f74aac7bc7cb4583de27cbe client-2 heard command 15 is done\n\
step 167 10.666682905s 43ed142a615bbb4e77f8fac06097229dadeabd098b8830ee9b05516e10690c00 node-6 committed through 16\n\
step 168 10.677704882s 3fa9490c31b93db2ec516b9b91fd6d6b3c89220820ef9b8c047d130a3b5e4d3a node-7 committed through 16\n\
step 169 10.707405703s 1aa6d2fce5bc5d19351e51a43f6b157a352ce6b84f17ba377e4e9390712f7638 node-4 committed through 16\n\
step 170 10.710792188s 8b204b289e419c0a8fd05115f00d2d18b1fca0286691cfc3bc353417c08340d1 node-3 committed through 16\n";

/// A schedule, read back from its text, failing the test if it is not one.
fn faults(text: &str) -> FaultSchedule {
    text.parse()
        .unwrap_or_else(|e| panic!("a test schedule is a schedule: {e}"))
}

/// Runs the system, failing the test rather than returning an error no case expects.
fn runs(seed: u64, faults: &FaultSchedule) -> (Trace, Outcome) {
    let (trace, _, outcome) =
        replog::run(seed, faults).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
    (trace, outcome)
}

/// The messages of the steps at or after the heal, in order.
fn after_the_heal(trace: &Trace) -> Vec<&str> {
    trace
        .steps()
        .iter()
        .filter(|step| step.event().at().as_nanos() >= HEALED)
        .map(|step| step.event().message())
        .collect()
}

/// The replica named at the start of the first message in `messages` that says `what`.
fn first_to<'a>(messages: &[&'a str], what: &str) -> Option<&'a str> {
    messages
        .iter()
        .find_map(|message| message.split_once(what).map(|(who, _)| who))
}

#[test]
fn a_recorded_run_is_the_run_this_seed_produces() {
    // Compared through the written form: both sides name the same seed, so the header cannot hide a
    // difference in the steps.
    let (trace, outcome) = runs(SEED, &FaultSchedule::default());
    assert_eq!(trace.to_string(), RECORDED);
    assert_eq!(outcome, Outcome::Pass);
}

/// What a run's steps say about each command: the entry a leader last took it as, when a client
/// sent it, and when that client heard it was done.
#[derive(Debug, Default)]
struct Said {
    entry: Option<u64>,
    sent: Option<VirtualTime>,
    done: Option<VirtualTime>,
}

/// Every command's [`Said`], read off the messages of `trace`'s steps.
fn said(trace: &Trace) -> BTreeMap<u64, Said> {
    let mut said: BTreeMap<u64, Said> = BTreeMap::new();
    for step in trace.steps() {
        let (at, message) = (step.event().at(), step.event().message());
        let Some((_, rest)) = message.split_once(' ') else {
            continue;
        };
        let number = |text: &str| text.parse::<u64>().ok();
        if let Some((command, entry)) = rest
            .strip_prefix("took command ")
            .and_then(|rest| rest.split_once(" as entry "))
            .and_then(|(command, entry)| Some((number(command)?, number(entry)?)))
        {
            said.entry(command).or_default().entry = Some(entry);
        } else if let Some(command) = rest.strip_prefix("sent command ").and_then(number) {
            said.entry(command).or_default().sent = Some(at);
        } else if let Some(command) = rest
            .strip_prefix("heard command ")
            .and_then(|rest| rest.strip_suffix(" is done"))
            .and_then(number)
        {
            said.entry(command).or_default().done = Some(at);
        }
    }
    said
}

#[test]
fn a_run_commits_its_clients_commands_in_an_order_each_of_them_could_have_seen() {
    // Reached by a different route from the module's check: the entry each command became and
    // the instants each was sent and answered, read off what the steps say rather than off the
    // world they recorded. Both runs keep every committed entry, so the entry a leader last took a
    // command as is where it stays.
    let cases = [
        ("no faults", SEED, FaultSchedule::default()),
        ("the isolation", SEED, faults(ISOLATED)),
    ];
    for (name, seed, schedule) in cases {
        let (trace, outcome) = runs(seed, &schedule);
        assert_eq!(outcome, Outcome::Pass, "{name}");
        let said = said(&trace);
        assert_eq!(said.len(), 15, "{name}: every command was sent");

        let mut entries: Vec<u64> = said.values().filter_map(|command| command.entry).collect();
        let taken = entries.len();
        entries.sort_unstable();
        entries.dedup();
        assert_eq!(
            entries.len(),
            taken,
            "{name}: no two commands share an entry"
        );

        for (earlier, before) in &said {
            let (Some(done), Some(at)) = (before.done, before.entry) else {
                panic!("{name}: command {earlier} was answered and taken: {before:?}");
            };
            for (later, after) in &said {
                if after.sent.is_some_and(|sent| done <= sent) {
                    assert!(
                        after.entry.is_some_and(|entry| at < entry),
                        "{name}: command {earlier} was done before command {later} was sent, \
                         and lands after it: {before:?} {after:?}"
                    );
                }
            }
        }
    }

    // And the log was free to choose, and chose otherwise than the numbering: a check that every
    // command lands in the order it is numbered would fail this run.
    let said = said(&runs(SEED, &FaultSchedule::default()).0);
    assert!(
        said[&2].entry < said[&1].entry,
        "command 2 overtook command 1: {said:?}"
    );
}

/// Node 5, which leads the first term of [`SEED`]'s run, cut off from the other replicas both ways
/// from three seconds to nine — and **not** from the clients, which go on reaching it and asking it
/// to take their commands.
const DEPOSED: &str = "chronoloop faults\n\
                       partition on node 5 -> node 3 from 3.000000000s until 9.000000000s\n\
                       partition on node 3 -> node 5 from 3.000000000s until 9.000000000s\n\
                       partition on node 5 -> node 4 from 3.000000000s until 9.000000000s\n\
                       partition on node 4 -> node 5 from 3.000000000s until 9.000000000s\n\
                       partition on node 5 -> node 6 from 3.000000000s until 9.000000000s\n\
                       partition on node 6 -> node 5 from 3.000000000s until 9.000000000s\n\
                       partition on node 5 -> node 7 from 3.000000000s until 9.000000000s\n\
                       partition on node 7 -> node 5 from 3.000000000s until 9.000000000s\n";

/// [`DEPOSED`], with the clients reaching **only** node 5 until fifteen seconds; and from the heal
/// until then, node 3 cut off from every replica and nodes 4, 6 and 7 from each other. Node 3 is
/// the replica leading when the heal comes in [`SEED`]'s run under it, found by running and
/// asserted by its case.
///
/// So node 5 goes on taking every command the clients send while it cannot commit one — a log
/// longer than anyone else's, ending in term 1 — while the replicas that left it behind hold only
/// what the later terms began with. After the heal none of nodes 4, 6 and 7 hears a leader, none
/// can reach a majority but through node 5, and node 5 can reach all three: when it asks whether
/// it could stand, the one thing between it and the lead is the rule for whose log is the more up
/// to date. Length alone would pick it.
const STRANDED: &str = "chronoloop faults\n\
                        partition on node 5 -> node 3 from 3.000000000s until 9.000000000s\n\
                        partition on node 3 -> node 5 from 3.000000000s until 9.000000000s\n\
                        partition on node 5 -> node 4 from 3.000000000s until 9.000000000s\n\
                        partition on node 4 -> node 5 from 3.000000000s until 9.000000000s\n\
                        partition on node 5 -> node 6 from 3.000000000s until 9.000000000s\n\
                        partition on node 6 -> node 5 from 3.000000000s until 9.000000000s\n\
                        partition on node 5 -> node 7 from 3.000000000s until 9.000000000s\n\
                        partition on node 7 -> node 5 from 3.000000000s until 9.000000000s\n\
                        partition on node 0 -> node 3 from 3.000000000s until 15.000000000s\n\
                        partition on node 3 -> node 0 from 3.000000000s until 15.000000000s\n\
                        partition on node 1 -> node 3 from 3.000000000s until 15.000000000s\n\
                        partition on node 3 -> node 1 from 3.000000000s until 15.000000000s\n\
                        partition on node 2 -> node 3 from 3.000000000s until 15.000000000s\n\
                        partition on node 3 -> node 2 from 3.000000000s until 15.000000000s\n\
                        partition on node 0 -> node 4 from 3.000000000s until 15.000000000s\n\
                        partition on node 4 -> node 0 from 3.000000000s until 15.000000000s\n\
                        partition on node 1 -> node 4 from 3.000000000s until 15.000000000s\n\
                        partition on node 4 -> node 1 from 3.000000000s until 15.000000000s\n\
                        partition on node 2 -> node 4 from 3.000000000s until 15.000000000s\n\
                        partition on node 4 -> node 2 from 3.000000000s until 15.000000000s\n\
                        partition on node 0 -> node 6 from 3.000000000s until 15.000000000s\n\
                        partition on node 6 -> node 0 from 3.000000000s until 15.000000000s\n\
                        partition on node 1 -> node 6 from 3.000000000s until 15.000000000s\n\
                        partition on node 6 -> node 1 from 3.000000000s until 15.000000000s\n\
                        partition on node 2 -> node 6 from 3.000000000s until 15.000000000s\n\
                        partition on node 6 -> node 2 from 3.000000000s until 15.000000000s\n\
                        partition on node 0 -> node 7 from 3.000000000s until 15.000000000s\n\
                        partition on node 7 -> node 0 from 3.000000000s until 15.000000000s\n\
                        partition on node 1 -> node 7 from 3.000000000s until 15.000000000s\n\
                        partition on node 7 -> node 1 from 3.000000000s until 15.000000000s\n\
                        partition on node 2 -> node 7 from 3.000000000s until 15.000000000s\n\
                        partition on node 7 -> node 2 from 3.000000000s until 15.000000000s\n\
                        partition on node 3 -> node 4 from 9.000000000s until 15.000000000s\n\
                        partition on node 4 -> node 3 from 9.000000000s until 15.000000000s\n\
                        partition on node 3 -> node 5 from 9.000000000s until 15.000000000s\n\
                        partition on node 5 -> node 3 from 9.000000000s until 15.000000000s\n\
                        partition on node 3 -> node 6 from 9.000000000s until 15.000000000s\n\
                        partition on node 6 -> node 3 from 9.000000000s until 15.000000000s\n\
                        partition on node 3 -> node 7 from 9.000000000s until 15.000000000s\n\
                        partition on node 7 -> node 3 from 9.000000000s until 15.000000000s\n\
                        partition on node 4 -> node 6 from 9.000000000s until 15.000000000s\n\
                        partition on node 6 -> node 4 from 9.000000000s until 15.000000000s\n\
                        partition on node 4 -> node 7 from 9.000000000s until 15.000000000s\n\
                        partition on node 7 -> node 4 from 9.000000000s until 15.000000000s\n\
                        partition on node 6 -> node 7 from 9.000000000s until 15.000000000s\n\
                        partition on node 7 -> node 6 from 9.000000000s until 15.000000000s\n";

/// Node 7 able to send to every other node and to hear from none, from one second to twenty: one
/// direction of every link it has, the other left open.
///
/// It never hears a leader, so its timer runs out again and again, and everything it asks reaches
/// every replica. It is back with ten seconds of the run to go, which is what the last command
/// needs to reach it.
const SEND_ONLY: &str = "chronoloop faults\n\
                         partition on node 0 -> node 7 from 1.000000000s until 20.000000000s\n\
                         partition on node 1 -> node 7 from 1.000000000s until 20.000000000s\n\
                         partition on node 2 -> node 7 from 1.000000000s until 20.000000000s\n\
                         partition on node 3 -> node 7 from 1.000000000s until 20.000000000s\n\
                         partition on node 4 -> node 7 from 1.000000000s until 20.000000000s\n\
                         partition on node 5 -> node 7 from 1.000000000s until 20.000000000s\n\
                         partition on node 6 -> node 7 from 1.000000000s until 20.000000000s\n";

/// How many whole seconds [`deposing`] keeps a leader from its followers.
const DEPOSED_FOR: u64 = 6;

/// One second, in the nanoseconds a step's instant is read in.
const SECOND: u64 = 1_000_000_000;

/// The replicas `trace` says became leader at an instant within `during`, in nanoseconds, each with
/// the term it led, in the order they did.
fn elected(trace: &Trace, during: Range<u64>) -> impl Iterator<Item = (&str, u64)> {
    trace
        .steps()
        .iter()
        .filter(move |step| during.contains(&step.event().at().as_nanos()))
        .filter_map(|step| step.event().message().split_once(" became leader of term "))
        .filter_map(|(who, term)| Some((who, term.parse().ok()?)))
}

/// The replica `trace` says leads at `at`, in nanoseconds: the one named by the last step before
/// then that says a replica became leader. Read off what the steps *say*, so it does not see a
/// leader step down — which is why [`first_deposable`] reads the world instead.
fn leading(trace: &Trace, at: u64) -> Option<&str> {
    elected(trace, 0..at).last().map(|(who, _)| who)
}

/// The replica leading at `at`, in nanoseconds, in a finished run, as its node number.
///
/// Read off the world the last step before `at` recorded rather than off what the steps say: the
/// replica whose role is leader — the one in the latest term, should a leader deposed not have
/// heard so yet — and nobody if no replica is leading, as when a leader has stepped down and the
/// election after it is not won. No seed of the sweep's five hundred has that happen before its
/// cut, so nothing here reaches that last branch; what the world buys today is a route to the
/// leader the steps' own account does not take.
fn leader_at(trace: &Trace, store: &StateStore, at: u64) -> Option<u64> {
    let step = trace
        .steps()
        .iter()
        .rfind(|step| step.event().at().as_nanos() < at)?;
    let node = store
        .get(step.state())
        .unwrap_or_else(|| panic!("the store holds every state the trace names"));
    let world = World::try_from(&node).unwrap_or_else(|e| panic!("a recorded world: {e}"));
    let (role, term) = (name("role"), name("term"));
    world
        .resources()
        .filter(|(_, replica)| replica.get(&role) == Some(&Value::Text("leader".into())))
        .filter_map(|(who, replica)| match replica.get(&term) {
            Some(Value::Count(term)) => Some((*term, who)),
            _ => None,
        })
        .max()
        .and_then(|(_, who)| who.as_str().strip_prefix("node-")?.parse().ok())
}

/// A name, failing the test if it is not one.
fn name(text: &str) -> Name {
    Name::new(text).unwrap_or_else(|e| panic!("{text} is a name: {e}"))
}

/// The replica leading a run with nothing in its way, as its node number, and the first whole
/// second from three on at which one leads: whom [`deposing`] cuts off for that seed, and from
/// when.
///
/// Read off the run with nothing in its way because it is the run with the cut, up to the instant
/// the cut begins: a fault decides nothing about a message sent before it is in force, and a fault
/// in force still draws against its odds. Three seconds, as [`DEPOSED`] has it, unless no replica
/// leads then, which a split vote can see to.
fn first_deposable(trace: &Trace, store: &StateStore) -> (u64, u64) {
    (3..10)
        .find_map(|second| Some((leader_at(trace, store, second * SECOND)?, second)))
        .unwrap_or_else(|| {
            panic!(
                "seed {} has a leader before ten seconds: {trace}",
                trace.seed()
            )
        })
}

/// [`first_deposable`] for `seed`, running it with nothing in its way.
fn deposable(seed: u64) -> (u64, u64) {
    let (trace, store, _) = replog::run(seed, &FaultSchedule::default())
        .unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
    first_deposable(&trace, &store)
}

/// [`DEPOSED`]'s shape around `node`: cut off from every other replica both ways for
/// [`DEPOSED_FOR`] seconds from `from` seconds on, and not from the clients.
fn deposing(node: u64, from: u64) -> FaultSchedule {
    let until = from + DEPOSED_FOR;
    let lines: Vec<String> = (3..8)
        .filter(|other| *other != node)
        .flat_map(|other| [(node, other), (other, node)])
        .map(|(a, b)| {
            format!(
                "partition on node {a} -> node {b} from {from}.000000000s until {until}.000000000s"
            )
        })
        .collect();
    faults(&format!("chronoloop faults\n{}\n", lines.join("\n")))
}

#[test]
fn the_deposed_leader_of_the_seed_run_here_is_the_one_every_seed_is_swept_under() {
    // The constant is what the single-seed case reads, and the derivation is what the sweep runs
    // under. They are one schedule only while node 5 is the replica leading at three seconds, which
    // is a fact about the timings and not about the schedule — so it is asserted, and a change that
    // moves the first election turns this red rather than turning the single-seed case into a
    // follower cut off.
    assert_eq!(deposable(SEED), (5, 3));
    assert_eq!(deposing(5, 3), faults(DEPOSED));
}

#[test]
fn each_seed_is_deposed_of_the_leader_it_actually_elected() {
    // The sweep under the deposed leader cannot tell a leader cut off from a follower cut off — the
    // system holds up either way — so this is what holds the derivation to account, by a second
    // route: the derivation reads the role each replica's world records in the run with nothing in
    // its way, and this reads what the steps of the run *with* the cut say — who last became leader
    // before it began, and who led while it lasted. Seeds 0, 2, 3, 8 and 26 put the first leader on
    // nodes 6, 7, 5, 4 and 3, and on seed 8 nobody leads until four seconds — chosen by running, and
    // asserted below, since a seed chosen for its timings is a hypothesis about the next version of
    // them. The gated sweep runs the derivation on five hundred seeds, holding only the verdicts.
    let mut deposed = BTreeSet::new();
    let mut late = false;
    for seed in [0, 2, 3, 8, 26] {
        let (node, from) = deposable(seed);
        deposed.insert(node);
        late |= from > 3;
        let (trace, outcome) = runs(seed, &deposing(node, from));
        let cut = from * SECOND;
        let name = format!("node-{node}");
        assert_eq!(
            leading(&trace, cut),
            Some(name.as_str()),
            "seed {seed}: the replica cut off was leading when the cut began"
        );
        assert!(
            elected(&trace, cut..cut + DEPOSED_FOR * SECOND).any(|(who, _)| who != name),
            "seed {seed}: another replica led while {name} was away"
        );
        assert_eq!(outcome, Outcome::Pass, "seed {seed}");
    }
    assert_eq!(
        deposed,
        BTreeSet::from([3, 4, 5, 6, 7]),
        "every replica was deposed on some seed"
    );
    assert!(late, "and one seed had nobody leading at three seconds");
}

#[test]
fn a_leader_its_clients_reach_and_its_followers_do_not_tells_them_nothing_it_has_not_committed() {
    // The run the clients' side of the verdict is for. Node 5 goes on believing it leads, takes
    // what the clients send it, and can commit none of it; a leader that told a client "done" on
    // taking a command would have the client move on, and the next round land in the log ahead of
    // a command that never does. The replicas' own checks see nothing wrong with that, since
    // nothing node 5 applied is ever given up — only the clients can tell. Chosen by running that
    // mutation: it fails here with "not linearizable", and the run itself holds up.
    let (trace, outcome) = runs(SEED, &faults(DEPOSED));
    let cut_off: Vec<&str> = trace
        .steps()
        .iter()
        .filter(|step| (3_100_000_000..HEALED).contains(&step.event().at().as_nanos()))
        .map(|step| step.event().message())
        .filter(|message| message.starts_with("node-5 took command "))
        .collect();

    assert!(
        !cut_off.is_empty(),
        "the clients reached node 5 while it was cut off"
    );
    assert!(
        trace
            .steps()
            .iter()
            .any(|step| step.event().message().contains(" became leader of term 2")),
        "another replica led while node 5 was away"
    );
    assert_eq!(outcome, Outcome::Pass, "{trace}");
}

#[test]
fn a_replica_back_from_a_partition_alone_asks_to_stand_is_refused_and_deposes_nobody() {
    // Under the vote in `2ff3f46`, which asked nothing of the candidate's log, node 7 came back
    // from here with a term every replica had to take, won, and four committed entries went with
    // it. Under the vote that asked for the log and nothing before it, it still came back with that
    // term and deposed the leader, only to be refused. Now nobody answers while it is away, so its
    // term never moves, and the replicas it asks on its return are following a leader.
    let (trace, outcome) = runs(SEED, &faults(ISOLATED));
    let messages: Vec<&str> = trace
        .steps()
        .iter()
        .map(|step| step.event().message())
        .collect();
    assert!(
        !messages
            .iter()
            .any(|message| message.starts_with("node-7 became candidate")),
        "node 7 never stood, away or back: {trace}"
    );
    let healed = after_the_heal(&trace);
    assert!(
        healed
            .iter()
            .any(|message| message.starts_with("node-7 asked whether it could stand for term ")),
        "though it asked after the heal: {healed:?}"
    );
    assert_eq!(
        first_to(&healed, " became candidate for term "),
        None,
        "and nobody stood after the heal, so the leader kept its place: {healed:?}"
    );
    assert_eq!(outcome, Outcome::Pass, "{trace}");
}

#[test]
fn a_replica_that_can_send_and_cannot_hear_deposes_nobody() {
    // The asymmetric failure the vote alone could not survive: a replica that hears no leader asks
    // to stand every time its timer runs out, and every replica it asks would have taken the term
    // it asked in. Asking first and standing only on a majority's word is what keeps one deaf
    // replica from deposing every leader for as long as it stays deaf — it never hears the answers.
    let (trace, outcome) = runs(SEED, &faults(SEND_ONLY));
    let asked = trace
        .steps()
        .iter()
        .filter(|step| step.event().at().as_nanos() < 20_000_000_000)
        .filter(|step| {
            step.event()
                .message()
                .starts_with("node-7 asked whether it could stand")
        })
        .count();
    assert!(asked > 1, "node 7 asked again and again: {trace}");
    let stood: Vec<&str> = trace
        .steps()
        .iter()
        .map(|step| step.event().message())
        .filter(|message| message.contains(" became candidate for term "))
        .collect();
    assert_eq!(
        stood,
        ["node-5 became candidate for term 1"],
        "one election in the whole run, the first"
    );
    assert_eq!(outcome, Outcome::Pass, "{trace}");
}

#[test]
fn a_deposed_leader_with_a_longer_log_from_an_older_term_asks_and_is_refused() {
    // The vote's other half from the isolation's: node 7 came back holding less than anyone, and
    // node 5 comes back here holding more, from a term since gone by. Both are behind, by the
    // order the vote compares in; only the second is behind by length too little to tell.
    let (trace, outcome) = runs(SEED, &faults(STRANDED));
    let steps: Vec<(u64, &str)> = trace
        .steps()
        .iter()
        .map(|step| (step.event().at().as_nanos(), step.event().message()))
        .collect();
    assert_eq!(
        elected(&trace, 0..HEALED).last(),
        Some(("node-3", 3)),
        "the replica the schedule cuts off is the one leading at the heal"
    );
    let asked = steps
        .iter()
        .find(|(at, message)| {
            *at >= HEALED && message.starts_with("node-5 asked whether it could stand")
        })
        .unwrap_or_else(|| panic!("node 5 asked after the heal: {trace}"));

    // Read off the steps rather than any replica's log: the longest log node 5 took for itself
    // before it asked, against the furthest any other replica had appended by then.
    let before: Vec<&str> = steps
        .iter()
        .filter(|(at, _)| *at < asked.0)
        .map(|(_, message)| *message)
        .collect();
    let took = before
        .iter()
        .filter(|message| message.starts_with("node-5 took command "))
        .filter_map(|message| last_number(message))
        .max();
    let held = before
        .iter()
        .filter(|message| !message.starts_with("node-5 ") && message.contains(" appended entr"))
        .filter_map(|message| last_number(message))
        .max();
    assert!(
        took.zip(held).is_some_and(|(took, held)| took > held),
        "node 5's log was the longer one when it asked: {took:?} against {held:?}"
    );

    assert!(
        !steps
            .iter()
            .any(|(at, message)| *at >= HEALED && message.starts_with("node-5 became candidate")),
        "none of them said it could stand: {trace}"
    );
    assert!(
        steps
            .iter()
            .any(|(at, message)| *at > asked.0
                && message.starts_with("node-5 dropped entries after ")),
        "and what it took and could not commit was cut back by a replica that led instead: {trace}"
    );
    assert_eq!(outcome, Outcome::Pass, "{trace}");
}

/// The number a step's message ends with: the entry a command was taken as, or the last entry a
/// copy appended.
fn last_number(message: &str) -> Option<u64> {
    message.rsplit(' ').next()?.parse().ok()
}

#[test]
#[ignore = "a sweep of two thousand five hundred runs; `make local-validation` runs it in both profiles"]
fn every_invariant_holds_on_every_seed_with_or_without_a_partition() {
    // The before and after on one range: under the vote that asked nothing of the candidate's log,
    // 475 of these seeds lost committed entries under the isolation. A pass is the verdict's word
    // that all four invariants held at every step — one leader a term, no committed entry changed,
    // an applied order every client could have seen, and every command committed everywhere.
    //
    // What it cannot feel: a sweep reporting nothing says only that no run broke a promise, not
    // what any run did, and every seed holding up means a run cut off from its seed holds up too.
    // The pinned trace is what feels the seed, and the cases above on the isolation and the
    // stranded leader are what say a stale replica is still put up for election at all.
    let schedules = [
        ("the isolation", faults(ISOLATED)),
        ("the replica that cannot hear", faults(SEND_ONLY)),
    ];
    for (name, schedule) in schedules {
        let swept = survey(SWEEP, |seed| {
            replog::run(seed, &schedule).map(|(_, _, outcome)| outcome)
        });
        assert_eq!(swept, (SWEEP, Vec::new()), "under {name}");
    }

    // No faults, then each seed's own leader cut off, rather than node 5 on every seed: node 5
    // leads at three seconds on a fifth of these, and on the rest a fixed node 5 cut off a follower
    // while the leader went on. One run is both the verdict with no faults and where the leader is
    // read from, so a seed reported here broke under one schedule or the other — the reason says
    // which invariant, and running the seed with no faults says which schedule. A seed that breaks
    // with no faults is not cut at all: who leads in a run that broke is no derivation to trust.
    let swept = survey(SWEEP, |seed| {
        let (trace, store, outcome) = replog::run(seed, &FaultSchedule::default())?;
        if outcome != Outcome::Pass {
            return Ok(outcome);
        }
        let (node, from) = first_deposable(&trace, &store);
        replog::run(seed, &deposing(node, from)).map(|(_, _, outcome)| outcome)
    });
    assert_eq!(
        swept,
        (SWEEP, Vec::new()),
        "with no faults, and under each seed's leader deposed"
    );
}
