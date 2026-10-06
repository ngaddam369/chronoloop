//! Five replicas keeping a log over the simulated network, with a client sending them commands.
//!
//! The unit tests beside [`chronoloop::systems::replog`] hold the protocol to account one message
//! at a time, on replicas a case builds by hand. This one asks what those cannot: does a **run** —
//! timeouts drawn, messages landing in whatever order the wire gives them — elect a leader and
//! commit every command, and what becomes of the log when one replica spends a partition alone?
//!
//! Schedules are read back from the text a schedule is written in, for the reason
//! `tests/outcome.rs` gives. The client is node 0 and the replicas nodes 1 to 5.
//!
//! What these cases can and cannot feel: the client's commands open on a fixed period, so the seed
//! reaches a run through how long each message spends on the wire and through each replica's
//! election timeouts. The recorded trace feels both. The pair under [`ISOLATED`] feels the seed
//! too, and that is what it was chosen for: under one and the same schedule, seed 0 loses committed
//! entries and seed 70 keeps them, and which one a run does is the draws' doing — whichever
//! replica's timeout runs out first after the heal stands first. A schedule whose failure did not
//! depend on the seed would leave a repro's seed doing nothing, which is the trap the reconciler's
//! first fixtures fell into.
//!
//! What no case here feels is the replicas' vote being granted whatever the candidate's log holds,
//! **as a rule**: the pair shows what it does on two seeds, and the gated sweep says how often under
//! one schedule. That a vote is granted without the log being asked about at all is held by a unit
//! case beside the module, and nowhere else.

use chronoloop::fault::FaultSchedule;
use chronoloop::outcome::{Outcome, Reason};
use chronoloop::systems::replog;
use chronoloop::trace::Trace;

/// The seed the recorded case runs, since it is not about a particular one.
const SEED: u64 = 20_261_006;

/// Node 5 cut off from every other node, both ways, from one second to nine.
///
/// It is gone before any replica's first timeout can run out, so it never leads the first term and
/// misses the first four commands; and it is back with twenty seconds of the run still to go.
const ISOLATED: &str = "chronoloop faults\n\
                        partition on node 5 -> node 0 from 1.000000000s until 9.000000000s\n\
                        partition on node 0 -> node 5 from 1.000000000s until 9.000000000s\n\
                        partition on node 5 -> node 1 from 1.000000000s until 9.000000000s\n\
                        partition on node 1 -> node 5 from 1.000000000s until 9.000000000s\n\
                        partition on node 5 -> node 2 from 1.000000000s until 9.000000000s\n\
                        partition on node 2 -> node 5 from 1.000000000s until 9.000000000s\n\
                        partition on node 5 -> node 3 from 1.000000000s until 9.000000000s\n\
                        partition on node 3 -> node 5 from 1.000000000s until 9.000000000s\n\
                        partition on node 5 -> node 4 from 1.000000000s until 9.000000000s\n\
                        partition on node 4 -> node 5 from 1.000000000s until 9.000000000s\n";

/// The instant [`ISOLATED`] heals at, in nanoseconds.
const HEALED: u64 = 9_000_000_000;

/// The lowest seed that loses committed entries under [`ISOLATED`], found by running.
const BREAKS: u64 = 0;

/// The lowest seed that keeps them under [`ISOLATED`], found by running. Node 5 stands after the
/// heal in this run, and loses: node 2 stood a third of a millisecond later and its requests reached
/// a majority first.
const HOLDS: u64 = 70;

/// The run `SEED` produces with nothing in its way, recorded from an actual run.
///
/// Node 3's timeout runs out first, at 1.96s, and every other replica votes for it; its no-op is
/// entry 1, and each command after it is taken, copied, committed by the leader once three replicas
/// hold it, and answered — the followers hearing of the commit on the next heartbeat, half a second
/// later. The command entries are 2 to 6, and no one stands against node 3 again.
const RECORDED: &str = "chronoloop trace seed 20261006\n\
step 0 0.000000000s 52787b84f80b3128125d356404c1fb2890fcdb04e58ccea85867d55cbbe77566 node-1 started as a follower in term 0\n\
step 1 0.000000000s 6b17f7adf9e65bc6339f3aaa2421f17ccd90469bca152f8e2d95c6cf994bc452 node-2 started as a follower in term 0\n\
step 2 0.000000000s 9eac167c1a6c8ce92c699ac1d778811ab854f3a57423d59b72fd05afc9ad43fe node-3 started as a follower in term 0\n\
step 3 0.000000000s 75e8b3509420a59927f67d0e44539ebb292475b7eb64e6857b890c3b67179bc4 node-4 started as a follower in term 0\n\
step 4 0.000000000s 93008c9fcdfb9000d81991e3ba6d7bfa87d747fb5edaa7a375027842940a21c4 node-5 started as a follower in term 0\n\
step 5 1.964643210s b253ea4d31f58b585c4a86d280ed8f2788fc4702c825f80051b67fe44dfc0238 node-3 became candidate for term 1\n\
step 6 1.990691194s 9fa81523704d1bc92791662c24be8d3c6feb6cab1ad244c382aa5423bd8eac5f node-4 voted for node-3 in term 1\n\
step 7 1.999827301s 9ed69a370badb3d675aed9a867795d8e4e3d75196e617a6396e51580288188cb node-2 voted for node-3 in term 1\n\
step 8 2.000000000s a3ca05a6167a4454b7317c8f1c0c01cc520b7129b0d1bac712e32449f9d497ca client sent command 1\n\
step 9 2.022357527s b6d8ae728a06a908c848e8b3cb6b158ededb7b07b9d2d26dc9fccca0c64b23bb node-1 voted for node-3 in term 1\n\
step 10 2.048376268s 20a2ea3174af5f879e620b99a95921f3db02475e70845a8f4cd0ae468749b8ba node-5 voted for node-3 in term 1\n\
step 11 2.093468792s 0dd88410448df6ec3266ac9dada077fc70f115ea09eb6ec4cd131fa5669b52f6 node-3 became leader of term 1\n\
step 12 2.114491686s 544e71340e809a44881e154957891aeaa243ba20d385336f842c3d97323bf755 node-1 appended entry 1\n\
step 13 2.133201215s 11a22d843b610d9d3786dc6bb167deb318c1e97ae3fca4029daa8901020e90e8 node-2 appended entry 1\n\
step 14 2.139309691s 189253f393b29ae983a2505027857b464f738f4e1cf342391c5274d4da8811f9 node-4 appended entry 1\n\
step 15 2.156088366s 15b454f1b8667c56af35d13bf40564c1990bac1de937c4bc9c60d7ef3943bc8e node-5 appended entry 1\n\
step 16 2.157784417s 1bf092b4458b1f45226ddd78f0aa91a10c62786c5f34da0f2afaa0d16060ab49 node-3 committed through 1\n\
step 17 2.240122107s d6cd20ed5d00ce79277391fbb4b78078709bbd94cdfe9f4792de5a793fa2fac8 node-3 took command 1 as entry 2\n\
step 18 2.280056124s cdd3d4f0a4f2f70d3712803b2c2ac667b6d1bced4a80e5bec829f5db3700f91d node-2 appended entry 2\n\
step 19 2.280056124s 93d75de6da5d8923d8c77cfa595cb677534db334dfb04042df5b58aeaf97dd4e node-2 committed through 1\n\
step 20 2.295171954s 70dd8f24552af946b3fd74a2201ecba3a8695e9ba8ccbfcce42bab887030c46b node-4 appended entry 2\n\
step 21 2.295171954s 9c2aa47d514449dd6baf79f5319db18cd90f876a426b317e91b61ea887a1802c node-4 committed through 1\n\
step 22 2.302390698s e6f2b3dd65d5fbcb14e8cc5e8f7fbb867fc5f92aff0c7929a6102ce0196ac56e node-5 appended entry 2\n\
step 23 2.302390698s a3b5289c1269d0e39b5b86acd1e4a19fbd7b7a491e107d3bd8c6e35c8837dfbd node-5 committed through 1\n\
step 24 2.336459100s 6ec7075634fe047403baedb3ff518c784678766bfe73866fe7877d6ab702e5fc node-1 appended entry 2\n\
step 25 2.336459100s a4324a0af90b15fd5d8b84c460b7d4a44d534d8925e7bc81b7f0c0e4b8c719db node-1 committed through 1\n\
step 26 2.351069321s 1622eb0f9870d0c9d9c3fe8f98661add5bb96206db520ad039c23108048d4d5e node-3 committed through 2\n\
step 27 2.401585561s 75e07a95865c979a517ca53d2dbe96131837ea78677b0d184e9882eff6539e56 client heard command 1 is done\n\
step 28 2.607529621s 262debdb212ea176250a820b59fb06ba668cf30e1bd48d13165f906b5ebd1bf3 node-5 committed through 2\n\
step 29 2.672264686s 8b0c1f87234d6f47ec0318ba6825acace467c11b5f955bf016efc6800a432a22 node-1 committed through 2\n\
step 30 2.682335333s 8a9763376188018733f3dd00565bd3b1e13c7cbb4f8bd14912bc88e162c4fea3 node-2 committed through 2\n\
step 31 2.690625236s 5a7ab4ff22ad0dc556454d876463624c29f59ea2c97233ec40dab449cdc2b8e6 node-4 committed through 2\n\
step 32 4.000000000s 3255aa6156040ed8ea2c7d737701fa23b242704e64106f8ef3e64152ce0aa801 client sent command 2\n\
step 33 4.077774998s 05addada3e3ffcf8b3ba5288050ed565fd4a45822d93af756507dbbd95917431 node-3 took command 2 as entry 3\n\
step 34 4.093752597s 18ed9c5f602faa1ad9e7235fb1043d999b0f7a3708d866866a1f6fdb115fe37e node-4 appended entry 3\n\
step 35 4.108440128s 4fc4c332119d265dfcad5e097b7d3f94478c43d2d1475425419fe3f4000f40c4 node-1 appended entry 3\n\
step 36 4.115987592s 3ee26d664bf80f15f336a0c6c51136d7de12b03775baa7459b83b8aa7f501ecb node-2 appended entry 3\n\
step 37 4.159845750s 196f191d6c9fa23b781b054ef5182d24d3d6cf806081fb96336a3660409359bd node-3 committed through 3\n\
step 38 4.166806544s 861ebb336f03ec09b4f82608a801cfd4dcea4f1cffb87e246bb3b6b25de91886 node-5 appended entry 3\n\
step 39 4.212033613s 4ead7b5fd3cbdbef90b1a36175b76bf7b85bd793ca77c55bbaf5c9b56ec0ac6d client heard command 2 is done\n\
step 40 4.606037240s 2a239df40e0f904b59a0ec5b44a84265f6b3ef7fc91a1e1e8b3006f80892546c node-4 committed through 3\n\
step 41 4.612522262s 2f7ad541a0d60c2505486d5f4dbe16528d3c33b764dd79c5e546fdd762b525ad node-1 committed through 3\n\
step 42 4.639491186s cc01d6845391c069e87ac15feacabcd02a35c768b4132dd91bc507dc0470aac3 node-5 committed through 3\n\
step 43 4.660689402s c3a07c1799316cc4a69907b3aaeb699fac6b07a3271c1d0f0e8007fb08ce667c node-2 committed through 3\n\
step 44 6.000000000s 6ec1d5754b6e2ef0906bc40038da6cf477a5be82060825fa5a0b8bbdf153fa75 client sent command 3\n\
step 45 6.027345412s bbf7c753beb180d1303cddde9d1238e35df15ced32cc57785a42c36b482f6fc4 node-3 took command 3 as entry 4\n\
step 46 6.055133884s e8e9fb85a0306b98c83190c652765d4bf5511d4906983e60a257edb81955236c node-2 appended entry 4\n\
step 47 6.077794273s 6a76a78a109bd1a4d79d3d5bd737455a9d791e30cecfe198552d31fba34ebad9 node-1 appended entry 4\n\
step 48 6.105489144s 2a7242c96558373d16231c96bae6daf397fe411bf37fa005b97a58ab17f6fffa node-5 appended entry 4\n\
step 49 6.113265633s 576d1f4aa586abe4658735dc2ed3f4ae22fa1dfbeeb41fb514ed83e35c19b386 node-3 committed through 4\n\
step 50 6.116413820s eeeec654ff6d002275fe307a266eb0a888e70e873bdf5d0ffd050f2419685229 node-4 appended entry 4\n\
step 51 6.182708113s 5d582c8a59de71e315e4583904179bc9dd4192cbb1302f7484677d983f227ef0 client heard command 3 is done\n\
step 52 6.643340592s b62f2c487fe76dbbf8b9cf0d953c7d287eaf2a2402833627ff886c0c95c3da90 node-5 committed through 4\n\
step 53 6.644645615s a0e49caf07e7ae0890b85ec298dce73389083f910bd572ec516e9365be0916b5 node-1 committed through 4\n\
step 54 6.652284080s a298670630abf43814d9f9a0c88a68c353c6b47faca0df72ff6e28cf5352a464 node-2 committed through 4\n\
step 55 6.654122691s 95be8233bdcc6a5afdfcf358f0b9d63fa33c4275ac9421d7f5b84d2b1dd77246 node-4 committed through 4\n\
step 56 8.000000000s ccf01fefa1df5f25f47493f700402f482dc4df34b53784a57fd036edf09154a9 client sent command 4\n\
step 57 8.069371449s b20ad826ddd2f5a00e3c98e97e226c50b2e69365d7277f819ac88e1d30aecb96 node-3 took command 4 as entry 5\n\
step 58 8.084797864s 2e1ac895af6fa04712e3ed86bd3a5383d985efa0e51773d473668732413f8fc6 node-4 appended entry 5\n\
step 59 8.091407423s 0a32220202dbaff387e67549722dc3a370122fb602f82f792b87167deb829837 node-2 appended entry 5\n\
step 60 8.102208997s a5b7afd74a6d83e87297713d19076dc9d00768dcbf7cdeae860c0cf99cacdc77 node-5 appended entry 5\n\
step 61 8.123748300s 626f118dc6ddd0aee7987fc01b8f02a28b846bf72629a0df5df03a25e1b17a23 node-1 appended entry 5\n\
step 62 8.140993304s d867349f3481bd7b23dd4aeec8368382b71e80f4461d7c5bb3313ed8feae9525 node-3 committed through 5\n\
step 63 8.191577645s d7e01abbe2f7c35b884e244f52fc48963f8321133dbd8063b6544c1c0167f303 client heard command 4 is done\n\
step 64 8.662733991s 2d02b61384eaa8fdf6d3e5a262347db8b1456a8cc18e254d8bbee34357bdbbe1 node-4 committed through 5\n\
step 65 8.669303405s 21e4dd4d47efd75e6c8e029cfb552022fb89f90890d84c4188296f2ab75def54 node-1 committed through 5\n\
step 66 8.677901826s 49e4127c0e918223b11af7a74949c4d0985fd99658d1446cfe5364daacf65da4 node-5 committed through 5\n\
step 67 8.690299222s 3fb4dd22fc6a12de64501dd977d490b9281bc9754b1f3139663fc5c79115b286 node-2 committed through 5\n\
step 68 10.000000000s 7538608da78b15a51f2b3f4760cb39d0266cd401f8d62f693a28f9bb4746b031 client sent command 5\n\
step 69 10.076079964s 3f76581ebf7f3b83177069f194bfa24f44f2dc16aaecda8e69b0fd807bdaf66a node-3 took command 5 as entry 6\n\
step 70 10.086264946s 71ef4a60753b7d6b6783136c43eaeecd0a1c1e28a8aba8b21bbdded318311958 node-4 appended entry 6\n\
step 71 10.097925322s 6073da2ace3f1f6b47ba217fd67657b1432e8fe82d7e02d1d33c2c76a547ad9e node-1 appended entry 6\n\
step 72 10.118046656s aa9f5424b7246adfe45d419c18ee07b55e326faa4b30cce4ada686178bcb955a node-2 appended entry 6\n\
step 73 10.124365408s 623569b4bae19d983d6d18f148777f41c6953dac388a79f3db64c5f81525230e node-5 appended entry 6\n\
step 74 10.145733465s 9b3dd33575b6b73b626646d7b74706da50b03ea7202d229df3525819cbc94158 node-3 committed through 6\n\
step 75 10.245655579s 44dffb0e9e78be8ce45badea97882136ae066e6896d6e37a4a2325a1c8b9f09e client heard command 5 is done\n\
step 76 10.631863144s 678a08ca5d2f1dfc32491bfbacf5b9587dd325b729e9ba640b5e3ab2a88f69f0 node-1 committed through 6\n\
step 77 10.633078491s 47a93ee8938831584eb3a4f4769f0530bd6dcbe68a1512c81193df4ed305a7e5 node-4 committed through 6\n\
step 78 10.665063238s ff45d13cdf14476f550556adef24cc9a0d55722810fd1bc9173c32fc78a5751f node-5 committed through 6\n\
step 79 10.687063493s f85002985892fbe17d3bd09f8f2fa9f702a74c65a3a4cb91c23fced253468a85 node-2 committed through 6\n";

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

#[test]
fn a_replica_back_from_a_partition_with_a_stale_log_can_win_and_overwrite_what_was_committed() {
    let (trace, outcome) = runs(BREAKS, &faults(ISOLATED));
    let Outcome::Fail { reason, step } = outcome else {
        panic!("seed {BREAKS} keeps its log under the isolation: {trace}");
    };
    assert_eq!(
        reason,
        Reason::new("a committed entry changed")
            .unwrap_or_else(|e| panic!("a reason is a reason: {e}"))
    );

    // Reached by a different route from the invariant's: the step's own words, and the commits the
    // trace announced before it, rather than the fields of the world it recorded.
    let healed = after_the_heal(&trace);
    assert_eq!(
        first_to(&healed, " became leader of term "),
        Some("node-5"),
        "the replica that was away leads the first term after the heal"
    );
    let message = trace
        .at(step)
        .unwrap_or_else(|| panic!("step {step} is a step the trace has"))
        .event()
        .message();
    let (replica, kept) = message
        .split_once(" dropped entries after ")
        .and_then(|(replica, rest)| Some((replica, rest.split(' ').next()?.parse::<u64>().ok()?)))
        .unwrap_or_else(|| panic!("the breach is a log cut back: {message:?}"));
    let committed = trace.steps()[..step]
        .iter()
        .filter_map(|earlier| {
            earlier
                .event()
                .message()
                .strip_prefix(&format!("{replica} committed through "))?
                .parse::<u64>()
                .ok()
        })
        .max()
        .unwrap_or(0);
    assert!(
        kept < committed,
        "{replica} kept {kept} entries of the {committed} it had committed"
    );
}

#[test]
fn a_stale_replica_that_stands_and_loses_the_race_leaves_every_committed_entry_in_place() {
    // The other half of the pair, on the same schedule. Node 5 stands after the heal here too — the
    // case asserts it — so what keeps the log is another replica winning that election, not node 5
    // never asking.
    let (trace, outcome) = runs(HOLDS, &faults(ISOLATED));
    let healed = after_the_heal(&trace);

    assert!(
        healed
            .iter()
            .any(|message| message.starts_with("node-5 became candidate for term ")),
        "node 5 stood after the heal: {healed:?}"
    );
    let leader = first_to(&healed, " became leader of term ");
    assert!(
        leader.is_some_and(|leader| leader != "node-5"),
        "and someone else won: {leader:?}"
    );
    assert_eq!(outcome, Outcome::Pass);
}

#[test]
#[ignore = "a sweep over many seeds; `make local-validation` runs it"]
fn no_seed_loses_a_committed_entry_unless_a_partition_takes_part_and_not_every_seed_does_then() {
    // Fixed before anything was measured: the draws alone must never reach the overwrite, or a
    // reduction would have nothing to remove; and under one partition the draws must take part, so
    // some seeds lose entries and some keep them.
    let seeds = 0..500_u64;
    let failing = |schedule: &FaultSchedule| -> Vec<(u64, Outcome)> {
        seeds
            .clone()
            .map(|seed| (seed, runs(seed, schedule).1))
            .filter(|(_, outcome)| *outcome != Outcome::Pass)
            .collect()
    };

    let calm = failing(&FaultSchedule::default());
    assert!(calm.is_empty(), "with no faults: {calm:?}");

    let isolated = failing(&faults(ISOLATED));
    let reasons: Vec<String> = isolated
        .iter()
        .filter_map(|(_, outcome)| match outcome {
            Outcome::Fail { reason, .. } => Some(reason.to_string()),
            Outcome::Pass => None,
        })
        .collect();
    assert!(
        !isolated.is_empty() && isolated.len() < seeds.clone().count(),
        "{} of 500 lose entries under the isolation",
        isolated.len()
    );
    assert!(
        reasons
            .iter()
            .all(|reason| reason == "a committed entry changed"),
        "{reasons:?}"
    );
    assert!(
        isolated.iter().any(|(seed, _)| *seed == BREAKS)
            && isolated.iter().all(|(seed, _)| *seed != HOLDS),
        "the pair is the pair the sweep finds"
    );
}
