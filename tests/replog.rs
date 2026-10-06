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
//! election timeouts. The recorded trace feels both. The pair under [`ISOLATED`] feels the seed
//! too, and that is what it was chosen for: under one and the same schedule, seed 0 loses committed
//! entries and seed 72 keeps them, and which one a run does is the draws' doing — whichever
//! replica's timeout runs out first after the heal stands first. A schedule whose failure did not
//! depend on the seed would leave a repro's seed doing nothing, which is the trap the reconciler's
//! first fixtures fell into.
//!
//! What no case here feels is the replicas' vote being granted whatever the candidate's log holds,
//! **as a rule**: the pair shows what it does on two seeds, and the gated sweep says how often under
//! one schedule. That a vote is granted without the log being asked about at all is held by a unit
//! case beside the module, and nowhere else.
//!
//! The order the log applied the clients' commands in is checked here a second time, by a route the
//! module's own check does not take: rebuilt from what each step says — which command a leader took
//! as which entry, and when a client sent it and heard it was done — rather than from the fields of
//! the world it recorded. Nothing here makes a run break that order, since nothing in the module
//! does; the unit cases beside it hold the check to account on worlds built to break it.

use std::collections::BTreeMap;

use chronoloop::clock::VirtualTime;
use chronoloop::fault::FaultSchedule;
use chronoloop::outcome::{Outcome, Reason};
use chronoloop::systems::replog;
use chronoloop::trace::Trace;

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

/// The lowest seed that loses committed entries under [`ISOLATED`], found by running.
const BREAKS: u64 = 0;

/// The lowest seed that keeps them under [`ISOLATED`], found by running. Node 7 stands after the
/// heal in this run, and loses: node 4 stood 38 milliseconds before it and had a majority's votes
/// before node 7's requests reached anyone.
const HOLDS: u64 = 72;

/// The run `SEED` produces with nothing in its way, recorded from an actual run.
///
/// Node 5's timeout runs out first, at 1.96s, and every other replica votes for it; its no-op is
/// entry 1, and each command after it is taken, copied, committed by the leader once three replicas
/// hold it, and answered — the followers hearing of the commit on the next heartbeat, half a second
/// later. The command entries are 2 to 16, and no one stands against node 5 again. Each round's
/// three commands are taken in whatever order they reach the leader, which is not the order they
/// are numbered in: command 6 is entry 6 and command 5 entry 7.
const RECORDED: &str = "chronoloop trace seed 20261006\n\
step 0 0.000000000s e23fb45b2b8f55d00f3a20685b1ae4d7c00b527473d7da131866432c70ff2134 node-3 started as a follower in term 0\n\
step 1 0.000000000s 5dfbfc08e04d1c91fa8210f129f2eea098490466f288ba9b8b808272f088a248 node-4 started as a follower in term 0\n\
step 2 0.000000000s 90bf7b15cd7d923ed3ad594e398c8fde3b9ffb0f262ca65a64116bd2219ef125 node-5 started as a follower in term 0\n\
step 3 0.000000000s 4b246fdd9e58a9b79991a7a8e21282489a0398436a9adc1268ae2456d26c5209 node-6 started as a follower in term 0\n\
step 4 0.000000000s b3c5fa999971079f393e1d76e425d857ec0c15a2d39fdabb8ea0369175655de0 node-7 started as a follower in term 0\n\
step 5 1.964643210s 846c33507a806bb47c04cccec3e4bc56b2de50a6c6add350963b21b39a6975bc node-5 became candidate for term 1\n\
step 6 1.990691194s fd318bc45a119a1b8247c35755935728a72ed206d23aa2fd01f889b508145d5e node-6 voted for node-5 in term 1\n\
step 7 1.999827301s 7d7c8c59d191b7b52769b72e84261f05e31dcdd42cb1e9aa7b85b82d38904374 node-4 voted for node-5 in term 1\n\
step 8 2.000000000s 5fc63202f467b4e84d38184b16bd8355251eb745158f3049f0e2bc6f8861fe1f client-0 sent command 1\n\
step 9 2.000000000s a383876a4ca2578726283d0247fb4afe243f4d1b4207f66aed88c1eaac3f1856 client-1 sent command 2\n\
step 10 2.000000000s 3e1e192d88fb55d9c3bb2a31027ef4438543cbd5947ce801e38536cb10c35b8e client-2 sent command 3\n\
step 11 2.022357527s ae34eb7a860fdb8cc8994265003b61ec07c79bf5463e01f4f89ab67900552b89 node-3 voted for node-5 in term 1\n\
step 12 2.048376268s bb126518e89b26c0de1c38b74fb62868f76542a4a118d7bb329076f3d8412022 node-7 voted for node-5 in term 1\n\
step 13 2.069399162s 18cca6b47598417e76c6e3d3ed500b48a4540708cb34d5833d628b020e2c6c0e node-5 became leader of term 1\n\
step 14 2.100764061s 07eea95a2e70f49e4ed3541ed201504f6abb20f28463eb2061a0a9370623fadb node-7 appended entry 1\n\
step 15 2.112691893s db6c55f1f107b215ba027667d8b7bd4086daf8e9836e5ab6b45071372a6bca58 node-6 appended entry 1\n\
step 16 2.115240061s 2a07ef03010256147df3daf8bced3c472a8a161b8948b4db10ec8baff860e2d4 node-3 appended entry 1\n\
step 17 2.132018736s 7b9d8f1067f570fe0d7d6acc953e94e1dab6d0ebd8011a0b835fd76d04812e38 node-4 appended entry 1\n\
step 18 2.155305214s cc269519e2a10e4977a8e9947473858a2921b539b1d1a4ef85ab09044276432a node-5 committed through 1\n\
step 19 2.221154029s fe889a5b8931230186e14046108b0f988d3ceb116f8dac0aad17ea4f5948c1ca node-5 took command 1 as entry 2\n\
step 20 2.251228961s bae63d415a6274dffb2bbc7165d96ff9f3c555cc5fddcfca0bde60631ebf50b0 node-5 took command 2 as entry 3\n\
step 21 2.265289790s 0c5fb33baad189252fccfc23699f145cefbb7ae5874385e967f198d86488d1c1 node-3 appended entries 2 to 3\n\
step 22 2.265289790s 530a1b839cbb16a6bd3a03cd05d86e2a9d0f57201db503b00bb0b47139055cc0 node-3 committed through 1\n\
step 23 2.271667997s 1616a1e4c23bb321c786261356f101654fab622e39b815b11838ca970e7d7e77 node-7 appended entries 2 to 3\n\
step 24 2.271667997s 896bf87f5c1bfdd376d8e545db426bd08954d037c5aef861b4823c539ad4bf23 node-7 committed through 1\n\
step 25 2.299949923s 3fe393561d6cc933556fe6c4a85aaff404b486206dc33dffb4eec8437335f38d node-4 appended entry 2\n\
step 26 2.299949923s 6520bf02bcab1520863a8a571512565fe9febca25548b4e4f7365557824b51f9 node-4 committed through 1\n\
step 27 2.306541900s 8ffedcc6918ff5b3266e622c591d8c04f65549ea6e653ce6e8431e9eb3e71d5b node-6 appended entries 2 to 3\n\
step 28 2.306541900s edb537e6959b09dcc075f8939b4b4230d7b8d7e9feb1e550392da04b15ecb8c4 node-6 committed through 1\n\
step 29 2.319605561s 4157fedc753f140bb6a6af5afe90bb9f320418e3a413f6ad030f9d4591217420 node-5 took command 3 as entry 4\n\
step 30 2.338612781s ce35689fd70a3d52ddc64589bf2399110e7158e9a8c478e8899a9a6586c85d88 node-6 appended entry 4\n\
step 31 2.346818905s eac6b6626496ed28a4ed176ce126ec9300f1855d5f9125402a69bd60f65a0e65 node-4 appended entry 3\n\
step 32 2.351281630s 7fc2a666fd817cfba822f05a61277fcf2910177f4fe87d6cd64ee7889d1f13c7 node-3 appended entry 4\n\
step 33 2.358479553s b82513f806d64bb4151c19ad05e8af659b90bc67ede71c277f9498c1194b7dba node-5 committed through 3\n\
step 34 2.374457152s 235692c3be6e21259fff588898b9594d3dfaeaa696d9e1849395e2af05a3e6f3 node-6 committed through 3\n\
step 35 2.386104572s f8a3847d1557718503ab760f14246aefadd3668c677c49dc9a183ce9d6dd3d90 node-7 appended entry 4\n\
step 36 2.389144683s 00ffd56241b58a448c6abeadca1e80663b4db0ef6b087f99e1ae0c6497a45ead client-0 heard command 1 is done\n\
step 37 2.405282867s a54ba348c149a852243fb04dfdba97389927c797d2c9b2acaa6588951994934b node-4 appended entry 4\n\
step 38 2.408623372s 27442dba68cd3d4e1630be1f0fa62c8e45c8179e121f0bf4ad9c5a73e264485d node-5 committed through 4\n\
step 39 2.414202630s 64dec398cbe85c2e8ca62301dfc7ca7341429f8894b9c1de07907f763222e2b7 client-1 heard command 2 is done\n\
step 40 2.421423731s aaf7912055c6d3dfed8914e5c178ed8cdd5477a273a3a507a9b8269dfbaacdec node-4 committed through 3\n\
step 41 2.450758967s 99e881fc1e2216bbabb31ef536dd563edd58fd89d4c53f7d7a367164769a4fca node-7 committed through 3\n\
step 42 2.480961395s d0592ae0a5ee3a7ef0495e41618b89ff9bf0354226da6788de899019309d0cff client-2 heard command 3 is done\n\
step 43 2.581967610s a926885ec23319dff4c08f91db86f5448814365879c8932adf5733abca4fca2a node-6 committed through 4\n\
step 44 2.588452632s 7859858a95665fab233f58f0190200575767dc714e6cf36b7d36e88da304e01c node-3 committed through 4\n\
step 45 2.615421556s 69bb7e1780ac394365cd51f11885e1b8a943ed191d1ee287a916a958a28497fa node-7 committed through 4\n\
step 46 2.636619772s dd4068f7f8b2ca96ad844660c8aa0471f259e3b7adb24429a72c72789e64f40e node-4 committed through 4\n\
step 47 4.000000000s a6d37ac6c8bdce6d88c05e27306fd2b20da7bad29176b023776d0c2ad2042b60 client-0 sent command 4\n\
step 48 4.000000000s 1efda0c9462147ba19a9db4efb45a3fb5d0d2186024da243d380bd1b10bb3b45 client-1 sent command 5\n\
step 49 4.000000000s 6a3cafdc88fa9fc9d5641f335badac635e489720089521e62d63dd5be3f79e48 client-2 sent command 6\n\
step 50 4.027345412s b0cc5d43e703f542a2dbbe7abc46590f05e191654eaae1f70882810d6590ad22 node-5 took command 4 as entry 5\n\
step 51 4.027788472s 43de446f66022405e567e921aa415fa047809d5f5f143ff6c3d1b2d0f9d766a1 node-5 took command 6 as entry 6\n\
step 52 4.039004660s 9ffaed7251baf201036c2eb0d03e53a8d53fbbe805bd357f8accb675b2c249d8 node-6 appended entry 5\n\
step 53 4.050448861s fe2abc70b08e90a961afdf79a9f87b1914a2f0c3fba0775e22ef7e5c4ee44c5c node-5 took command 5 as entry 7\n\
step 54 4.052035059s d76e79502b70c5b55f6c683fc00402355c9923f13d570f8840d6cab988d85327 node-7 appended entries 5 to 6\n\
step 55 4.081622262s 6573e5dfda7b5838c2d72d712edd3e45869ced2aaadf35b2039a408403057c14 node-6 appended entry 6\n\
step 56 4.090250204s 136fa0197421d5c26e4dd901e4499a7031fd13b00bab934b46269e4451547872 node-3 appended entries 5 to 6\n\
step 57 4.090550395s 0646382af2c755870b6aa2230b8e5d3eace714657ae47ebf89dd1bbf9bd057ac node-4 appended entries 5 to 6\n\
step 58 4.107547734s a91ab875426151ea7a30e2a7e6fc897b272bd6774359a7131fd2ec87db425224 node-5 committed through 6\n\
step 59 4.119270962s b43e0d7b1c8766ab4f8e41de9905d8f807b25b5147be3844023d23c92e510169 node-7 appended entry 7\n\
step 60 4.119891341s be0ab76d8aa2876b2626d816aafc8ea0ef88f9a9da02f493836f583d8c8dc3ad node-3 appended entry 7\n\
step 61 4.125854165s 924ec95bb55f4f075e47252c0181ddb071be4889268099a553d63f1886cc7db3 client-2 heard command 6 is done\n\
step 62 4.128214450s 7389ab2ee07e510bb07f933be29f13ccf536b0a39dc58321bf8cf565bf312ee0 node-4 appended entry 7\n\
step 63 4.129127188s 44d937c70088afda415fa96a4214ae3e7f2e483bd157e8e7033b520d9ebe50d7 node-6 appended entry 7\n\
step 64 4.157555729s 18fcb4f00094b5bf89246e1afdec2133f0d329936994075b2fe4391a728ea364 node-5 committed through 7\n\
step 65 4.171570173s 871c2c886f189fa08adeaf2fcfd15b262b197113291fa9259ae9623ad63f4f39 node-6 committed through 6\n\
step 66 4.172833526s 841c3551931a9a9753e51fb80012be10591f9b37546a7f65c21991bddf25d30f node-4 committed through 6\n\
step 67 4.172982144s f48a7ba6e6e2a53c1bc50b0e822d2d3dddeb57c3ad286dbddb87115ea84cf19b client-1 heard command 5 is done\n\
step 68 4.174069414s e7c638f9fc812afc761d31b02e48dc38e48c75e511b7e46d9b973a7f1cef30d0 node-3 committed through 6\n\
step 69 4.202715748s 39ca30a8a2f60bde42d20737ad224fe6959acd5d39d1794b3bd83918728e6cd6 client-0 heard command 4 is done\n\
step 70 4.223559916s e1d8d4645c0870b410135de61e367475b7d810cfe508a9a16c8521822fd005be node-7 committed through 6\n\
step 71 4.586644166s 189538c65223952920cebf217327235d2cca776d04bb96fae9138c5badd1c07e node-6 committed through 7\n\
step 72 4.589703088s 4882147adc3fdea8351fa56fc2adb7f003a915cae2ddda6f799b6d96cf42e60f node-4 committed through 7\n\
step 73 4.605769706s 66f2fe2bfdb732a2f8f8c95e0c70b3d401a339a1cfafb99561fb2827680e27b3 node-3 committed through 7\n\
step 74 4.613214332s 5980b1463133c3a188a3bfcd3472bfa77efa3d9a49332290d3077bf4569e9430 node-7 committed through 7\n\
step 75 6.000000000s 1809ded001377f00b4c040591a89296e063185d30509fd1f6baaeb09101e90fd client-2 sent command 9\n\
step 76 6.000000000s 5d5fd94b9eab0a9dee03161c364da44b2107f654d1611b51605a3d888ccc5cd5 client-1 sent command 8\n\
step 77 6.000000000s 71b2f319d0eb4e49c0a0b3a882604dab6fc1bebf6955210122b1efcb07a66ae3 client-0 sent command 7\n\
step 78 6.074105029s 1b2d8f8e73eb778369026fc2348cbc62adb7c7e686eb747df9436bce18f20d6f node-5 took command 9 as entry 8\n\
step 79 6.075868739s cda2ffddf452f920a1a31d7a9f86342a22bf8225ac656be7d30739d7e3816b80 node-5 took command 7 as entry 9\n\
step 80 6.084330181s 1a7f52411fe3083955ab4b6cef74a6266f8764d4b1958db36784004ea579d156 node-5 took command 8 as entry 10\n\
step 81 6.086053721s 449dc675de8515f23cad5120a223ea6eee15f625a76ef67a75496ea8e11c22ba node-3 appended entries 8 to 9\n\
step 82 6.095950387s 23761e0346cea143cbc608775b1de479442bd66a6ff3878070f833f88b7dd604 node-6 appended entry 8\n\
step 83 6.108908045s 4f75856382e7df85da0b05353339b297e04b6434dfa3ec99776ef3f8ae92c8d8 node-3 appended entry 10\n\
step 84 6.115473453s 09b1608a1d3ff7d02c0ff8f7e5b1fd89d45d3e468016f4be8da164d9f9059ab4 node-7 appended entries 8 to 9\n\
step 85 6.124154183s dd153f2eebdbf00ca9f9e5372386fc2643f18289741abedc823d504131c03097 node-4 appended entries 8 to 9\n\
step 86 6.124159126s 9f4b436d972a55892f449a9d6b53da8cbdc10a3fe76e1b95cbad664e8056a4b7 node-4 appended entry 10\n\
step 87 6.138545133s ff6a51ac44d15264203f8167e969e68ef43dff145d76c8a76f6d0be402db3cbf node-5 committed through 9\n\
step 88 6.147302397s 835ddea60ab32d26ca2d4304af7d3d2b272d29487301a39d5cee5552b3d15961 node-5 committed through 10\n\
step 89 6.152266630s 32d11f4983e5ed06f4a1247313fd90aa35fcc3efcc62b32d85c0911524b5a9cb node-6 appended entry 9\n\
step 90 6.161370534s faf0ce919a9a0489acbeb8326a1d0695ab7c08d3af57b6ddee30328306944eaf node-7 appended entry 10\n\
step 91 6.167896684s 3c141c219ebbc2799a06d45c8c956269a2becb4b4edb61f8415bd78d422fd111 node-6 appended entry 10\n\
step 92 6.203006009s ceb25faf80294375c2f15cc87f56f45fd76942e8e57e313e2d6418830bbee3d7 node-7 committed through 10\n\
step 93 6.203855925s f029c7f942c7b8b6005eb3424dd8aaaea6060b2997f7970aae02be384daf57c9 client-2 heard command 9 is done\n\
step 94 6.212314456s 980e7d485c2bf7c0e6177519267b1e754546a1535264cc417210e200c7159983 node-6 committed through 10\n\
step 95 6.224072786s b01d81169908ffd4116f5769a08dd92b9cc299e4bcda91b23650364124f05865 client-0 heard command 7 is done\n\
step 96 6.226694168s cd28dc1a5d8ac54663324f3e34bf1daa38fa43c78c7ccc65f6c8059e94cd6e8d client-1 heard command 8 is done\n\
step 97 6.228775738s 6187478bd4e945e1a23141b66d721167ca13ac41e4af6b803513cfdb6ce1d1e3 node-4 committed through 9\n\
step 98 6.636725350s c66af9159e2d0dc0feeb69f68dfe90e4bc1c4beaa6bec3a0b994c51113b12029 node-4 committed through 10\n\
step 99 6.644893104s 9c02fcd7d46933fd05d31003d301a8a8f6e9ec47bc4c13b5e652c02e235a1745 node-3 committed through 10\n\
step 100 8.000000000s 80bf55e584898d3c3c761f08bec55ff569a7340562d2c97291a986420034a455 client-2 sent command 12\n\
step 101 8.000000000s 3be4bbfd11b458064d32c4fc3738ac81fc99517cb019d3fd5f5e1e42c990a3ec client-0 sent command 10\n\
step 102 8.000000000s 9339e649f3703361fbbdfd8e5597c50da1cbd2ad4f61844181527d58a8638090 client-1 sent command 11\n\
step 103 8.015157232s fce357da1a4bbb1110f57d81a159a3d59d6086ed060292c7980341c79d5a5413 node-5 took command 12 as entry 11\n\
step 104 8.044541796s e17042e37e1bb5e7a850e374ac60e571217195d22522417c7b8b7fc7fbc00e78 node-5 took command 11 as entry 12\n\
step 105 8.051488813s e5e7cc4d8e9b7089cec426d931e67b99f2a2a8b6a1a9eceb0b4d961db5f118bf node-5 took command 10 as entry 13\n\
step 106 8.068263625s 1aab24e98f5dab1b723eee948cc4eab3556d87d01c043cc29530080d0d947977 node-3 appended entries 11 to 13\n\
step 107 8.068380196s 2f9e411cc70dfba5c921e355d0f46955c36c8820f0b0ecc69e54844fdd6e24b3 node-7 appended entry 11\n\
step 108 8.070178188s b355f24b5c4b30e2ef23658f4204e7a1d25959d45c3edfff5144951a17138fba node-7 appended entry 12\n\
step 109 8.080140865s 73ed0348ba3356a508b15968708a6ea03cff3decedbcfbce072ae0878dd9ed2d node-4 appended entry 11\n\
step 110 8.083107473s 7000371b6505651458f4d64ef441c45dd823a6ea2ac56d155a57d6058ed0d7bd node-6 appended entries 11 to 13\n\
step 111 8.113655516s 84ebe715e387a419c616bdfb56d20190776074ef51e6fc4b9a68166dd16e9156 node-5 committed through 12\n\
step 112 8.120899825s 8fe22462032a4417e04389c1da9766489d5c6249b768e7b2252dc3e1b422dded node-7 appended entry 13\n\
step 113 8.130361060s 894536d24a8732ad44d9b9fbd0043b4b52ca65a65efee84dd5ec1cd3ba3329d3 node-4 appended entry 12\n\
step 114 8.145233836s de13f18127aedc3eab108f154179b90a23a0856f7a205a36d6ed3f244ef3fea5 node-5 committed through 13\n\
step 115 8.150399025s 327373dd9246a7deb91848d3486f92a1884c3964951e23b44a8883ec1035c9ef node-4 appended entry 13\n\
step 116 8.156723908s 803924d124dd8f52d62a70e32c7363e9c7739ac292be2555657d635e466900ea client-1 heard command 11 is done\n\
step 117 8.161497531s 0ed6393beaf530f5325dcab3b8582b4a7fc6886750d74a1934a343565dd1e402 client-0 heard command 10 is done\n\
step 118 8.174111046s debe640b2033c9da67b61736caabb5a9a589842297f0bce96590c2580fa42f3b client-2 heard command 12 is done\n\
step 119 8.185465090s 0869f5f6d61755b12d5b4808e665c66a3dd28f42bd50058e1230b58ec6d00d8f node-4 committed through 12\n\
step 120 8.214360856s a9bd4f234ff9e794f1b4f7cbdd511945ba093c724c9b18e4a2d9eb9a7c9d4d88 node-4 committed through 13\n\
step 121 8.230376882s e59bd553348f537bf40477babad9ae6322ecd250d9195e1729092771a97dc944 node-7 committed through 13\n\
step 122 8.612462863s 058093de8067d6345f8c4464047e48acef93d70468ef42945ce3c8cfe570110f node-3 committed through 13\n\
step 123 8.616435010s ab6de014f10fd310958a1dd00cb522deec356cd9abf6327d9cb77a0c821473eb node-6 committed through 13\n\
step 124 10.000000000s ecbbe7b8bbdfac758e3a3bf35b90d30dc2663f8a0d17a22794b2c9951340c364 client-1 sent command 14\n\
step 125 10.000000000s 8554128f1255ad0e0b9f9596046ecdcac44b4d24bb8448a0e43dbf9441ddba1d client-0 sent command 13\n\
step 126 10.000000000s 94c75fd4c7fd8aae8d0eb43b98c68e606d0d056762e43b2d91a986b3fefe9a66 client-2 sent command 15\n\
step 127 10.023812377s dc79565db4ddec97c25b03297574f1207212c61a2d73bdbf61df7cec913dd119 node-5 took command 15 as entry 14\n\
step 128 10.043079506s ee67f3bee7ebc6a85ae64a049540acffcf2e03e89f317a3f705339153d7284f8 node-5 took command 13 as entry 15\n\
step 129 10.045366891s 974dbf4a50c91c674c625a2243f7ad714dba6abd020873d272d1fa51f6716cd6 node-5 took command 14 as entry 16\n\
step 130 10.049362394s 79150d5af09de192f36e6ea750a9c90d7c5bf2729bb2b6c0a9d530808db74489 node-3 appended entry 14\n\
step 131 10.053665356s 57a01c99614eb1ef5c03fb3c584d494128e2d65d31a4df224ec285b0f67f9c76 node-6 appended entry 14\n\
step 132 10.056959596s 502d3c482185a7a62c86a70a700e917ae92b6189bf6a5c37391aab5b92e18ac3 node-3 appended entry 15\n\
step 133 10.065393791s 4fceb0792aaf375758c3dcb40d0e8c19ebec07c2dc631dda295370e49b87a9fd node-7 appended entry 14\n\
step 134 10.069699852s 9861b2551e35683e37e42937eceef435eed5788d9d6f412ffc6d32a57acfed6a node-3 appended entry 16\n\
step 135 10.082105535s 1654aca3c3ecba7243b196eebc526aecd205b362ec8e9078cef4cae18ff945d1 node-5 committed through 14\n\
step 136 10.095554084s a7f53055caf4bfede00352ef87a8300712737d9d248c273c76ed9b3281bc7ced node-4 appended entries 14 to 16\n\
step 137 10.114540290s 7cf471b40fc97f5f430995974082fda501e8b9b504439d9ed1875f0ba21b4057 node-6 appended entry 15\n\
step 138 10.114800667s 5f3c0a011a0ec9b2df17bf5a335d6183b6e95d63f28097dfdfc417a19000ee50 node-7 appended entry 15\n\
step 139 10.123105824s 1ec9cd429290337e4b33aec4dce5b363aed7633c85d46815e35154859acd93b0 node-7 appended entry 16\n\
step 140 10.123815266s 9bca98b0dea9367bba8559d939dda679602e1816ad544a1acfba5e3791b0da02 node-6 appended entry 16\n\
step 141 10.123815266s 1ca10fce7b8005bedb36bc1e2b76b3f2b29d73f07ff09375c36c7ed94c557e9b node-6 committed through 14\n\
step 142 10.150235423s 50aeb57d08380f71db287259945ac2e7a37607de49b28cea2ccaedcc37752934 node-7 committed through 14\n\
step 143 10.155509781s bc8e3dcdf752749f7122b5540653f88903bdc2801b7c2868dae1b70986eaa312 node-5 committed through 16\n\
step 144 10.174276981s 8881a281bdd95dd574e5f9dd03e5f8c3ee0ac7b7830b78eeedbbdb64bda6352c client-2 heard command 15 is done\n\
step 145 10.196723973s 2d3c2d69b5257335c7e0142ecc43db202db7d01fcd0cb6e080cb34edc70a1817 client-0 heard command 13 is done\n\
step 146 10.235564134s 67bfb17f8052278bf611fb24c3ade91e3e17eeff8b836cea44003948b4a85fda client-1 heard command 14 is done\n\
step 147 10.587346672s 944b48096f87b454b0ef16222632dd6a9fdbb0d3fc483aa42e27a6a54f85d737 node-6 committed through 16\n\
step 148 10.605247289s e279649c98462e81185cd901bcfde29d77e354c92262f742aa3522219eaf492a node-3 committed through 16\n\
step 149 10.620730554s 5a3c07793fbf798b002903ec34549649459e9bc3290c3e63e1343f227db0e59a node-7 committed through 16\n\
step 150 10.631601955s 07af69df83bd9bd77afbc91f189433a073f6396e54bd793c8790d70b2d81230b node-4 committed through 16\n";

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
        ("the stale replica losing the race", HOLDS, faults(ISOLATED)),
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
        said[&6].entry < said[&5].entry,
        "command 6 overtook command 5: {said:?}"
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
        Some("node-7"),
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
    // The other half of the pair, on the same schedule. Node 7 stands after the heal here too — the
    // case asserts it — so what keeps the log is another replica winning that election, not node 7
    // never asking.
    let (trace, outcome) = runs(HOLDS, &faults(ISOLATED));
    let healed = after_the_heal(&trace);

    assert!(
        healed
            .iter()
            .any(|message| message.starts_with("node-7 became candidate for term ")),
        "node 7 stood after the heal: {healed:?}"
    );
    let leader = first_to(&healed, " became leader of term ");
    assert!(
        leader.is_some_and(|leader| leader != "node-7"),
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
