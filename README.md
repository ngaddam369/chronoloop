# chronoloop

Deterministic simulation engine for infrastructure control loops — virtual time, replayable history,
shrinkable failures.

> **Status: early.** The repository scaffold is in place; the engine is being built in phases.
> This README is a skeleton and will be filled in as the phases land.

## The problem

Control loops — Kubernetes operators, reconcilers, provisioning state machines — fail in ways that are
almost impossible to reproduce. The bug needs a specific interleaving of a slow API call, a retry, a
teardown and a replica that has not caught up yet. You see it once in production, and never again in a
test environment.

## The approach

chronoloop runs your control loop in **virtual time** on a single-threaded deterministic executor. Time,
randomness, network and I/O are traits, so the same loop logic runs against a simulated world where every
delay, drop and partition is drawn from one seed and nothing else. A run is therefore a pure function of
its seed: the same seed replays the same history, byte for byte, forever.

On top of that determinism:

- **Time travel** — every step's world state is content-addressed, so you can jump to step 37, diff it
  against step 36, and fork a new timeline from there.
- **Shrinking** — hand it a seed that fails and it reduces the injected-fault schedule to the shortest
  sequence that still reproduces the failure, emitting a repro artifact of a few hundred bytes.

## Prior art

Deterministic simulation testing is not new. [`madsim`](https://github.com/madsim-rs/madsim),
[`turmoil`](https://github.com/tokio-rs/turmoil), [`shuttle`](https://github.com/awslabs/shuttle),
[`loom`](https://github.com/tokio-rs/loom), FoundationDB's simulator and Antithesis all attack the same
family of problems, and chronoloop borrows liberally from their ideas.

Where chronoloop differs: those tools are built to answer *"does my network protocol survive
partitions?"* chronoloop is built for **control loops**, and its headline capability is what happens
*after* a failing run — inspecting, diffing and forking the recorded history, and shrinking the failure
to a minimal repro. A full comparison table lands with the design docs.

## License

Apache-2.0 — see [LICENSE](LICENSE).
