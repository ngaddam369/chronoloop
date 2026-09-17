//! The one source of randomness a simulation is allowed to use.

use core::ops::RangeInclusive;
use core::time::Duration;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// A run's random choices, drawn from its seed and nothing else.
///
/// Two generators built from one seed produce the same draws forever, which is what makes a run a
/// pure function of its seed:
///
/// ```
/// use chronoloop::rng::SeededRng;
///
/// let mut first = SeededRng::from_seed(7);
/// let mut second = SeededRng::from_seed(7);
/// assert_eq!(first.next_u64(), second.next_u64());
/// assert_eq!(first.range(1..=6), second.range(1..=6));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeededRng {
    seed: u64,
    inner: ChaCha8Rng,
}

impl SeededRng {
    /// Creates a generator for `seed`.
    pub fn from_seed(seed: u64) -> Self {
        Self {
            seed,
            inner: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    /// Returns the seed this generator was built from, which is what a repro records.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Draws the next 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        self.inner.random()
    }

    /// Draws a value within `bounds`, both ends included.
    ///
    /// # Panics
    ///
    /// Panics if the range is back to front, in the same way indexing past the end of a slice does.
    pub fn range(&mut self, bounds: RangeInclusive<u64>) -> u64 {
        self.inner.random_range(bounds)
    }

    /// Draws `true` with odds of `numerator` in `denominator`.
    ///
    /// The odds are integers, so no floating-point rounding sits between a seed and a decision.
    ///
    /// # Panics
    ///
    /// Panics if `denominator` is zero or `numerator` exceeds it.
    pub fn chance(&mut self, numerator: u32, denominator: u32) -> bool {
        self.inner.random_ratio(numerator, denominator)
    }

    /// Draws a duration within `bounds`, both ends included.
    ///
    /// Durations are drawn in nanoseconds and capped at the end of virtual time, matching what
    /// sleeping on the virtual clock can represent.
    ///
    /// # Panics
    ///
    /// Panics if the range is back to front.
    pub fn duration_in(&mut self, bounds: RangeInclusive<Duration>) -> Duration {
        let low = nanos_of(*bounds.start());
        let high = nanos_of(*bounds.end());
        Duration::from_nanos(self.range(low..=high))
    }
}

/// The duration in nanoseconds, capped at the end of virtual time.
fn nanos_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl From<u64> for SeededRng {
    fn from(seed: u64) -> Self {
        Self::from_seed(seed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DRAWS: usize = 32;

    fn draws(seed: u64, count: usize) -> Vec<u64> {
        let mut rng = SeededRng::from_seed(seed);
        (0..count).map(|_| rng.next_u64()).collect()
    }

    #[test]
    fn same_seed_produces_the_same_sequence() {
        struct Case {
            name: &'static str,
            seed: u64,
        }
        let cases = [
            Case {
                name: "zero",
                seed: 0,
            },
            Case {
                name: "one",
                seed: 1,
            },
            Case {
                name: "forty two",
                seed: 42,
            },
            Case {
                name: "the largest seed",
                seed: u64::MAX,
            },
        ];
        for case in cases {
            assert_eq!(
                draws(case.seed, DRAWS),
                draws(case.seed, DRAWS),
                "{}",
                case.name
            );
            assert_ne!(
                draws(case.seed, DRAWS),
                draws(case.seed.wrapping_add(1), DRAWS),
                "{}: a different seed must not replay the same draws",
                case.name
            );
        }
    }

    #[test]
    fn recorded_draws_do_not_change() {
        // Recorded histories are only replayable while this sequence holds. If a dependency bump
        // changes it, every trace committed before the bump is invalid, so this test must fail
        // loudly rather than let the change pass unnoticed. Re-record it only on purpose.
        assert_eq!(
            draws(42, 4),
            vec![
                12_578_764_544_318_200_737,
                17_529_487_244_874_322_312,
                7_886_285_670_807_131_020,
                11_572_758_976_476_374_866,
            ]
        );
    }

    #[test]
    fn recorded_bounded_draws_do_not_change() {
        // Bounded draws go through the sampling the underlying crate provides rather than through
        // the stream above, so the stream holding still is not enough to keep them the same. A
        // recorded history whose intervals were drawn this way is only replayable while this holds,
        // which is why it is pinned here, beside the draw, rather than left to fail somewhere that
        // would blame the engine for a dependency's change.
        let mut rng = SeededRng::from_seed(42);
        let bounded: Vec<u64> = (0..8).map(|_| rng.range(1..=60)).collect();
        assert_eq!(bounded, vec![41, 58, 26, 38, 18, 9, 19, 49]);
    }

    #[test]
    fn range_stays_within_its_bounds() {
        struct Case {
            name: &'static str,
            bounds: RangeInclusive<u64>,
        }
        let cases = [
            Case {
                name: "a single value",
                bounds: 5..=5,
            },
            Case {
                name: "a small range",
                bounds: 1..=6,
            },
            Case {
                name: "the whole space",
                bounds: 0..=u64::MAX,
            },
            Case {
                name: "the top of the space",
                bounds: (u64::MAX - 3)..=u64::MAX,
            },
        ];
        for case in cases {
            let mut rng = SeededRng::from_seed(9);
            let drawn: Vec<u64> = (0..1_000).map(|_| rng.range(case.bounds.clone())).collect();
            for value in &drawn {
                assert!(
                    case.bounds.contains(value),
                    "{}: drew {value} outside {:?}",
                    case.name,
                    case.bounds
                );
            }
            if case.bounds.start() < case.bounds.end() {
                assert!(
                    drawn.windows(2).any(|pair| pair[0] != pair[1]),
                    "{}: every draw came back the same",
                    case.name
                );
            }
        }
        let mut rng = SeededRng::from_seed(9);
        assert_eq!(rng.range(5..=5), 5, "a single-value range has one answer");
    }

    #[test]
    fn chance_respects_certain_and_impossible_odds() {
        let mut rng = SeededRng::from_seed(3);
        for _ in 0..100 {
            assert!(!rng.chance(0, 1), "zero in one must never happen");
            assert!(rng.chance(1, 1), "one in one must always happen");
        }

        let mut first = SeededRng::from_seed(11);
        let mut second = SeededRng::from_seed(11);
        let count = (0..1_000).filter(|_| first.chance(1, 2)).count();
        let again = (0..1_000).filter(|_| second.chance(1, 2)).count();
        assert_eq!(count, again, "the same seed must give the same coin flips");
        assert!(
            (400..600).contains(&count),
            "even odds landed heads {count} times in 1000"
        );
    }

    #[test]
    fn duration_in_stays_within_its_bounds() {
        struct Case {
            name: &'static str,
            bounds: RangeInclusive<Duration>,
        }
        let cases = [
            Case {
                name: "milliseconds",
                bounds: Duration::from_millis(5)..=Duration::from_millis(50),
            },
            Case {
                name: "a range of no width",
                bounds: Duration::from_secs(2)..=Duration::from_secs(2),
            },
            Case {
                name: "from nothing to an hour",
                bounds: Duration::ZERO..=Duration::from_secs(3600),
            },
        ];
        for case in cases {
            let mut rng = SeededRng::from_seed(4);
            let drawn: Vec<Duration> = (0..1_000)
                .map(|_| rng.duration_in(case.bounds.clone()))
                .collect();
            for value in &drawn {
                assert!(
                    case.bounds.contains(value),
                    "{}: drew {value:?} outside {:?}",
                    case.name,
                    case.bounds
                );
            }
            if case.bounds.start() < case.bounds.end() {
                assert!(
                    drawn.windows(2).any(|pair| pair[0] != pair[1]),
                    "{}: every draw came back the same",
                    case.name
                );
            }
        }
    }

    #[test]
    fn seed_is_recoverable() {
        for seed in [0, 1, 42, u64::MAX] {
            let mut rng = SeededRng::from_seed(seed);
            let _ = rng.next_u64();
            assert_eq!(rng.seed(), seed, "the seed must survive being drawn from");
        }
    }

    #[test]
    fn a_clone_continues_the_same_stream() {
        let mut rng = SeededRng::from_seed(77);
        let _ = rng.next_u64();
        let mut copy = rng.clone();

        let from_original: Vec<u64> = (0..DRAWS).map(|_| rng.next_u64()).collect();
        let from_copy: Vec<u64> = (0..DRAWS).map(|_| copy.next_u64()).collect();
        assert_eq!(from_original, from_copy);
    }
}
