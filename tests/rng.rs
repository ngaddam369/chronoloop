//! Randomised delays driving a simulation: the same seed must replay exactly.

use core::cell::RefCell;
use core::time::Duration;
use std::rc::Rc;

use chronoloop::executor::Executor;
use chronoloop::rng::SeededRng;

const LABELS: [&str; 3] = ["alpha", "beta", "gamma"];
const STEPS: u32 = 3;

type Log = Rc<RefCell<Vec<(u64, String)>>>;

/// Runs three tasks whose delays all come from one generator, and returns what they logged.
fn run(seed: u64) -> Vec<(u64, String)> {
    let mut executor = Executor::new();
    let log: Log = Log::default();
    let rng = Rc::new(RefCell::new(SeededRng::from_seed(seed)));

    for label in LABELS {
        let handle = executor.handle();
        let task_log = Rc::clone(&log);
        let task_rng = Rc::clone(&rng);
        executor.spawn(async move {
            for step in 0..STEPS {
                let delay = task_rng
                    .borrow_mut()
                    .duration_in(Duration::from_millis(1)..=Duration::from_millis(100));
                handle.sleep(delay).await;
                task_log
                    .borrow_mut()
                    .push((handle.now().as_nanos(), format!("{label} step {step}")));
            }
        });
    }

    executor
        .run()
        .unwrap_or_else(|e| panic!("run did not finish: {e}"));
    log.borrow().clone()
}

fn labels_in_order(log: &[(u64, String)]) -> Vec<String> {
    log.iter()
        .map(|(_, entry)| entry.split(' ').next().unwrap_or_default().to_owned())
        .collect()
}

#[test]
fn randomised_delays_replay_identically() {
    let first = run(1);
    assert_eq!(first, run(1), "one seed must always produce one history");
    assert_ne!(
        first,
        run(2),
        "a different seed must explore something else"
    );
}

#[test]
fn randomised_delays_reorder_the_tasks() {
    let log = run(1);
    assert_eq!(log.len(), LABELS.len() * STEPS as usize);

    let spawn_order: Vec<String> = (0..STEPS)
        .flat_map(|_| LABELS.iter().map(|label| (*label).to_owned()))
        .collect();
    assert_ne!(
        labels_in_order(&log),
        spawn_order,
        "random delays must interleave the tasks, not leave them in spawn order"
    );

    let times: Vec<u64> = log.iter().map(|(at, _)| *at).collect();
    assert!(
        times.windows(2).all(|pair| pair[0] <= pair[1]),
        "virtual time must never go backwards: {times:?}"
    );
}
