//! Property tests: generated fault schedules on the deterministic simulator,
//! and random-bytes robustness of the frame/RPC decoders.
//!
//! Runs are reproducible: a fixed ChaCha seed drives generation, so CI always
//! executes the same cases. Override with `PROPTEST_CASES` for longer runs.

use proptest::prelude::*;
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use raft_mvp::rpc::{self, decode_frame, MAX_FRAME};
use raft_mvp::sim::{self, Step};

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (1u64..40).prop_map(Step::Run),
        1 => (0u32..300).prop_map(Step::Drop),
        1 => (1u64..=3, 1u64..=3).prop_map(|(a, b)| Step::Partition(a, b)),
        1 => Just(Step::Heal),
        1 => (1u64..=3).prop_map(Step::Restart),
        3 => any::<u8>().prop_map(Step::Put),
        1 => Just(Step::ToggleMembership),
    ]
}

fn runner(default_cases: u32) -> TestRunner {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default_cases);
    let cfg = Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    };
    let seed = [0x52u8; 32]; // "R": fixed so CI cases are reproducible
    TestRunner::new_with_rng(cfg, TestRng::from_seed(RngAlgorithm::ChaCha, &seed))
}

#[test]
fn generated_schedules_keep_raft_invariants() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let strat = (any::<u64>(), prop::collection::vec(step(), 1..25));
    runner(48)
        .run(&strat, |(seed, steps)| {
            let r = rt.block_on(sim::check_schedule(seed, &steps));
            prop_assert!(r.is_ok(), "{}", r.unwrap_err());
            Ok(())
        })
        .unwrap();
}

#[test]
fn fixed_seeds_still_pass_through_schedule_checker() {
    // The legacy seed list, driven through the new checker with a canonical
    // partition schedule: keeps the 1..=80 corpus as a regression floor.
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let steps = [
        Step::Drop(50),
        Step::Run(40),
        Step::Put(1),
        Step::Partition(1, 3),
        Step::Partition(2, 3),
        Step::Run(20),
        Step::Heal,
        Step::Restart(3),
        Step::Run(40),
    ];
    for &seed in sim::PROPERTY_SEEDS {
        rt.block_on(sim::check_schedule(seed, &steps))
            .unwrap_or_else(|e| panic!("{e}"));
    }
}

#[test]
fn decode_random_bytes_never_panics() {
    runner(2048)
        .run(&prop::collection::vec(any::<u8>(), 0..512), |bytes| {
            let _ = rpc::decode(&bytes);
            let _ = decode_frame(&bytes);
            Ok(())
        })
        .unwrap();
}

#[test]
fn decode_rejects_oversize_and_hostile_lengths() {
    let mut frame = ((MAX_FRAME + 1) as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&[0; 8]);
    assert!(decode_frame(&frame).is_err());
    // Variant 5 (AppendEntries) followed by a huge Vec length must fail fast,
    // not try to allocate.
    let mut payload = 4u32.to_le_bytes().to_vec();
    payload.extend_from_slice(&[0u8; 8 * 4]);
    payload.extend_from_slice(&u64::MAX.to_le_bytes());
    assert!(rpc::decode(&payload).is_err());
}

#[test]
fn encode_decode_roundtrip_for_generated_messages() {
    let strat = (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>());
    runner(256)
        .run(&strat, |(term, id, li, lt)| {
            let msg = rpc::Message::RequestVote {
                term,
                candidate_id: id,
                last_log_index: li,
                last_log_term: lt,
            };
            let bytes = rpc::encode(&msg).unwrap();
            let back = decode_frame(&bytes).unwrap();
            prop_assert_eq!(format!("{back:?}"), format!("{msg:?}"));
            Ok(())
        })
        .unwrap();
}
