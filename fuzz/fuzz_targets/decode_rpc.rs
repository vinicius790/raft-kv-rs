#![no_main]
//! Raw bincode payload decoder (bincode 1, limited to MAX_FRAME).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(msg) = raft_kv_rs::rpc::decode(data) {
        // Anything that decodes must re-encode.
        let _ = raft_kv_rs::rpc::encode(&msg).expect("re-encode");
    }
});
