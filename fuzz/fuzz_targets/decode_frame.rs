#![no_main]
//! `[u32 len][bincode]` frame decoder: no panic, no allocation past MAX_FRAME.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = raft_kv_rs::rpc::decode_frame(data);
});
