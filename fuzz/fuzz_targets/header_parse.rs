#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    object_storage_client::encryption::fuzzing::header_parse(data);
});
