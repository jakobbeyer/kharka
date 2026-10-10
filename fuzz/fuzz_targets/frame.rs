//! The frame decoder: whatever bytes come over the stream, reading them ends in frames or an error, never a panic. A
//! frame that reads is sent again as it would be, and reads again; a hello's prefixes go through the hub's scope check.

#![no_main]

use std::sync::OnceLock;

use kharka::frame::{self, Frame};
use kharka::scope::Scope;
use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;
use tokio_util::codec::{FramedRead, FramedWrite};

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Builder::new_current_thread().build().unwrap())
}

fuzz_target!(|data: &[u8]| {
    runtime().block_on(async {
        let mut reader = FramedRead::new(data, frame::codec());
        while let Ok(Some(frame)) = frame::recv(&mut reader).await {
            if let Frame::Hello { prefixes, .. } = &frame {
                let _ = Scope::of(prefixes.clone());
            }
            let mut sent = Vec::new();
            frame::send(&mut FramedWrite::new(&mut sent, frame::codec()), &frame)
                .await
                .expect("a frame that was read can be sent");
            let again = frame::recv(&mut FramedRead::new(sent.as_slice(), frame::codec()))
                .await
                .expect("a frame that was sent can be read")
                .expect("one frame");
            assert_eq!(again.name(), frame.name());
        }
    });
});
