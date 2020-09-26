# RSMQ in async Rust

RSMQ port to async rust. RSMQ is a simple redis queue system that works in any redis v2.6+. It contains the same methods as the original one in https://github.com/smrchy/rsmq

This crate uses async in the implementation. If you want to use it in your sync code you can use tokio "block_on" method. Async was used in order to simplify the code and allow 1-to-1 port oft he JS code.

[![Crates.io](https://img.shields.io/crates/v/rsmq_async)](https://crates.io/crates/rsmq_async) [![Crates.io](https://img.shields.io/crates/l/rsmq_async)](https://choosealicense.com/licenses/mit/) [![dependency status](https://deps.rs/crate/rsmq_async/2.1.0/status.svg)](https://deps.rs/crate/rsmq_async)

## Installation

Check [https://crates.io/crates/rsmq_async](https://crates.io/crates/rsmq_async)

## Async executor

Since version 0.16 [where this pull request was merged](https://github.com/mitsuhiko/redis-rs/issues/280) redis dependency supports tokio and async_std executors. By default it will guess what you are using when creating the connection. You can check [redis](https://github.com/mitsuhiko/redis-rs/blob/master/Cargo.toml) `Cargo.tolm` for the flags `async-std-comp` and `tokio-comp` in order to fice one or the other.


## Example

```rust
use rsmq_async::Rsmq;

#[tokio::main]
async fn main() {
    let mut rsmq = Rsmq::<String>::new(Default::default())
        .await
        .expect("connection failed");

    rsmq.create_queue("myqueue", None, None, None)
        .await
        .expect("failed to create queue");

    rsmq.send_message("myqueue", &"testmessage".to_string(), None)
        .await
        .expect("failed to send message");

    let message = rsmq
        .receive_message("myqueue", None)
        .await
        .expect("cannot receive message");

    if let Some(message) = message {
        rsmq.delete_message("myqueue", &message.id).await;
    }
}
```
