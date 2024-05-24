<!-- cargo-sync-readme start -->

# RSMQ in async Rust

RSMQ port to async rust. RSMQ is a simple redis queue system that works in any
redis v2.6+. It contains the same methods as the original one
in [https://github.com/smrchy/rsmq](https://github.com/smrchy/rsmq)

This crate uses async in the implementation. If you want to use it in your sync
code you can use tokio/async_std "block_on" method. Async was used in order to
simplify the code and allow 1-to-1 port oft he JS code.

[![Crates.io](https://img.shields.io/crates/v/rsmq_sync)](https://crates.io/crates/rsmq_sync)
[![Crates.io](https://img.shields.io/crates/l/rsmq_sync)](https://choosealicense.com/licenses/mit/)
[![dependency status](https://deps.rs/crate/rsmq_sync/latest/status.svg)](https://deps.rs/crate/rsmq_sync)
[![Docs](https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square)](https://docs.rs/rsmq_sync)

## Example

```rust,no_run

use rsmq_sync::{Rsmq, RsmqError, RsmqConnection};

let mut rsmq = Rsmq::new(Default::default()).await?;

let message = rsmq.receive_message::<String>("myqueue", None).await?;

if let Some(message) = message {
    rsmq.delete_message("myqueue", &message.id).await?;
}


```

Main object documentation are in: <a href="struct.Rsmq.html">Rsmq</a> and
<a href="struct.PooledRsmq.html">PooledRsmq</a> and they both implement the trait
<a href="trait.RsmqConnection.html">RsmqConnection</a> where you can see all the RSMQ
methods. Make sure you always import the trait <a href="trait.RsmqConnection.html">RsmqConnection</a>.

## Installation

Check [https://crates.io/crates/rsmq_sync](https://crates.io/crates/rsmq_sync)


## Example

```rust,no_run
use rsmq_sync::{Rsmq, RsmqConnection};

async fn it_works() {
    let mut rsmq = Rsmq::new(Default::default())
        .await
        .expect("connection failed");

    rsmq.create_queue("myqueue", None, None, None)
        .await
        .expect("failed to create queue");

    rsmq.send_message("myqueue", "testmessage", None)
        .await
        .expect("failed to send message");

    let message = rsmq
        .receive_message::<String>("myqueue", None)
        .await
        .expect("cannot receive message");

    if let Some(message) = message {
        rsmq.delete_message("myqueue", &message.id).await;
    }
}

```

## Realtime

When [initializing](#initialize) RSMQ you can enable the realtime PUBLISH for
new messages. On every new message that gets sent to RSQM via `sendMessage` a
Redis PUBLISH will be issued to `{rsmq.ns}:rt:{qname}`. So, you can subscribe
to it using redis-rs library directly.

### How to use the realtime option

Besides the PUBLISH when a new message is sent to RSMQ nothing else will happen.
Your app could use the Redis SUBSCRIBE command to be notified of new messages
and issue a `receiveMessage` then. However make sure not to listen with multiple
workers for new messages with SUBSCRIBE to prevent multiple simultaneous
`receiveMessage` calls.

## Guarantees

If you want to implement "at least one delivery" guarantee, you need to receive
the messages using "receive_message" and then, once the message is successfully
processed, delete it with "delete_message".

## Connection Pool

If you want to use a connection pool, just use <a href="struct.PooledRsmq.html">PooledRsmq</a>
instad of Rsmq. It implements the RsmqConnection trait as the normal Rsmq.

If you want to accept any of both implementation, just accept the trait
<a href="trait.RsmqConnection.html">RsmqConnection</a>

## Response types

There are 3 functions that take generic types:

- `pop_message` and `receive_message`: Where the type for the received message is
`RsmqMessage<E>` where `E: TryFrom<RedisBytes, Error = Vec<u8>>`. So, If you have custom type, you can implement the trait
`TryFrom<RedisBytes>` for `YourCustomType` and use it like: `rsmq.receive_message::<YourCustomType>("myqueue", None)`.
Implementations are provided for `String` and `Vec<u8>`.
- `send_message` where the message to send needs to implement `Into<RedisBytes> + Send`. So you will
need to implement the trait for your type. You can check the implementations for the type RedisBytes and see how
we did it. Implementations are provided for `String`, `&str` and `Vec<u8>`.

All this is because strings in Rust are very convenient to use for json messages, so always returning a Vec<u8>
may not be the most ergonomic solution. But at the same time, we can just add some already made implementations
for it and you can just use it with your type or, if you are sending, let's say, images, just use the method
like: `rsmq.receive_message::<Vec<u8>>("myqueue", None)` and transform it later to your type. (Or just implement
the TryFrom<RedisBytes> for your type and the transformation will be automatic.)

### Example for implementing a custom type

```rust,ignore
impl TryFrom<RedisBytes> for String {

    // We sacrifice the ability of recovering the original error for the ability of having the
    // original data. If you know how to conserver both, let me know!
    
    type Error = Vec<u8>; // Always set Error as Vec<u8>;

    fn try_from(bytes: RedisBytes) -> Result<Self, Self::Error> {
        String::from_utf8(bytes.0).map_err(|e| e.into_bytes())
    }
}
```


<!-- cargo-sync-readme end -->
