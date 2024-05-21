use crate::functions::RsmqFunctions;
use crate::r#trait::RsmqConnection;
use crate::types::{RedisBytes, RsmqMessage, RsmqOptions, RsmqQueueAttributes};
use crate::RsmqResult;
use core::convert::TryFrom;
use core::marker::PhantomData;
use std::time::Duration;


#[derive(Clone)]
pub struct RedisConnection(pub redis::Client);

impl std::fmt::Debug for RedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RedisConnection")
    }
}

#[derive(Debug, Clone)]
pub struct Rsmq {
    connection: RedisConnection,
    functions: RsmqFunctions<redis::Client>,
}

impl Rsmq {
    /// Creates a new RSMQ instance, including its connection
    pub fn new(options: RsmqOptions) -> RsmqResult<Rsmq> {
        let conn_info = redis::ConnectionInfo {
            addr: redis::ConnectionAddr::Tcp(options.host, options.port),
            redis: redis::RedisConnectionInfo {
                db: options.db.into(),
                username: options.username,
                password: options.password,
            },
        };

        let client = redis::Client::open(conn_info)?;


        Ok(Rsmq::new_with_client(
            client,
            options.realtime,
            Some(&options.ns),
        ))
    }

    /// Special method for when you already have a redis-rs connection and you don't want redis_async to create a new one.
    pub fn new_with_client(
        connection: redis::Client,
        realtime: bool,
        ns: Option<&str>,
    ) -> Rsmq {
        Rsmq {
            connection: RedisConnection(connection),
            functions: RsmqFunctions {
                ns: ns.unwrap_or("rsmq").to_string(),
                realtime,
                conn: PhantomData,
            },
        }
    }
}

impl RsmqConnection for Rsmq {
    fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        hidden: Duration,
    ) -> RsmqResult<()> {
        self.functions
            .change_message_visibility(&mut self.connection.0, qname, message_id, hidden)
    }

    fn create_queue(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i32>,
    ) -> RsmqResult<()> {
        self.functions
            .create_queue(&mut self.connection.0, qname, hidden, delay, maxsize)
    }

    fn delete_message(&mut self, qname: &str, id: &str) -> RsmqResult<bool> {
        self.functions
            .delete_message(&mut self.connection.0, qname, id)
    }
    fn delete_queue(&mut self, qname: &str) -> RsmqResult<()> {
        self.functions
            .delete_queue(&mut self.connection.0, qname)
    }
    fn get_queue_attributes(&mut self, qname: &str) -> RsmqResult<RsmqQueueAttributes> {
        self.functions
            .get_queue_attributes(&mut self.connection.0, qname)
    }

    fn list_queues(&mut self) -> RsmqResult<Vec<String>> {
        self.functions.list_queues(&mut self.connection.0)
    }

    fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
    ) -> RsmqResult<Option<RsmqMessage<E>>> {
        self.functions
            .pop_message::<E>(&mut self.connection.0, qname)
    }

    fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> RsmqResult<Option<RsmqMessage<E>>> {
        self.functions
            .receive_message::<E>(&mut self.connection.0, qname, hidden)
    }

    fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        message: E,
        delay: Option<Duration>,
    ) -> RsmqResult<String> {
        self.functions
            .send_message(&mut self.connection.0, qname, message, delay)
    }

    fn set_queue_attributes(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RsmqResult<RsmqQueueAttributes> {
        self.functions
            .set_queue_attributes(&mut self.connection.0, qname, hidden, delay, maxsize)
    }
}
