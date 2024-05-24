use crate::functions::RsmqFunctions;
use crate::r#trait::RsmqConnection;
use crate::types::RedisBytes;
use crate::types::{RsmqMessage, RsmqOptions, RsmqQueueAttributes};
use crate::RsmqResult;
use core::convert::TryFrom;
use redis::RedisError;
use std::marker::PhantomData;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: redis::Client,
}

impl RedisConnectionManager {
    pub fn from_client(client: redis::Client) -> Result<RedisConnectionManager, RedisError> {
        Ok(RedisConnectionManager { client })
    }
}

impl r2d2::ManageConnection for RedisConnectionManager {
    type Connection = redis::Connection;
    type Error = RedisError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_connection()
    }

    fn is_valid(&self, conn: &mut redis::Connection) -> Result<(), Self::Error> {
        redis::cmd("PING").query(conn)
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Default)]
pub struct PoolOptions {
    pub max_size: Option<u32>,
    pub min_idle: Option<u32>,
}

pub struct PooledRsmq {
    pool: r2d2::Pool<RedisConnectionManager>,
    functions: RsmqFunctions<redis::Connection>,
}

impl Clone for PooledRsmq {
    fn clone(&self) -> Self {
        PooledRsmq {
            pool: self.pool.clone(),
            functions: RsmqFunctions {
                ns: self.functions.ns.clone(),
                realtime: self.functions.realtime,
                conn: PhantomData,
            },
        }
    }
}

impl PooledRsmq {
    pub fn new(options: RsmqOptions, pool_options: PoolOptions) -> RsmqResult<PooledRsmq> {
        let conn_info = redis::ConnectionInfo {
            addr: redis::ConnectionAddr::Tcp(options.host, options.port),
            redis: redis::RedisConnectionInfo {
                db: options.db.into(),
                username: options.username,
                password: options.password,
            },
        };

        let client = redis::Client::open(conn_info)?;

        let manager = RedisConnectionManager::from_client(client)?;
        let builder = r2d2::Pool::builder();

        let mut builder = if let Some(value) = pool_options.max_size {
            builder.max_size(value)
        } else {
            builder
        };

        builder = builder.min_idle(pool_options.min_idle);

        let pool = builder.build(manager)?;

        Ok(PooledRsmq {
            pool,
            functions: RsmqFunctions {
                ns: options.ns.clone(),
                realtime: options.realtime,
                conn: PhantomData,
            },
        })
    }

    pub fn new_with_pool(
        pool: r2d2::Pool<RedisConnectionManager>,
        realtime: bool,
        ns: Option<&str>,
    ) -> PooledRsmq {
        PooledRsmq {
            pool,
            functions: RsmqFunctions {
                ns: ns.unwrap_or("rsmq").to_string(),
                realtime,
                conn: PhantomData,
            },
        }
    }
}

impl RsmqConnection for PooledRsmq {
    fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        hidden: Duration,
    ) -> RsmqResult<()> {
        let mut conn = self.pool.get()?;

        self.functions
            .change_message_visibility(&mut conn, qname, message_id, hidden)
    }

    fn create_queue(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i32>,
    ) -> RsmqResult<()> {
        let mut conn = self.pool.get()?;

        self.functions
            .create_queue(&mut conn, qname, hidden, delay, maxsize)
    }

    fn delete_message(&mut self, qname: &str, id: &str) -> RsmqResult<bool> {
        let mut conn = self.pool.get()?;

        self.functions.delete_message(&mut conn, qname, id)
    }
    fn delete_queue(&mut self, qname: &str) -> RsmqResult<()> {
        let mut conn = self.pool.get()?;

        self.functions.delete_queue(&mut conn, qname)
    }
    fn get_queue_attributes(&mut self, qname: &str) -> RsmqResult<RsmqQueueAttributes> {
        let mut conn = self.pool.get()?;

        self.functions.get_queue_attributes(&mut conn, qname)
    }

    fn list_queues(&mut self) -> RsmqResult<Vec<String>> {
        let mut conn = self.pool.get()?;

        self.functions.list_queues(&mut conn)
    }

    fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
    ) -> RsmqResult<Option<RsmqMessage<E>>> {
        let mut conn = self.pool.get()?;

        self.functions.pop_message::<E>(&mut conn, qname)
    }

    fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> RsmqResult<Option<RsmqMessage<E>>> {
        let mut conn = self.pool.get()?;

        self.functions
            .receive_message::<E>(&mut conn, qname, hidden)
    }

    fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        messages: Vec<E>,
        delay: Option<Duration>,
    ) -> RsmqResult<Vec<String>> {
        let mut conn = self.pool.get()?;

        self.functions
            .send_message(&mut conn, qname, messages, delay)
    }

    fn set_queue_attributes(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RsmqResult<RsmqQueueAttributes> {
        let mut conn = self.pool.get()?;

        self.functions
            .set_queue_attributes(&mut conn, qname, hidden, delay, maxsize)
    }
}
