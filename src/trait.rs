use crate::types::RedisBytes;
use crate::types::{RsmqMessage, RsmqQueueAttributes};
use crate::RsmqResult;
use core::convert::TryFrom;
use std::time::Duration;

pub trait RsmqConnection {
    /// Change the hidden time of a already sent message.
    ///
    /// `hidden` has a max time of 9_999_999 for compatibility reasons to this library JS version counterpart
    fn change_message_visibility(
        &mut self,
        qname: &str,
        message_id: &str,
        hidden: Duration,
    ) -> RsmqResult<()>;

    /// Creates a new queue. Attributes can be later modified with "set_queue_attributes" method
    ///
    /// hidden: Time the messages will be hidden when they are received with the "receive_message" method. It
    /// has a max time of 9_999_999 for compatibility reasons to this library JS version counterpart
    ///
    /// delay: Time the messages will be delayed before being delivered
    ///
    /// maxsize: Maximum size in bytes of each message in the queue. Needs to be between 1024 or 65536 or -1 (unlimited
    /// size)
    fn create_queue(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i32>,
    ) -> RsmqResult<()>;

    /// Deletes a message from the queue.
    ///
    /// Important to use when you are using receive_message.
    fn delete_message(&mut self, qname: &str, id: &str) -> RsmqResult<bool>;

    /// Deletes the queue and all the messages on it
    fn delete_queue(&mut self, qname: &str) -> RsmqResult<()>;

    /// Returns the queue attributes and statistics
    fn get_queue_attributes(&mut self, qname: &str) -> RsmqResult<RsmqQueueAttributes>;

    /// Returns a list of queues in the namespace
    fn list_queues(&mut self) -> RsmqResult<Vec<String>>;

    /// Deletes and returns a message. Be aware that using this you may end with deleted & unprocessed messages.
    fn pop_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
    ) -> RsmqResult<Option<RsmqMessage<E>>>;

    /// Returns a message. The message stays hidden for some time (defined by "hidden" argument or the queue
    /// settings). After that time, the message will be redelivered. In order to avoid the redelivery, you need to use
    /// the "delete_message" after this function.
    ///
    /// `hidden` has a max time of 9_999_999 for compatibility reasons to this library JS version counterpart.
    fn receive_message<E: TryFrom<RedisBytes, Error = Vec<u8>>>(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
    ) -> RsmqResult<Option<RsmqMessage<E>>>;

    /// Sends a message to the queue. The message will be delayed some time (controlled by the "delayed" argument or
    /// the queue settings) before being delivered to a client.
    fn send_message<E: Into<RedisBytes> + Send>(
        &mut self,
        qname: &str,
        messages: Vec<E>,
        delay: Option<Duration>,
    ) -> RsmqResult<Vec<String>>;

    /// Modify the queue attributes. Keep in mind that "hidden" and "delay" can be overwritten when the message
    /// is sent. "hidden" can be changed by the method "change_message_visibility"
    ///
    /// hidden: Time the messages will be hidden when they are received with the "receive_message" method. It
    /// has a max time of 9_999_999 for compatibility reasons to this library JS version counterpart
    ///
    /// delay: Time the messages will be delayed before being delivered
    ///
    /// maxsize: Maximum size in bytes of each message in the queue. Needs to be between 1024 or 65536 or -1 (unlimited
    /// size)
    fn set_queue_attributes(
        &mut self,
        qname: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> RsmqResult<RsmqQueueAttributes>;
}
