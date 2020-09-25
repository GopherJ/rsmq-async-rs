use rsmq_async::{Rsmq, RsmqError, RsmqOptions};
use tokio::time::delay_for;

use std::time::Duration;

#[tokio::test]
async fn send_receiving_deleting_message() {
    let mut rsmq = Rsmq::new(RsmqOptions {
        db: 0,
        ..Default::default()
    })
    .await
    .unwrap();

    rsmq.create_queue("queue1", None, None, None).await.unwrap();

    rsmq.send_message("queue1", "testmessage", None)
        .await
        .unwrap();

    let message = rsmq.receive_message("queue1", None).await.unwrap();
    assert!(message.is_some());

    let message = message.unwrap();

    rsmq.delete_message("queue1", &message.id).await.unwrap();

    assert_eq!(message.message, "testmessage".to_string());

    let message = rsmq.receive_message("queue1", None).await.unwrap();

    assert!(message.is_none());
    rsmq.delete_queue("queue1").await.unwrap();
}

#[tokio::test]
async fn pop_message() {
    let mut rsmq = Rsmq::new(RsmqOptions {
        db: 1,
        ..Default::default()
    })
    .await
    .unwrap();

    rsmq.create_queue("queue2", None, None, None).await.unwrap();

    rsmq.send_message("queue2", "testmessage", None)
        .await
        .unwrap();

    let message = rsmq.pop_message("queue2").await.unwrap();

    assert!(message.is_some());

    let message = message.unwrap();

    assert_eq!(message.message, "testmessage");

    let message = rsmq.pop_message("queue2").await.unwrap();

    assert!(message.is_none());

    rsmq.delete_queue("queue2").await.unwrap();
}

#[tokio::test]
async fn creating_queue() {
    let mut rsmq = Rsmq::new(RsmqOptions {
        db: 2,
        ..Default::default()
    })
    .await
    .unwrap();

    rsmq.create_queue("queue3", None, None, None).await.unwrap();

    let queues = rsmq.list_queues().await.unwrap();

    assert_eq!(queues, vec!("queue3"));

    let result = rsmq.create_queue("queue3", None, None, None).await;

    assert!(result.is_err());

    if let Err(RsmqError::QueueExists) = result {
        rsmq.delete_queue("queue3").await.unwrap();
        return;
    } else {
        panic!()
    }
}

#[tokio::test]
async fn updating_queue() {
    let mut rsmq = Rsmq::new(RsmqOptions {
        db: 3,
        ..Default::default()
    })
    .await
    .unwrap();

    rsmq.create_queue("queue4", None, None, None).await.unwrap();

    let attributes = rsmq.get_queue_attributes("queue4").await.unwrap();

    assert_eq!(attributes.vt, 30);
    assert_eq!(attributes.delay, 0);
    assert_eq!(attributes.maxsize, 65536);
    assert_eq!(attributes.totalrecv, 0);
    assert_eq!(attributes.totalsent, 0);
    assert_eq!(attributes.msgs, 0);
    assert_eq!(attributes.hiddenmsgs, 0);
    assert!(attributes.created > 0);
    assert!(attributes.modified > 0);

    rsmq.set_queue_attributes("queue4", Some(45), Some(5), Some(2048))
        .await
        .unwrap();

    let attributes = rsmq.get_queue_attributes("queue4").await.unwrap();

    assert_eq!(attributes.vt, 45);
    assert_eq!(attributes.delay, 5);
    assert_eq!(attributes.maxsize, 2048);
    assert_eq!(attributes.totalrecv, 0);
    assert_eq!(attributes.totalsent, 0);
    assert_eq!(attributes.msgs, 0);
    assert_eq!(attributes.hiddenmsgs, 0);
    assert!(attributes.created > 0);
    assert!(attributes.modified > 0);

    rsmq.delete_queue("queue4").await.unwrap();
}

#[tokio::test]
async fn deleting_queue() {
    let mut rsmq = Rsmq::new(RsmqOptions {
        db: 4,
        ..Default::default()
    })
    .await
    .unwrap();

    rsmq.create_queue("queue5", None, None, None).await.unwrap();

    let queues = rsmq.list_queues().await.unwrap();

    assert_eq!(queues, vec!("queue5"));

    rsmq.delete_queue("queue5").await.unwrap();

    let queues = rsmq.list_queues().await.unwrap();

    assert_eq!(queues, Vec::<String>::new());

    let result = rsmq.delete_queue("queue5").await;

    assert!(result.is_err());

    if let Err(RsmqError::QueueNotFound) = result {
    } else {
        panic!()
    }

    let result = rsmq.get_queue_attributes("queue5").await;

    assert!(result.is_err());

    if let Err(RsmqError::QueueNotFound) = result {
    } else {
        panic!()
    }

    let result = rsmq
        .set_queue_attributes("queue5", Some(45), Some(5), Some(2048))
        .await;

    assert!(result.is_err());

    if let Err(RsmqError::QueueNotFound) = result {
        return;
    } else {
        panic!()
    }
}

#[tokio::test]
async fn change_message_visibility() {
    let mut rsmq = Rsmq::new(RsmqOptions {
        db: 5,
        ..Default::default()
    })
    .await
    .unwrap();

    rsmq.create_queue("queue6", None, None, None).await.unwrap();

    rsmq.send_message("queue6", "testmessage", None)
        .await
        .unwrap();

    let message = rsmq.receive_message("queue6", None).await.unwrap();
    assert!(message.is_some());

    let message_id = message.unwrap().id;

    let message = rsmq.receive_message("queue6", None).await.unwrap();
    assert!(message.is_none());

    rsmq.change_message_visibility("queue6", &message_id, 0)
        .await
        .unwrap();

    let ten_millis = std::time::Duration::from_millis(10);
    std::thread::sleep(ten_millis);

    let message = rsmq.receive_message("queue6", None).await.unwrap();
    assert!(message.is_some());

    assert_eq!(message_id, message.unwrap().id);

    rsmq.delete_queue("queue6").await.unwrap();
}

#[tokio::test]
async fn send_receiving_hidden_30s_message() {
    let mut rsmq = Rsmq::new(RsmqOptions {
        db: 6,
        ..Default::default()
    })
    .await
    .unwrap();

    rsmq.create_queue("queue7", None, None, Some(-1))
        .await
        .unwrap();

    rsmq.send_message("queue7", "testmessage", None)
        .await
        .unwrap();

    let message = rsmq.receive_message("queue7", None).await.unwrap();
    assert!(message.is_some());

    delay_for(Duration::from_secs(29)).await;
    let message = rsmq.receive_message("queue7", None).await.unwrap();
    assert!(message.is_none());

    delay_for(Duration::from_secs(2)).await;
    let message = rsmq.receive_message("queue7", None).await.unwrap();
    assert!(message.is_some());

    rsmq.delete_queue("queue7").await.unwrap();
}

#[tokio::test]
async fn send_receiving_hidden_15s_message() {
    let mut rsmq = Rsmq::new(RsmqOptions {
        db: 7,
        ..Default::default()
    })
    .await
    .unwrap();

    rsmq.create_queue("queue8", Some(15), None, Some(-1))
        .await
        .unwrap();

    rsmq.send_message("queue8", "testmessage", None)
        .await
        .unwrap();

    let message = rsmq.receive_message("queue8", None).await.unwrap();
    assert!(message.is_some());

    delay_for(Duration::from_secs(14)).await;
    let message = rsmq.receive_message("queue8", None).await.unwrap();
    assert!(message.is_none());

    delay_for(Duration::from_secs(2)).await;
    let message = rsmq.receive_message("queue8", None).await.unwrap();
    assert!(message.is_some());

    rsmq.delete_queue("queue8").await.unwrap();
}
