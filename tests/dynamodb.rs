// SPDX-FileCopyrightText: 2025 Abe Kohandel <abe@kodebooth.com>
// SPDX-License-Identifier: MIT

use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
    time::Duration,
};

use aws_config::Region;
use aws_sdk_dynamodb::{
    Client,
    config::Credentials,
    types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType},
};
use dlock::{
    DLock,
    error::DLockError::AlreadyAcquired,
    providers::{Lease, dynamodb::DynamodbProvider},
};
use rand::Rng;
use testcontainers_modules::{
    dynamodb_local::DynamoDb,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};
use tokio::{runtime::Handle, task::JoinSet, time::sleep};

const TABLE_NAME: &str = "table_name";

async fn setup() -> (ContainerAsync<DynamoDb>, DynamodbProvider) {
    let db = DynamoDb::default().with_tag("3.1.0").start().await.unwrap();

    let credentials = Credentials::for_tests();
    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version_latest()
        .endpoint_url(format!(
            "http://{}:{}",
            db.get_host().await.unwrap(),
            db.get_host_port_ipv4(8000).await.unwrap()
        ))
        .region(Region::new("test"))
        .credentials_provider(credentials)
        .build();
    let client = Client::from_conf(config);

    client
        .create_table()
        .billing_mode(BillingMode::PayPerRequest)
        .table_name(TABLE_NAME)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(DynamodbProvider::NAME_ATTRIBUTE)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("should be able to build partition key attribute"),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(DynamodbProvider::NAME_ATTRIBUTE)
                .key_type(KeyType::Hash)
                .build()
                .expect("should be able to build hash key"),
        )
        .send()
        .await
        .expect("should be able to create table");

    let provider = DynamodbProvider::builder()
        .client(Arc::new(client))
        .table_name(TABLE_NAME.to_string())
        .build();

    (db, provider)
}

#[tokio::test]
async fn single_lock() {
    let (_db, provider) = setup().await;
    let lock = DLock::builder()
        .name("test_lock".to_string())
        .owner("owner".to_string())
        .duration(Duration::from_secs(10))
        .provider(provider)
        .build();
    let _ = lock.acquire().await.expect("should acquire lock");
}

#[tokio::test]
async fn reject_already_locked() {
    let (_db, provider) = setup().await;
    let lock = DLock::builder()
        .name("test_lock".to_string())
        .owner("owner".to_string())
        .duration(Duration::from_secs(5))
        .provider(provider)
        .build();
    let _ = lock.acquire().await.expect("should acquire lock");
    lock.acquire().await.expect_err("should fail");
}

#[tokio::test]
async fn relock_released_lock() {
    let (_db, provider) = setup().await;
    let lock = DLock::builder()
        .name("test_lock".to_string())
        .owner("owner".to_string())
        .duration(Duration::from_secs(5))
        .provider(provider)
        .build();
    let lease = lock.acquire().await.expect("should acquire lock");
    lease.release().await.expect("should release lock");
    lock.acquire().await.expect("should reacquire lock");
}

#[tokio::test]
async fn recover_dead_lock() {
    let (_db, provider) = setup().await;

    let lock = DLock::builder()
        .name("test_lock".to_string())
        .owner("owner".to_string())
        .duration(Duration::from_secs(1))
        .provider(provider)
        .build();

    let lease = lock.acquire().await.expect("should acquire lock");
    assert_eq!(lease.token(), 0);

    let AlreadyAcquired(retry) = lock.acquire().await.expect_err("should not acquire lock") else {
        panic!("unexpected error");
    };

    sleep(Duration::from_secs(2)).await;

    let lease = lock.retry(retry).await.expect("should acquire lock");
    assert_eq!(lease.token(), 1);
}

#[tokio::test]
async fn holding_lock() {
    let (_db, provider) = setup().await;

    let lock = DLock::builder()
        .name("test_lock".to_string())
        .owner("owner".to_string())
        .duration(Duration::from_secs(1))
        .provider(provider)
        .build();

    let result = lock
        .with(async |token| {
            assert_eq!(token, 0);
            true
        })
        .await
        .expect("should do work while holding lock");

    assert_eq!(result, true);
}

#[tokio::test]
async fn holding_lock_back_to_back() {
    let (_db, provider) = setup().await;
    let lock = DLock::builder()
        .name("test_lock".to_string())
        .owner("owner".to_string())
        .duration(Duration::from_secs(1))
        .provider(provider)
        .build();

    let result = lock
        .with(async |token| {
            assert_eq!(token, 0);
            true
        })
        .await
        .expect("should do work while holding lock");
    assert_eq!(result, true);

    let result = lock
        .with(async |token| {
            assert_eq!(token, 0);
            true
        })
        .await
        .expect("should do work while holding lock");
    assert_eq!(result, true);
}

#[tokio::test]
async fn holding_lock_concurrently() {
    let (_db, provider) = setup().await;
    let lock = DLock::builder()
        .name("test_lock".to_string())
        .owner("owner".to_string())
        .duration(Duration::from_secs(1))
        .provider(provider)
        .build();

    let v = RefCell::new(0);

    let (first, second) = tokio::join!(
        lock.with(async |_| *v.borrow_mut() += 1),
        lock.with(async |_| *v.borrow_mut() += 1)
    );

    first.expect("first should complete without error");
    second.expect("second should complete without error");
    assert_eq!(*v.borrow(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1000)]
#[ignore]
async fn chaos() {
    let (_db, provider) = setup().await;
    let value = Arc::new(Mutex::new(0));
    let mut rng = rand::rng();
    let lease_duration = Duration::from_secs(5);

    let mut set = JoinSet::new();

    let workers = Handle::current().metrics().num_workers();
    for owner in 0..workers {
        let v = Arc::clone(&value);
        let d = Duration::from_micros(rng.random_range(0..1000));
        let l = DLock::builder()
            .name("test_lock".to_string())
            .duration(lease_duration)
            .provider(provider.clone())
            .owner(owner.to_string())
            .retry(Duration::from_millis(1))
            .build();
        set.spawn(async move {
            l.with(async move |_| {
                // Random sleep to add chaos
                sleep(d).await;
                // Increment a counter
                *v.try_lock()
                    .expect("exclusive access should be guaranteed by DLock") += 1;
            })
            .await
            .expect("should succeed");
        });
    }

    let _ = set.join_all().await;

    assert_eq!(*value.lock().expect("lock should be acquired"), workers);
}
