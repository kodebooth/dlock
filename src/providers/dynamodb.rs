// SPDX-FileCopyrightText: 2025 Abe Kohandel <abe@kodebooth.com>
// SPDX-License-Identifier: MIT

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use aws_sdk_dynamodb::{
    Client,
    error::SdkError,
    operation::{
        delete_item::DeleteItemError,
        put_item::{PutItemError, PutItemOutput},
        update_item::{UpdateItemError, UpdateItemOutput},
    },
    types::{AttributeValue, ReturnValue, ReturnValuesOnConditionCheckFailure},
};
use bon::Builder;
use serde::{Deserialize, Serialize};
use serde_dynamo::{aws_sdk_dynamodb_1::from_item, to_item};
use tracing::debug;
use uuid::Uuid;

use crate::{
    error::DLockError,
    providers::{Lease, Provider},
};

/// [DynamoDB](aws_sdk_dynamodb) provider for [DLock](crate::DLock) implementation
#[derive(Builder, Debug, Clone)]
pub struct DynamodbProvider {
    client: Arc<Client>,
    table_name: String,
}

impl DynamodbProvider {
    pub const NAME_ATTRIBUTE: &str = "lock_name";
    pub const LEASE_ATTRIBUTE: &str = "lease";
    pub const TOKEN_ATTRIBUTE: &str = "token";

    async fn acquire_non_existing(
        &self,
        name: &str,
        owner: &str,
        duration: &Duration,
    ) -> Result<DynamodbLease, DLockError<DynamodbRetry>> {
        let lock = DynamodbLockItem {
            lease: Uuid::new_v4(),
            owner: owner.to_string(),
            duration: *duration,
            name: name.to_string(),
            token: 0,
        };

        let item = to_item(lock.clone()).unwrap();
        self.client
            .put_item()
            .table_name(self.table_name.clone())
            .set_item(Some(item))
            .set_return_values_on_condition_check_failure(Some(
                ReturnValuesOnConditionCheckFailure::AllOld,
            ))
            .condition_expression(format!("attribute_not_exists({})", Self::NAME_ATTRIBUTE))
            .send()
            .await
            .map_err(|sdk_error| match &sdk_error {
                SdkError::ServiceError(e) => match e.err() {
                    PutItemError::ConditionalCheckFailedException(e) => {
                        debug!("Acquiring non-existing lock failed {:?}", lock);
                        let item: DynamodbLockItem = e.item().unwrap().into();

                        DLockError::AlreadyAcquired(DynamodbRetry {
                            lease: item.lease,
                            duration: item.duration,
                            start: Instant::now(),
                        })
                    }
                    _ => DLockError::ProviderError(sdk_error.into()),
                },
                _ => DLockError::ProviderError(sdk_error.into()),
            })?;
        Ok(DynamodbLease {
            item: lock,
            client: Arc::clone(&self.client),
            table: self.table_name.clone(),
        })
    }

    async fn acquire_dead_lease(
        &self,
        name: &str,
        owner: &str,
        duration: &Duration,
        retry: DynamodbRetry,
    ) -> Result<DynamodbLease, DLockError<DynamodbRetry>> {
        let lock = DynamodbLockItem {
            lease: Uuid::new_v4(),
            duration: *duration,
            name: name.to_string(),
            owner: owner.to_string(),
            token: 0,
        };
        let item = self
            .client
            .update_item()
            .table_name(self.table_name.clone())
            .key(
                DynamodbProvider::NAME_ATTRIBUTE,
                AttributeValue::S(lock.name.to_string()),
            )
            .condition_expression("#lease = :prev_lease")
            .update_expression("SET #lease = :new_lease, #token = #token + :one")
            .expression_attribute_names("#lease", DynamodbProvider::LEASE_ATTRIBUTE)
            .expression_attribute_names("#token", DynamodbProvider::TOKEN_ATTRIBUTE)
            .expression_attribute_values(":prev_lease", AttributeValue::S(retry.lease.to_string()))
            .expression_attribute_values(":new_lease", AttributeValue::S(lock.lease.to_string()))
            .expression_attribute_values(":one", AttributeValue::N(1.to_string()))
            .return_values(ReturnValue::AllNew)
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .send()
            .await
            .map_err(|sdk_error| match &sdk_error {
                SdkError::ServiceError(e) => match e.err() {
                    UpdateItemError::ConditionalCheckFailedException(_) => {
                        DLockError::AlreadyReleased
                    }
                    _ => DLockError::ProviderError(sdk_error.into()),
                },
                _ => DLockError::ProviderError(sdk_error.into()),
            })?
            .into();

        Ok(DynamodbLease {
            item,
            client: Arc::clone(&self.client),
            table: self.table_name.clone(),
        })
    }
}

impl Provider for DynamodbProvider {
    type T = u64;
    type L = DynamodbLease;
    type R = DynamodbRetry;

    async fn acquire(
        &self,
        name: &str,
        owner: &str,
        duration: &Duration,
        retry: Option<DynamodbRetry>,
    ) -> Result<Self::L, DLockError<Self::R>> {
        if let Some(retry) = retry {
            if retry.start.elapsed() < retry.duration {
                // Too early to retry, last duration read for the given lease has not expired yet
                self.acquire_non_existing(name, owner, duration)
                    .await
                    .map_err(|e| match e {
                        DLockError::AlreadyAcquired(_) => DLockError::AlreadyAcquired(retry),
                        _ => e,
                    })
            } else {
                // Duration has passed:
                // 1. if the previous lease has not been updated the lock owner is dead so take over the lock
                // 2. if the previous lease has been updated, someone renewed their lease so start over
                self.acquire_dead_lease(name, owner, duration, retry).await
            }
        } else {
            // No retry context, the lock can only be acquired if it doesn't exist
            self.acquire_non_existing(name, owner, duration).await
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct DynamodbLockItem {
    #[serde(rename(serialize = "lock_name", deserialize = "lock_name"))]
    name: String,
    owner: String,
    lease: Uuid,
    duration: Duration,
    token: u64,
}

#[derive(Debug, Clone)]
pub struct DynamodbLease {
    item: DynamodbLockItem,
    client: Arc<Client>,
    table: String,
}

impl Lease<Self, u64> for DynamodbLease {
    async fn release(&self) -> Result<(), DLockError> {
        self.client
            .delete_item()
            .table_name(self.table.to_string())
            .key(
                DynamodbProvider::NAME_ATTRIBUTE,
                AttributeValue::S(self.item.name.to_string()),
            )
            .condition_expression("#lease = :lease")
            .expression_attribute_names("#lease", DynamodbProvider::LEASE_ATTRIBUTE)
            .expression_attribute_values(":lease", AttributeValue::S(self.item.lease.to_string()))
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .send()
            .await
            .map_err(|sdk_error| match &sdk_error {
                SdkError::ServiceError(e) => match e.err() {
                    DeleteItemError::ConditionalCheckFailedException(_) => {
                        DLockError::AlreadyReleased
                    }
                    _ => DLockError::ProviderError(sdk_error.into()),
                },
                _ => DLockError::ProviderError(sdk_error.into()),
            })?;
        Ok(())
    }

    async fn renew(&self) -> Result<Self, DLockError> {
        let new_lease = Uuid::new_v4();
        let item = self
            .client
            .update_item()
            .table_name(self.table.to_string())
            .key(
                DynamodbProvider::NAME_ATTRIBUTE,
                AttributeValue::S(self.item.name.to_string()),
            )
            .condition_expression("#lease = :prev_lease")
            .update_expression("SET #lease = :new_lease")
            .expression_attribute_names("#lease", DynamodbProvider::LEASE_ATTRIBUTE)
            .expression_attribute_values(
                ":prev_lease",
                AttributeValue::S(self.item.lease.to_string()),
            )
            .expression_attribute_values(":new_lease", AttributeValue::S(new_lease.to_string()))
            .return_values(ReturnValue::AllNew)
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .send()
            .await
            .map_err(|sdk_error| match &sdk_error {
                SdkError::ServiceError(e) => match e.err() {
                    UpdateItemError::ConditionalCheckFailedException(_) => {
                        DLockError::AlreadyReleased
                    }
                    _ => DLockError::ProviderError(sdk_error.into()),
                },
                _ => DLockError::ProviderError(sdk_error.into()),
            })?
            .into();

        Ok(DynamodbLease {
            item,
            client: Arc::clone(&self.client),
            table: self.table.clone(),
        })
    }

    fn token(&self) -> u64 {
        self.item.token
    }
}

#[derive(Debug, Clone)]
pub struct DynamodbRetry {
    lease: Uuid,
    duration: Duration,
    start: Instant,
}

impl From<PutItemOutput> for DynamodbLockItem {
    fn from(value: PutItemOutput) -> Self {
        from_item(value.attributes().unwrap().clone()).unwrap()
    }
}

impl From<UpdateItemOutput> for DynamodbLockItem {
    fn from(value: UpdateItemOutput) -> Self {
        from_item(value.attributes().unwrap().clone()).unwrap()
    }
}

impl From<&HashMap<String, AttributeValue>> for DynamodbLockItem {
    fn from(value: &HashMap<String, AttributeValue>) -> Self {
        from_item(value.to_owned()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use aws_config::Region;
    use aws_sdk_dynamodb::{
        Client,
        config::Credentials,
        types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType},
    };
    use testcontainers_modules::{
        dynamodb_local::DynamoDb,
        testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
    };

    use super::*;

    const TABLE_NAME: &str = "table_name";
    async fn setup() -> (ContainerAsync<DynamoDb>, Client) {
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
        (db, client)
    }

    #[tokio::test]
    async fn acquire_fresh_lock() {
        let (_db, client) = setup().await;

        let provider = DynamodbProvider::builder()
            .client(Arc::new(client))
            .table_name(TABLE_NAME.to_string())
            .build();

        let lease = provider
            .acquire("test_lock", "owner", &Duration::from_secs(5), None)
            .await
            .expect("should be able to acquire lock");

        assert_eq!(lease.item.duration, Duration::from_secs(5));
        assert_eq!(lease.item.name, "test_lock");
    }

    #[tokio::test]
    async fn renew_lock() {
        let (_db, client) = setup().await;

        let provider = DynamodbProvider::builder()
            .client(Arc::new(client))
            .table_name(TABLE_NAME.to_string())
            .build();

        let lease = provider
            .acquire("test_lock", "owner", &Duration::from_secs(5), None)
            .await
            .expect("should be able to acquire lock");

        assert_eq!(lease.item.duration, Duration::from_secs(5));
        assert_eq!(lease.item.name, "test_lock");

        let new_lease = lease.renew().await.expect("renew should work");

        assert_eq!(lease.item.duration, new_lease.item.duration);
        assert_eq!(lease.item.name, new_lease.item.name);
        assert_eq!(lease.item.token, new_lease.item.token);
        assert!(lease.item.lease != new_lease.item.lease);
    }

    #[tokio::test]
    async fn reacquire_already_locked() {
        let (_db, client) = setup().await;

        let provider = DynamodbProvider::builder()
            .client(Arc::new(client))
            .table_name(TABLE_NAME.to_string())
            .build();

        let lock_name = "test_lock";
        let owner = "owner";
        let duration = Duration::from_hours(1);

        let lease = provider
            .acquire(lock_name, owner, &duration, None)
            .await
            .expect("should be able to acquire lock");

        assert_eq!(lease.item.duration, duration);
        assert_eq!(lease.item.name, lock_name);

        let result = provider
            .acquire(lock_name, owner, &duration, None)
            .await
            .err()
            .unwrap();

        match result {
            DLockError::AlreadyAcquired(retry) => {
                assert_eq!(retry.lease, lease.item.lease);
                assert!(retry.start <= Instant::now());
            }
            _ => panic!("unexpected error: {}", result),
        };
    }

    #[tokio::test]
    async fn reacquire_dropped_lock() {
        let (_db, client) = setup().await;

        let provider = DynamodbProvider::builder()
            .client(Arc::new(client))
            .table_name(TABLE_NAME.to_string())
            .build();

        let lock_name = "test_lock";
        let owner = "owner";
        let duration = Duration::from_hours(1);

        {
            let lease = provider
                .acquire(lock_name, owner, &duration, None)
                .await
                .expect("should be able to acquire lock");

            assert_eq!(lease.item.duration, duration);
            assert_eq!(lease.item.name, lock_name);

            lease.release().await.unwrap();
        }

        {
            let lease = provider
                .acquire(lock_name, owner, &duration, None)
                .await
                .expect("should be able to acquire lock");

            assert_eq!(lease.item.duration, duration);
            assert_eq!(lease.item.name, lock_name);
        }
    }
}
