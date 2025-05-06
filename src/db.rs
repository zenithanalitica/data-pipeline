use backoff::{Error as BackoffError, ExponentialBackoff};
use futures::future;
use neo4rs::{self, Graph, query};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio;
use tokio::sync::Semaphore;

use crate::json;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Credentials {
    uri: String,
    user: String,
    password: String,
}

impl ::std::default::Default for Credentials {
    fn default() -> Self {
        Self {
            uri: String::from("localhost:7676"),
            user: String::from("neo4j"),
            password: String::from("neo4j"),
        }
    }
}

pub async fn prepare_database(creds: Credentials) -> Result<(), neo4rs::Error> {
    let graph = Graph::new(creds.uri, creds.user, creds.password)
        .await
        .unwrap();

    let mut txn = graph.start_txn().await?;
    // Run this BEFORE starting any imports to ensure uniqueness of users
    txn.run(query(
        "
            CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE;
            ",
    ))
    .await
    .unwrap();

    txn.run(query(
        "
            CREATE CONSTRAINT IF NOT EXISTS FOR (t:Tweet) REQUIRE t.id IS UNIQUE;
            ",
    ))
    .await
    .unwrap();

    txn.commit().await?;

    // Wait a moment for the constraint to be fully applied
    println!("Waiting for the constraint to be applied...");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    Ok(())
}

pub async fn insert_new_tweets(creds: Credentials, tweets: Vec<json::Tweet>) {
    let graph = Graph::new(creds.uri, creds.user, creds.password)
        .await
        .unwrap();

    let batch_size = 500; // How many nodes per transaction
    let max_concurrent_batches = 8; // Limit concurrent transactions

    // Create semaphore for concurrent control
    let semaphore = Arc::new(Semaphore::new(max_concurrent_batches));
    let mut handles = Vec::new();

    for (batch_idx, chunk) in tweets.chunks(batch_size).enumerate() {
        let graph_clone = graph.clone();
        let chunk_vec = chunk.to_vec();
        let sem_clone = semaphore.clone();

        let handle = tokio::spawn(async move {
            let _permit = sem_clone.acquire().await.unwrap();
            let batch = prepare_batch_parameters(chunk_vec);

            // Define retry configuration
            let backoff = ExponentialBackoff {
                initial_interval: Duration::from_millis(100),
                max_interval: Duration::from_secs(10),
                multiplier: 2.0,
                max_elapsed_time: Some(Duration::from_secs(60)), // Max 1 minute of retries
                ..ExponentialBackoff::default()
            };

            // Execute with retry logic
            match backoff::future::retry(backoff, || async {
                match run_insert_with_txn(&graph_clone, batch.clone()).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        // Check if error is a deadlock error
                        if is_deadlock_error(&e) {
                            println!("Deadlock detected in batch {}, will retry", batch_idx);
                            Err(BackoffError::transient(e))
                        } else {
                            // For other errors, don't retry
                            Err(BackoffError::permanent(e))
                        }
                    }
                }
            })
            .await
            {
                Ok(_) => println!("Batch {} completed successfully", batch_idx),
                Err(e) => eprintln!(
                    "Failed to process batch {} after all retries: {:?}",
                    batch_idx, e
                ),
            }
        });

        handles.push(handle);
    }

    // Wait for all batches to complete
    future::join_all(handles).await;
}

// Helper function to detect if an error is a deadlock error
fn is_deadlock_error(error: &neo4rs::Error) -> bool {
    // Neo4j deadlock errors typically contain specific codes or text
    // This is a common pattern, adjust based on actual error details
    let error_string = format!("{:?}", error);
    error_string.contains("DeadlockDetected")
        || error_string.contains("TransactionTerminatedException")
        || error_string.contains("concurrent access")
        || error_string.contains("deadlock")
}

// Separated transaction execution function for retry logic
async fn run_insert_with_txn(
    graph: &Graph,
    batch: Vec<HashMap<String, neo4rs::BoltType>>,
) -> Result<(), neo4rs::Error> {
    let mut txn = graph.start_txn().await?;

    // Run the query
    txn.run(
        query(
            "
            UNWIND $batch AS tweet
            MERGE (t:Tweet {id: tweet.id})
            SET 
                t.text = tweet.text,
                t.created_at = tweet.created_at,
                t.reply_to = tweet.reply_to,
                t.lang = tweet.lang,
                t.hashtags = tweet.hashtags,
                t.user_mentions = tweet.user_mentions
            MERGE (u:User {id: tweet.userId})
            ON CREATE SET 
                u.name = tweet.userName,
                u.location = tweet.userLocation,
                u.verified = tweet.userVerified,
                u.followers_count = tweet.userFollowersCount,
                u.friends_count = tweet.userFriendsCount,
                u.listed_count = tweet.userListedCount,
                u.favourites_count = tweet.userFavouritesCount,
                u.statuses_count = tweet.userStatusesCount,
                u.created_at = tweet.userCreatedAt,
                u.utc_offset = tweet.userUtcOffset
            CREATE (t)-[:POSTED_BY]->(u)
            ",
        )
        .param("batch", batch),
    )
    .await?;

    // Commit the transaction
    txn.commit().await?;

    Ok(())
}

pub async fn add_replies_to_relation(creds: Credentials) -> Result<(), neo4rs::Error> {
    println!("Linking tweets together...");
    let graph = Graph::new(creds.uri, creds.user, creds.password)
        .await
        .unwrap();

    let mut txn = graph.start_txn().await?;
    txn.run(query(
        "
        CALL apoc.periodic.iterate(
          '
          MATCH (t1:Tweet)
          WHERE t1.reply_to IS NOT NULL
          RETURN t1
          ',
          '
          MATCH (t2:Tweet {id: t1.reply_to})
          MERGE (t1)-[:REPLIES_TO]->(t2)
          ',
          {batchSize: 10000, parallel: false}
        );
        ",
    ))
    .await
    .unwrap();

    txn.commit().await?;

    Ok(())
}

pub async fn add_user_mention_relation(creds: Credentials) -> Result<(), neo4rs::Error> {
    println!("Adding user mentions...");
    let graph = Graph::new(creds.uri, creds.user, creds.password)
        .await
        .unwrap();

    let mut txn = graph.start_txn().await?;
    txn.run(query(
        "
        CALL apoc.periodic.iterate(
          '
          match (t: Tweet) with t, 
          t.user_mentions as m UNWIND m as uid 
          match (u: User {id: uid}) return t, u
          ',
          '
          MERGE (t)-[:MENTIONS]->(u)
          ',
          {batchSize: 10000, parallel: false}
        );
        ",
    ))
    .await
    .unwrap();

    txn.commit().await?;

    Ok(())
}

fn prepare_batch_parameters(chunk_vec: Vec<json::Tweet>) -> Vec<HashMap<String, neo4rs::BoltType>> {
    // Build batch parameters
    let batch: Vec<HashMap<String, neo4rs::BoltType>> = chunk_vec
        .iter()
        .map(|tweet| {
            let mut tweet_map = HashMap::new();

            // Tweet fields
            tweet_map.insert("id".to_string(), tweet.id_str.clone().into());
            tweet_map.insert("text".to_string(), tweet.text.clone().into());
            tweet_map.insert(
                "created_at".to_string(),
                tweet.created_at.to_rfc3339().into(),
            );
            tweet_map.insert("reply_to".to_string(), tweet.reply_to.clone().into());
            tweet_map.insert("lang".to_string(), tweet.lang.clone().into());
            tweet_map.insert(
                "hashtags".to_string(),
                tweet.entities.hashtags.clone().into(),
            );
            tweet_map.insert(
                "user_mentions".to_string(),
                tweet.entities.user_mentions.clone().into(),
            );

            // User fields
            tweet_map.insert("userId".to_string(), tweet.user.id_str.clone().into());
            tweet_map.insert(
                "userName".to_string(),
                tweet.user.screen_name.clone().into(),
            );
            tweet_map.insert(
                "userLocation".to_string(),
                tweet.user.location.clone().into(),
            );
            tweet_map.insert("userVerified".to_string(), tweet.user.verified.into());
            tweet_map.insert(
                "userFollowersCount".to_string(),
                tweet.user.followers_count.into(),
            );
            tweet_map.insert(
                "userFriendsCount".to_string(),
                tweet.user.friends_count.into(),
            );
            tweet_map.insert(
                "userListedCount".to_string(),
                tweet.user.listed_count.into(),
            );
            tweet_map.insert(
                "userFavouritesCount".to_string(),
                tweet.user.favourites_count.into(),
            );
            tweet_map.insert(
                "userStatusesCount".to_string(),
                tweet.user.statuses_count.into(),
            );
            tweet_map.insert(
                "userCreatedAt".to_string(),
                tweet.user.created_at.to_rfc3339().into(),
            );
            tweet_map.insert("userUtcOffset".to_string(), tweet.user.utc_offset.into());
            tweet_map
        })
        .collect();
    return batch;
}
