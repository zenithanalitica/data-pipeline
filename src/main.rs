use backoff::{Error as BackoffError, ExponentialBackoff};
use confy;
use futures::future;
use glob::glob;
use neo4rs::{self, Graph, query};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, prelude::*};
use std::sync::Arc;
use std::time::Duration;
use tokio;
use tokio::sync::Semaphore;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Credentials {
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

#[derive(Debug, Deserialize, Clone)]
struct User {
    id_str: String,
    name: String,
}

#[derive(Debug, Deserialize, Clone)]
struct Tweet {
    id_str: String,
    text: String,
    user: User,
    // #[serde(rename(serialize = "in_reply_to_status_id_str"))]
    // reply_to: Option<String>,
}

#[tokio::main]
async fn main() {
    // let creds = parse_arguments();
    let creds: Credentials = confy::load_path("./credentials.toml").unwrap();
    // prepare_database(creds.clone()).await;

    for file in glob("../data/*.json").expect("Failed to read glob pattern") {
        let filename = file.unwrap().to_str().unwrap().to_owned();
        let tweets = parse_file(filename);
        insert_new_tweets(creds.clone(), tweets).await;
    }
}

async fn prepare_database(creds: Credentials) {
    let graph = Graph::new(creds.uri, creds.user, creds.password)
        .await
        .unwrap();

    // Run this BEFORE starting any imports to ensure uniqueness of users
    graph
        .run(query(
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
        ))
        .await
        .unwrap();
    //
    // Wait a moment for the constraint to be fully applied
    println!("Waiting for the constraint to be applied...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
}

fn parse_file(filename: String) -> Vec<Tweet> {
    println!("Parsing file {}", filename);
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);
    let mut tweets = vec![];

    for line in reader.lines() {
        let content = line.unwrap();
        if content.contains("\"delete\":") {
            continue;
        }

        match serde_json::from_str::<Tweet>(&content) {
            Ok(tweet) => tweets.push(tweet),
            Err(e) => eprintln!("Failed to parse line: {}", e),
        }
    }
    return tweets;
}

async fn insert_new_tweets(creds: Credentials, tweets: Vec<Tweet>) {
    let graph = Graph::new(creds.uri, creds.user, creds.password)
        .await
        .unwrap();
    let batch_size = 1000; // How many nodes per transaction
    let max_concurrent_batches = 4; // Limit concurrent transactions

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
            CREATE (t:Tweet {id: tweet.id, text: tweet.text})
            MERGE (u:User {id: tweet.userId})
            ON CREATE SET u.name = tweet.userName
            CREATE (t)-[:CREATED_BY]->(u)
            ",
        )
        .param("batch", batch),
    )
    .await?;

    // Commit the transaction
    txn.commit().await?;

    Ok(())
}

fn prepare_batch_parameters(chunk_vec: Vec<Tweet>) -> Vec<HashMap<String, neo4rs::BoltType>> {
    // Build batch parameters
    let batch: Vec<HashMap<String, neo4rs::BoltType>> = chunk_vec
        .iter()
        .map(|tweet| {
            let mut tweet_map = HashMap::new();
            tweet_map.insert("id".to_string(), tweet.id_str.clone().into());
            tweet_map.insert("text".to_string(), tweet.text.clone().into());
            tweet_map.insert("userId".to_string(), tweet.user.id_str.clone().into());
            tweet_map.insert("userName".to_string(), tweet.user.name.clone().into());
            tweet_map
        })
        .collect();
    return batch;
}
