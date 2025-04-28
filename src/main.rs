use confy;
use neo4rs::{self, Graph, query};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufReader, prelude::*};
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
    #[serde(rename(serialize = "in_reply_to_status_id_str"))]
    reply_to: Option<String>,
}

#[tokio::main]
async fn main() {
    // let creds = parse_arguments();
    let creds: Credentials = confy::load_path("./credentials.toml").unwrap();

    let tweets = readfile("../data/airlines-1558527599826.json".to_string());
    load_data(creds, tweets).await;
}

fn readfile(filename: String) -> Vec<Tweet> {
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

async fn load_data(creds: Credentials, tweets: Vec<Tweet>) {
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

    let batch_size = 500; // How many nodes per transaction
    let max_concurrent_batches = 1; // Has to be one in order to eliminate race conditions, but
    // still be async

    // Wait a moment for the constraint to be fully applied
    println!("Waiting for the constraint to be applied...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Then proceed with concurrent batch processing
    let semaphore = std::sync::Arc::new(Semaphore::new(max_concurrent_batches));
    let mut handles = Vec::new();

    for (batch_idx, chunk) in tweets.chunks(batch_size).enumerate() {
        let graph_clone = graph.clone();
        let chunk_vec = chunk.to_vec();
        let sem_clone = semaphore.clone();

        let handle = tokio::spawn(async move {
            let _permit = sem_clone.acquire().await.unwrap();

            let mut txn = graph_clone.start_txn().await.unwrap();

            // Create batch parameters - similar to your current code
            let batch = prepare_batch_parameters(chunk_vec);

            // Execute with error handling to deal with potential constraint violations
            match txn
                .run(
                    query(
                        "
            UNWIND $batch AS tweet
            CREATE (t:Tweet {id: tweet.id, text: tweet.text})
            MERGE (u:User {id: tweet.userId})
            ON CREATE SET u.name = tweet.userName
            CREATE (t)-[:USER]->(u)
        ",
                    )
                    .param("batch", batch),
                )
                .await
            {
                Ok(_) => match txn.commit().await {
                    Ok(_) => println!("Batch {} completed successfully", batch_idx),
                    Err(e) => eprintln!("Failed to commit batch {}: {:?}", batch_idx, e),
                },
                Err(e) => eprintln!("Failed to execute batch {}: {:?}", batch_idx, e),
            }
        });

        handles.push(handle);
    }

    futures::future::join_all(handles).await;
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
