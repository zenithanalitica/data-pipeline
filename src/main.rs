use std::sync::{Arc, Mutex};

use confy;
use glob::glob;
use rayon::prelude::*;
use tokio;
mod db;
mod json;

#[tokio::main]
async fn main() {
    let creds: db::Credentials = confy::load_path("./credentials.toml").unwrap();
    // db::prepare_database(creds.clone()).await.unwrap();

    let deleted_tweets = Arc::new(Mutex::new(0));
    let number_of_tweets = Arc::new(Mutex::new(0));
    let number_of_retweets = Arc::new(Mutex::new(0));

    // For the async function, we need to collect results and process them after parallel execution
    let files: Vec<_> = glob("../data/*.json")
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .collect();

    // Process files in parallel
    let results: Vec<_> = files
        .par_iter()
        .map(|file| {
            let filename = file.to_str().unwrap().to_owned();
            let (tweets, deleted, tweet_num, retweet_num) = json::parse_file(filename);

            // Update shared counters
            {
                let mut deleted_count = deleted_tweets.lock().unwrap();
                *deleted_count += deleted;
            }
            {
                let mut tweets_count = number_of_tweets.lock().unwrap();
                *tweets_count += tweet_num;
            }
            {
                let mut retweets_count = number_of_retweets.lock().unwrap();
                *retweets_count += retweet_num;
            }

            // Return tweets for later async processing
            tweets
        })
        .collect();

    // Process database insertions sequentially since they're async operations
    for tweets in results {
        // db::insert_new_tweets(creds.clone(), tweets).await;
    }

    println!("Number of tweets: {}", *number_of_tweets.lock().unwrap());
    println!(
        "Number of deleted tweets: {}",
        *deleted_tweets.lock().unwrap()
    );
    println!(
        "Percentage of retweets: {}%",
        *number_of_retweets.lock().unwrap() as f32 / *number_of_tweets.lock().unwrap() as f32
            * 100.
    );

    // db::add_replies_to_relation(creds.clone()).await.unwrap();
    // db::add_user_mention_relation(creds.clone()).await.unwrap();
    println!("Done!")
}
