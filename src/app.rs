use confy;
use glob::glob;
use rayon::prelude::*;
use std::process::exit;
use std::sync::{Arc, Mutex};

use crate::db;
use crate::json;

pub struct App {
    credentials: db::Credentials,
    tweet_count: u64,
    deletet_tweet_count: u32,
    retweet_count: u32,
}

impl App {
    pub async fn run(&mut self) {
        let res = db::prepare_database(self.credentials.clone()).await;

        match res {
            Ok(_) => (),
            Err(e) => {
                eprintln!("{}", e);
                eprintln!("Could not connect to the database. Check if it's running.");
                exit(1)
            }
        }

        // For the async function, we need to collect results and process them after parallel execution
        let files: Vec<_> = glob("/data/airlines-*.json")
            .expect("Failed to read glob pattern")
            .filter_map(Result::ok)
            .collect();

        let results = self.parse_files(files);

        // Process database insertions sequentially since they're async operations
        for tweets in results {
            db::insert_new_tweets(self.credentials.clone(), tweets).await;
        }

        println!("Number of tweets: {}", self.tweet_count);
        println!("Number of deleted tweets: {}", self.deletet_tweet_count);
        println!(
            "Percentage of retweets: {}%",
            self.retweet_count as f32 / self.tweet_count as f32 * 100.
        );

        db::add_replies_to_relation(self.credentials.clone())
            .await
            .unwrap();
        db::add_user_mention_relation(self.credentials.clone())
            .await
            .unwrap();
        db::add_airline_labels(self.credentials.clone())
            .await
            .unwrap();
        println!("Done!")
    }

    pub fn parse_files(&mut self, files: Vec<std::path::PathBuf>) -> Vec<Vec<json::Tweet>> {
        let deleted_tweets = Arc::new(Mutex::new(0));
        let number_of_tweets = Arc::new(Mutex::new(0));
        let number_of_retweets = Arc::new(Mutex::new(0));

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

        self.tweet_count = *number_of_tweets.lock().unwrap();
        self.deletet_tweet_count = *deleted_tweets.lock().unwrap();
        self.retweet_count = *number_of_retweets.lock().unwrap();
        results
    }
}

impl Default for App {
    fn default() -> Self {
        let credentials: db::Credentials = confy::load_path("./credentials.toml").unwrap();
        Self {
            credentials,
            tweet_count: Default::default(),
            deletet_tweet_count: Default::default(),
            retweet_count: Default::default(),
        }
    }
}
