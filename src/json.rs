use serde::Deserialize;
use serde_json;
use std::fs::File;
use std::io::{BufReader, prelude::*};

#[derive(Debug, Deserialize, Clone)]
pub struct User {
    pub id_str: String,
    pub name: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Tweet {
    pub id_str: String,
    pub text: String,
    pub user: User,
    // #[serde(rename(serialize = "in_reply_to_status_id_str"))]
    // reply_to: Option<String>,
}

pub fn parse_file(filename: String) -> Vec<Tweet> {
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
