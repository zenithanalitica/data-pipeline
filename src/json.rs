use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json;
use std::fs::File;
use std::io::{BufReader, prelude::*};

#[derive(Debug, Deserialize, Clone)]
pub struct User {
    pub id_str: String,
    pub screen_name: String,
    pub location: Option<String>,
    pub verified: bool,
    pub followers_count: u32,
    pub friends_count: u32,
    pub listed_count: u32,
    pub favourites_count: u32,
    pub statuses_count: u32,
    #[serde(deserialize_with = "deserialize_twitter_date")]
    pub created_at: DateTime<Utc>,
    pub utc_offset: Option<i32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Tweet {
    #[serde(deserialize_with = "deserialize_twitter_date")]
    pub created_at: DateTime<Utc>,
    pub id_str: String,
    pub text: String,
    pub user: User,
    #[serde(rename = "in_reply_to_status_id_str")]
    pub reply_to: Option<String>,
    pub quote_count: u32,
    pub reply_count: u32,
    pub retweet_count: u32,
    pub favorite_count: u32,
    pub filter_level: String,
    pub lang: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Entity {
    pub hashtags: Vec<String>,
    pub user_mentions: Vec<User>,
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

fn deserialize_twitter_date<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: &str = serde::Deserialize::deserialize(deserializer)?;
    // Twitter format: "Thu May 23 14:54:46 +0000 2019"
    DateTime::parse_from_str(s, "%a %b %d %H:%M:%S %z %Y")
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(serde::de::Error::custom)
}
