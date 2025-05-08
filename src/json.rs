use chrono::{DateTime, Utc};
use lines;
use lines::linereader::LineReader;
use serde::Deserialize;
use serde_json;
use std::fs::File;
use std::str::from_utf8;

#[derive(Debug, Deserialize, Clone)]
pub struct User {
    pub id_str: String,
    pub screen_name: String,
    pub location: Option<String>,
    pub verified: bool,
    pub followers_count: i32,
    pub friends_count: i32,
    pub listed_count: Option<i32>,
    pub favourites_count: i32,
    pub statuses_count: i32,
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
    pub lang: String,
    pub entities: Entity,
    #[serde(default)]
    pub is_retweet: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Entity {
    #[serde(deserialize_with = "deserialize_hashtags")]
    pub hashtags: Vec<String>,
    #[serde(deserialize_with = "deserialize_user_mentions")]
    pub user_mentions: Vec<String>,
}

pub fn parse_file(filename: String) -> (Vec<Tweet>, u32, u64, u32) {
    println!("Parsing file {}", filename);

    let file = File::open(filename.clone()).unwrap();

    let mut tweets = vec![];
    let mut deleted = 0;
    let mut tweet_num: u64 = 0;
    let mut retweet_num = 0;

    lines::read_lines!(line in LineReader::new(file), {
        tweet_num += 1;
        let content = from_utf8(line.unwrap()).unwrap();
        if content.contains("\"delete\":") {
            deleted += 1;
            continue;
        }

        match serde_json::from_str::<Tweet>(&content) {
            Ok(mut tweet) => {
                if content.contains("\"retweeted_status\":") {
                    retweet_num += 1;
                    tweet.is_retweet = true;
                }
                tweets.push(tweet);
            }
            Err(e) => {
                eprintln!("Failed to parse file {} \nline: {}\n {}", filename, e, content);
            }
        }
    });
    return (tweets, deleted, tweet_num, retweet_num);
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

fn deserialize_user_mentions<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mentions: Vec<serde_json::Value> = Deserialize::deserialize(deserializer)?;
    let ids = mentions
        .into_iter()
        .filter_map(|mention| {
            mention
                .get("id_str")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
        })
        .collect();
    Ok(ids)
}

fn deserialize_hashtags<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let hashtag_maps: Vec<serde_json::Value> = Deserialize::deserialize(deserializer)?;
    let hashtags = hashtag_maps
        .into_iter()
        .filter_map(|mention| {
            mention
                .get("text")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
        })
        .collect();
    Ok(hashtags)
}
