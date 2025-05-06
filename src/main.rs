use confy;
use glob::glob;
use tokio;
mod db;
mod json;

#[tokio::main]
async fn main() {
    let creds: db::Credentials = confy::load_path("./credentials.toml").unwrap();
    db::prepare_database(creds.clone()).await.unwrap();
    let mut deleted_tweets = 0;
    let mut number_of_tweets = 0;
    let mut number_of_retweets = 0;

    for file in glob("../data/*.json").expect("Failed to read glob pattern") {
        let filename = file.unwrap().to_str().unwrap().to_owned();
        let (tweets, deleted, tweet_num, retweet_num) = json::parse_file(filename);

        deleted_tweets += deleted;
        number_of_tweets += tweet_num;
        number_of_retweets += retweet_num;

        db::insert_new_tweets(creds.clone(), tweets).await;
    }

    println!("Number of tweets: {}", number_of_tweets);
    println!("Number of deleted tweets: {}", deleted_tweets);
    println!(
        "Percentage of retweets: {}%",
        number_of_retweets as f32 / number_of_tweets as f32 * 100.
    );

    db::add_replies_to_relation(creds.clone()).await.unwrap();
    db::add_user_mention_relation(creds.clone()).await.unwrap();
    println!("Done!")
}
