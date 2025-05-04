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

    for file in glob("../data/*.json").expect("Failed to read glob pattern") {
        let filename = file.unwrap().to_str().unwrap().to_owned();
        let (tweets, deleted) = json::parse_file(filename);
        deleted_tweets += deleted;
        db::insert_new_tweets(creds.clone(), tweets).await;
    }
    println!("Number of deleted tweets: {}", deleted_tweets);

    db::link_tweets(creds.clone()).await.unwrap();
}
