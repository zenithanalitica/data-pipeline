use confy;
use glob::glob;
use tokio;
mod db;
mod json;

#[tokio::main]
async fn main() {
    let creds: db::Credentials = confy::load_path("./credentials.toml").unwrap();
    db::prepare_database(creds.clone()).await;

    for file in glob("../data/*.json").expect("Failed to read glob pattern") {
        let filename = file.unwrap().to_str().unwrap().to_owned();
        let tweets = json::parse_file(filename);
        db::insert_new_tweets(creds.clone(), tweets).await;
    }
}
