use tokio;
mod app;
mod db;
mod json;

#[tokio::main]
async fn main() {
    let mut app = app::App::default();
    app.run().await;
}
