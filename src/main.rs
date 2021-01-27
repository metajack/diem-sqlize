use anyhow::{anyhow, Result};
use diem_json_rpc_client::async_client::{Client, Retry};
use diem_types::{
    transaction::Transaction,
};
use diem_vm::{
    DiemVM, VMExecutor,
};
use itertools::Itertools;
use sqlx::{
    sqlite::SqlitePoolOptions,
    migrate::MigrateDatabase,
};
use structopt::StructOpt;
use url::Url;

use crate::{
    annotator::MoveValueAnnotator,
    db::DB,
    resolver::Resolver,
    state::{GenesisState, SqlState},
};

mod annotator;
mod db;
mod fat_type;
mod resolver;
mod state;
mod util;

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long, parse(try_from_str = Url::parse))]
    pub endpoint: Url,
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::from_args();

    let client = Client::from_url(options.endpoint.clone(), Retry::default()).unwrap();
    let metadata = client.get_metadata().await?;
    let latest_version = metadata.version;

    if sqlx::Sqlite::database_exists("sqlite:chain.db").await? {
        return Err(anyhow!("database already exists"));
    }

    sqlx::Sqlite::create_database("sqlite:chain.db").await?;

    let pool = SqlitePoolOptions::new()
        .connect("sqlite:chain.db").await?;
    let db = DB::from_pool(pool.clone());
    db.initialize().await;

    println!("current version is {}", latest_version);

    // Replay genesis (version 0)
    {
        println!("tx 0");
        let txs = client.get_transactions(0, 1, false).await?;
        let bytes = hex::decode(&txs[0].bytes).unwrap();
        let tx: Transaction = bcs::from_bytes(&bytes).unwrap();
        // VM is not async, but will call the `StateView` implementation which
        // must make async calls so we use `spawn_blocking` to let tokio know.
        let output  = tokio::task::spawn_blocking(move || {
            let state_view = GenesisState;
            let mut outputs = DiemVM::execute_block(vec![tx], &state_view).unwrap();
            outputs.remove(0)
        }).await?;
        println!("tx {}", output.status().status().unwrap());
        let resolver = Resolver::from_pool_and_genesis_write_set(pool.clone(), output.write_set());
        let annotator = MoveValueAnnotator::new(resolver);

        for (access_path, write_op) in output.write_set() {
            db.execute_with_annotator(access_path, write_op, &annotator).await;
        }
    }

    // Replay the rest of the chain in chunks
    for versions in &(1..latest_version).chunks(50) {
        let versions = versions.collect::<Vec<_>>();
        let first_version = versions[0];
        println!("txs from {}", first_version);
        let txs = client.get_transactions(first_version, versions.len() as u64, false)
            .await?
            .iter()
            .map(|t| {
                let bytes = hex::decode(&t.bytes).unwrap();
                bcs::from_bytes::<Transaction>(&bytes).unwrap()
            })
            .collect::<Vec<_>>();

        // VM is not async, but will call the `StateView` implementation which
        // must make async calls so we use `spawn_blocking` to let tokio know.
        let pool = pool.clone();
        let pool2 = pool.clone();
        let output = tokio::task::spawn_blocking(move || {
            let state_view = SqlState::from_pool(pool2);
            let mut outputs = DiemVM::execute_block(txs, &state_view).unwrap();
            outputs.remove(0)
        }).await?;

        println!("tx {}", output.status().status().unwrap());
        let resolver = Resolver::from_pool(pool.clone());
        let annotator = MoveValueAnnotator::new(resolver);

        for (access_path, write_op) in output.write_set() {
            db.execute_with_annotator(access_path, write_op, &annotator).await;
        }
    }

    Ok(())
}
