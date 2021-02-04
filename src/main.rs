use anyhow::{anyhow, Result};
use diem_json_rpc_client::async_client::{Client, Retry};
use diem_types::{
    account_address::AccountAddress,
    account_state::AccountState,
    access_path::{AccessPath, Path},
    transaction::Transaction,
    write_set::WriteOp,
};
use diem_vm::{
    DiemVM, VMExecutor,
};
use itertools::Itertools;
use std::{
    convert::TryFrom,
    path::PathBuf,
};
use sqlx::{
    sqlite::SqlitePoolOptions,
    migrate::MigrateDatabase,
};
use structopt::StructOpt;
use url::Url;

use crate::{
    annotator::MoveValueAnnotator,
    backup::Backup,
    db::DB,
    resolver::Resolver,
    state::{GenesisState, SqlState},
};

mod annotator;
mod backup;
mod db;
mod fat_type;
mod resolver;
mod state;
mod util;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
struct Options {
    #[structopt(long, parse(try_from_str = Url::parse))]
    pub endpoint: Url,
    #[structopt(long, parse(from_os_str), requires("backup-version"))]
    pub backup_file: Option<Vec<PathBuf>>,
    #[structopt(long, requires("backup-file"))]
    pub backup_version: Option<u64>,
}

fn find_account_address(state: &AccountState) -> AccountAddress {
    state
        .get_account_address()
        .unwrap()
        .unwrap_or_else(|| {
            state
                .iter()
                .find_map(|(k, _)| {
                    match Path::try_from(k).unwrap() {
                        Path::Code(module_id) => Some(module_id.address().clone()),
                        Path::Resource(_) => None,
                    }
                })
                .unwrap()
        })
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::from_args();

    let client = Client::from_url(options.endpoint.clone(), Retry::default()).unwrap();

    if sqlx::Sqlite::database_exists("sqlite:chain.db").await? {
        return Err(anyhow!("database already exists"));
    }

    sqlx::Sqlite::create_database("sqlite:chain.db").await?;

    let pool = SqlitePoolOptions::new()
        .connect("sqlite:chain.db").await?;
    let db = DB::from_pool(pool.clone());
    db.initialize().await;

    // if state backup is provided, boostrap with that
    let mut next_version = if let (Some(backup_file), Some(backup_version)) = (options.backup_file, options.backup_version) {
        // build an initial resolver. we can do this from genesis since new
        // modules don't get published.
        let txns = client.get_transactions(0, 1, false).await?;
        let bytes = hex::decode(&txns[0].bytes)?;
        let genesis_tx: Transaction = bcs::from_bytes(&bytes)?;
        let output = tokio::task::spawn_blocking(move || {
            let state_view = GenesisState;
            let mut outputs = DiemVM::execute_block(vec![genesis_tx], &state_view).unwrap();
            outputs.remove(0)
        }).await?;
        let resolver = Resolver::from_pool_and_genesis_write_set(pool.clone(), output.write_set());
        let annotator = MoveValueAnnotator::new(resolver);

        // process state snaphost from backup
        for file in backup_file {
            let backup = Backup::from_file(&file)?;
            for account_state in backup {
                let address = find_account_address(&account_state);
                for (key, value) in account_state.iter() {
                    let access_path = AccessPath::new(address.clone(), key.clone());
                    let write_op = WriteOp::Value(value.clone());
                    db.execute_with_annotator(&access_path, &write_op, &annotator).await;
                }
            }
        }
        backup_version + 1
    } else {
        0
    };

    if next_version == 0 {
        // Replay genesis (version 0)
        println!("tx 0");
        let txs = client.get_transactions(0, 1, false).await?;
        let bytes = hex::decode(&txs[0].bytes).unwrap();
        let tx: Transaction = bcs::from_bytes(&bytes).unwrap();
        // VM is not async, but will call the `StateView` implementation which
        // must make async calls so we use `spawn_blocking` to let tokio know.
        let output = tokio::task::spawn_blocking(move || {
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

        next_version += 1;
    }

    let resolver = Resolver::from_pool(pool.clone());
    let annotator = MoveValueAnnotator::new(resolver);

    // Replay the rest of the chain in chunks and continuing tailing
    loop {
        let metadata = client.get_metadata().await?;
        let latest_version = metadata.version;
        if latest_version < next_version {
            println!("up to date; waiting for new blocks...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            continue;
        }

        for versions in &(next_version..latest_version).chunks(100) {
            let versions = versions.collect::<Vec<_>>();
            let first_version = versions[0];
            let last_version = versions.last().unwrap();
            
            println!("syncing from {} to {}", first_version, last_version);
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
            let outputs = tokio::task::spawn_blocking(move || {
                let state_view = SqlState::from_pool(pool2);
                DiemVM::execute_block(txs, &state_view).unwrap()
            }).await?;

            for output in outputs {
                for (access_path, write_op) in output.write_set() {
                    db.execute_with_annotator(access_path, write_op, &annotator).await;
                }
            }

            next_version = last_version + 1;
        }
    }
}
