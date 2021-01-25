use anyhow::{anyhow, Result};
use diem_json_rpc_client::async_client::{Client, Retry};
use diem_state_view::StateView;
use diem_types::{
    transaction::Transaction,
};
use diem_vm::{
    DiemVM, VMExecutor,
    data_cache::StateViewCache,
};
use move_vm_runtime::data_cache::RemoteCache;
use resource_viewer::MoveValueAnnotator;
use sqlx::{
    sqlite::SqlitePoolOptions,
    migrate::MigrateDatabase,
};
use structopt::StructOpt;
use url::Url;

use crate::{
    state::{GenesisMemoryCache, GenesisState, SqlState},
};

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
    db::initialize(&pool).await;

    for version in 0..latest_version {
        println!("tx {}", version);
        let txs = client.get_transactions(version, 1, false).await?;
        let bytes = hex::decode(&txs[0].bytes).unwrap();
        let tx: Transaction = bcs::from_bytes(&bytes).unwrap();

        // VM is not async, but will call the `StateView` implementation which
        // must make async calls so we use `spawn_blocking` to let tokio know.
        let pool = pool.clone();
        let pool2 = pool.clone();
        let output = tokio::task::spawn_blocking(move || {
            if version == 0 {
                let state_view = GenesisState;
                let mut outputs = DiemVM::execute_block(vec![tx], &state_view).unwrap();
                outputs.remove(0)
            } else {
                let state_view = SqlState::from_pool(pool2);
                let mut outputs = DiemVM::execute_block(vec![tx], &state_view).unwrap();
                outputs.remove(0)
            }
        }).await?;

        let state_view = SqlState::from_pool(pool.clone());
        let cache = if version == 0 {
            let cache = GenesisMemoryCache::from_write_set(output.write_set());
            Box::new(cache) as Box<dyn RemoteCache>
        } else {
            let cache = StateViewCache::new(&state_view as &dyn StateView);
            Box::new(cache) as Box<dyn RemoteCache>
        };

        if version == 0 {
            println!("tx {}", output.status().status().unwrap());
            let annotator = MoveValueAnnotator::new_no_stdlib(&*cache);
            let db = db::DB::from_pool(pool);

            for (access_path, write_op) in output.write_set() {
                db.execute_with_annotator(access_path, write_op, &annotator).await;
            }
        } else {
            println!("tx {}", output.status().status().unwrap());
            let annotator = MoveValueAnnotator::new_no_stdlib(&*cache);
            let db = db::DB::from_pool(pool);

            for (access_path, write_op) in output.write_set() {
                db.execute_with_annotator(access_path, write_op, &annotator).await;
                todo!();
            }
        }
    }

    // rough plan:
    // 1. get sqlite going
    // 2. make a stateview impl for sqlite
    // 3. spin up a DiemVM
    // 4. pass in txns one at a time
    // 5. convert writeset into sql
    //    a. decode AccessPath to Path
    //    b. decode blob into AnnotatedMoveStruct (or similar)
    //       - will need to make a temp RemoteCache for genesis
    //    c. fetch old blob (may want to keep a binary cache of bcs data by accesspath)
    //    d. decode old blob into AnnotatedMoveStruct
    //    e. produce PrimitiveWriteSet for the blob
    //    f. generate SQL from primitive write set

    Ok(())
}
