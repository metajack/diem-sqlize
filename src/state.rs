use anyhow::{anyhow, Result};
use diem_state_view::StateView;
use diem_types::{
    access_path::{AccessPath, Path},
};
use sqlx::{Row, sqlite::SqlitePool};
use tokio::runtime;

use crate::{
    db,
    resolver::Resolver,
    util,
};


/// State for the genesis transaction is empty
pub struct GenesisState;

impl StateView for GenesisState {
    fn get(&self, _access_path: &AccessPath) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn multi_get(&self, access_paths: &[AccessPath]) -> Result<Vec<Option<Vec<u8>>>> {
        access_paths.iter().map(|_| Ok(None)).collect()
    }

    fn is_genesis(&self) -> bool {
        true
    }
}

/// State for normal transactions reads from SQL. Structs are stored in
/// tables, and a special table `__root__$struct` maps addresses to top level
/// structs. Modules are stored in `__module`.
pub struct SqlState {
    pool: SqlitePool,
}

impl SqlState {
    pub fn from_pool(pool: SqlitePool) -> SqlState {
        SqlState {
            pool,
        }
    }
}

impl StateView for SqlState {
    fn get(&self, access_path: &AccessPath) -> Result<Option<Vec<u8>>> {
        let (address, path) = util::decode_access_path(access_path);
        println!("StateView::get({})", access_path);
        let rt = runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut db = self.pool.acquire().await?;
            match path {
                Path::Code(module_id) => {
                    println!("module get({})", module_id);
                    let select_sql = "SELECT data FROM __module WHERE address = ? AND name = ?";
                    let result = sqlx::query(select_sql)
                        .bind(module_id.address().as_ref())
                        .bind(module_id.name().as_str())
                        .fetch_optional(&mut db)
                        .await
                        .unwrap();
                    match result {
                        None => Ok(None),
                        Some(row) => Ok(row.get(0)),
                    }
                },
                Path::Resource(struct_tag) => {
                    println!("resource get({}::{})", address, struct_tag);
                    let sql_tag = db::struct_tag_to_sql(&struct_tag);
                    let select_sql = format!(
                        "SELECT id FROM __root__{} WHERE address = ?",
                        sql_tag,
                    );
                    println!("QUERY: {}\nPARAM: {}", select_sql, hex::encode(address));
                    let result = sqlx::query(&select_sql)
                        .bind(address.as_ref())
                        .fetch_optional(&mut db)
                        .await
                        .unwrap();
                    match result {
                        None => Ok(None),
                        Some(row) => {
                            println!("FETCHING STRUCT: {:?}", struct_tag);
                            let resolver = Resolver::from_pool(self.pool.clone());
                            let struct_ = db::fetch_struct(&struct_tag, row.get(0), &resolver, &mut db).await;
                            println!("FETCHED STRUCT: {:?}", struct_);
                            let bytes = bcs::to_bytes(&struct_).unwrap();
                            Ok(Some(bytes))
                        },
                    }
                },
            }
        })
    }

    fn multi_get(&self, access_paths: &[AccessPath]) -> Result<Vec<Option<Vec<u8>>>> {
        println!("get({:?})", access_paths);
        Err(anyhow!("not implemented"))
    }

    fn is_genesis(&self) -> bool {
        false
    }
}
