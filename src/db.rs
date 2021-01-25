use diem_types::{
    access_path::{AccessPath, Path},
    write_set::WriteOp,
};
use move_core_types::{
    account_address::AccountAddress,
    identifier::Identifier,
    language_storage::{ModuleId, StructTag, TypeTag},
    value::{MoveStruct, MoveValue},
};
use resource_viewer::{
    AnnotatedMoveStruct, AnnotatedMoveValue, MoveValueAnnotator,
};
use sqlx::{
    Row,
    pool::PoolConnection,
    sqlite::{Sqlite, SqlitePool},
};
use std::{
    convert::{TryFrom, TryInto},
    future::Future,
    pin::Pin,
};
use vm::normalized::{Struct, Type};

use crate::{
    resolver,
    util,
};

pub struct DB {
    pool: SqlitePool,
}

impl DB {
    /// Initialize `DB` with default cache built on `SqlState`
    pub fn from_pool(pool: SqlitePool) -> DB {
        DB {
            pool,
        }
    }

    pub async fn execute_with_annotator(
        &self,
        access_path: &AccessPath,
        op: &WriteOp,
        annotator: &MoveValueAnnotator<'_>,
    ) {
        let (address, path) = util::decode_access_path(access_path);
        match (&path, op) {
            (Path::Code(id), WriteOp::Deletion) => self.unpublish(id).await,
            (Path::Code(id), WriteOp::Value(v)) => self.publish(id, v).await,
            (Path::Resource(tag), WriteOp::Deletion) => self.delete(&address, tag).await,
            (Path::Resource(tag), WriteOp::Value(v)) => {
                let resource = annotator.view_resource(tag, v).unwrap();
                self.store(&address, tag, resource).await
            },
        }
    }

    async fn unpublish(&self, id: &ModuleId) {
        println!("unpublishing {}", id);
        todo!();
    }

    async fn publish(&self, id: &ModuleId, data: &[u8]) {
        println!("publishing {}", id);
        let address = id.address();
        let name = id.name().as_str();
        let create_sql = format!(
            "INSERT INTO __module VALUES (?, ?, ?)",
        );
        let mut db = self.pool.acquire().await.unwrap();
        sqlx::query(&create_sql)
            .bind(address.as_ref())
            .bind(name)
            .bind(data)
            .execute(&mut db)
            .await
            .unwrap();
    }

    async fn delete(&self, address: &AccountAddress, tag: &StructTag) {
        println!("deleting {}::{}", address, tag);
        todo!();
    }

    async fn store(&self, address: &AccountAddress, tag: &StructTag, data: AnnotatedMoveStruct) {
        println!("storing {}::{}", address, tag);
        println!("{}", data);
        let mut db = self.pool.acquire().await.unwrap();
        generate_sql(&address, Some(&data), &mut db).await;
    }
}

pub async fn initialize(pool: &SqlitePool) {
    let mut db = pool.acquire().await.unwrap();

    let create_sql = format!(
        "CREATE TABLE __module (address BLOB NOT NULL, name STRING NOT NULL, data BLOB NOT NULL, CONSTRAINT __module_pkey PRIMARY KEY (address, name))",
    );
    sqlx::query(&create_sql).execute(&mut db).await.unwrap();
}

pub async fn generate_sql(address: &AccountAddress, value: Option<&AnnotatedMoveStruct>, db: &mut PoolConnection<Sqlite>) {
    // post order traversal of the struct to write it
    match value {
        Some(struct_) => {
            let id = struct_to_sql(struct_, db).await;

            // attach struct to global storage
            let create_sql = format!(
                "CREATE TABLE IF NOT EXISTS __root__{} (address BLOB UNIQUE NOT NULL, id INTEGER NOT NULL)",
                struct_tag_to_sql(&struct_.type_),
            );
            println!("{}", create_sql);
            sqlx::query(&create_sql).execute(&mut *db).await.unwrap();

            let insert_sql = format!(
                "INSERT INTO __root__{} VALUES (x'{}', {})",
                struct_tag_to_sql(&struct_.type_),
                hex::encode(address),
                id,
            );
            println!("{}", insert_sql);
            sqlx::query(&insert_sql).execute(&mut *db).await.unwrap();
        },
        None => {
            todo!();
        },
    }
}

fn struct_to_sql<'a>(struct_: &'a AnnotatedMoveStruct, db: &'a mut PoolConnection<Sqlite>) -> Pin<Box<dyn Future<Output=i64> + 'a>> {
    Box::pin(async move {
        // handle fields
        let mut field_names = vec![];
        let mut fields = vec![];
        let mut values = vec![];

        fields.push("__id INTEGER PRIMARY KEY".to_string());

        for (ident, val) in &struct_.value {
            match val {
                AnnotatedMoveValue::U8(i) => {
                    field_names.push(format!("{}", ident));
                    fields.push(format!("{} INTEGER NOT NULL", ident));
                    values.push(format!("{}", i));
                },
                AnnotatedMoveValue::U64(i) => {
                    field_names.push(format!("{}", ident));
                    fields.push(format!("{} INTEGER NOT NULL", ident));
                    values.push(format!("{}", i));
                },
                AnnotatedMoveValue::U128(i) => {
                    field_names.push(format!("{}", ident));
                    fields.push(format!("{} BLOB NOT NULL", ident));
                    values.push(format!("x'{}'", hex::encode(i.to_be_bytes())));
                },
                AnnotatedMoveValue::Bool(i) => {
                    field_names.push(format!("{}", ident));
                    fields.push(format!("{} BOOLEAN NOT NULL", ident));
                    values.push(format!("{}", i));
                },
                AnnotatedMoveValue::Address(i) => {
                    field_names.push(format!("{}", ident));
                    fields.push(format!("{} BLOB NOT NULL", ident));
                    values.push(format!("x'{}'", hex::encode(i)));
                },
                AnnotatedMoveValue::Bytes(i) => {
                    field_names.push(format!("{}", ident));
                    fields.push(format!("{} BLOB NOT NULL", ident));
                    values.push(format!("x'{}'", hex::encode(&i)));
                },
                AnnotatedMoveValue::Struct(s) => {
                    let id = struct_to_sql(s, db).await;
                    field_names.push(format!("{}", ident));
                    fields.push(format!("{} INTEGER NOT NULL", ident));
                    values.push(format!("{}", id));
                },
                AnnotatedMoveValue::Vector(ty, v) => {
                    match ty {
                        TypeTag::Bool |
                        TypeTag::U8 |
                        TypeTag::U64 |
                        TypeTag::U128 => {
                            // primitive vectors are stored inline
                            let bytes = vector_to_bytes(v);
                            field_names.push(format!("{}", ident));
                            fields.push(format!("{} BLOB NOT NULL", ident));
                            values.push(format!("x'{}'", hex::encode(&bytes)));
                        },

                        TypeTag::Signer => unreachable!(),

                        TypeTag::Address |
                        TypeTag::Vector(_) |
                        TypeTag::Struct(_) => {
                            // complex vectors are stored in a separate table named after the field
                            // these generate no fields in this struct, and are handled later
                        },
                    }
                },
            }
        }

        if !struct_.value.is_empty() {
            let create_sql = format!("CREATE TABLE IF NOT EXISTS {} ({})",
                                     struct_tag_to_sql(&struct_.type_),
                                     fields.join(", "));
            println!("{}", create_sql);
            sqlx::query(&create_sql).execute(&mut *db).await.unwrap();
            
            let insert_sql = if !field_names.is_empty() {
                format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    struct_tag_to_sql(&struct_.type_),
                    field_names.join(", "),
                    values.join(", "),
                )
            } else {
                format!("INSERT INTO {} DEFAULT VALUES", struct_tag_to_sql(&struct_.type_))
            };
            println!("{}", insert_sql);
            let result = sqlx::query(&insert_sql).execute(&mut *db).await.unwrap();
            let id = result.last_insert_rowid();

            // handle complex vectors inside the struct
            for (ident, val) in &struct_.value {
                match val {
                    AnnotatedMoveValue::Vector(ty, v) => {
                        match ty {
                            TypeTag::Address |
                            TypeTag::Vector(_) |
                            TypeTag::Struct(_) => {
                                let name = vector_table_name(&struct_.type_, ident);
                                vector_to_sql(name, id, &ty, &v, &mut *db).await;
                            },
                            _ => {},
                        }
                    },
                    _ => {},
                }
            }

            id
        } else {
            let create_sql = format!("CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY)",
                                     struct_tag_to_sql(&struct_.type_));
            println!("{}", create_sql);
            sqlx::query(&create_sql).execute(&mut *db).await.unwrap();
            
            let insert_sql = format!("INSERT INTO {} DEFAULT VALUES;", struct_tag_to_sql(&struct_.type_));
            println!("{}", insert_sql);
            let result = sqlx::query(&insert_sql).execute(&mut *db).await.unwrap();

            result.last_insert_rowid()
        }
    })
}

async fn vector_to_sql(name: String, pid: i64, ty: &TypeTag, v: &[AnnotatedMoveValue], db: &mut PoolConnection<Sqlite>) {
    // create table for this vector

    let field = match ty {
        TypeTag::Address => "slot BLOB NOT NULL".to_string(),
        TypeTag::Vector(vty) => {
            match **vty {
                // this is Vector<u8> aka Bytes
                TypeTag::U8 => "slot BLOB NOT NULL".to_string(),
                // other vectors generate no field
                _ => "".to_string(),
            }
        },
        TypeTag::Struct(_) => "slot INTEGER NOT NULL".to_string(),
        _ => unreachable!(),
    };

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, {})",
        name,
        field,
    );
    println!("{}", create_sql);
    sqlx::query(&create_sql).execute(&mut *db).await.unwrap();
    
    // populate table
    for e in v {
        match e {
            AnnotatedMoveValue::Address(a) => {
                let insert_sql = format!(
                    "INSERT INTO {} (parent_id, slot) VALUES ({}, x'{}')",
                    name,
                    pid,
                    hex::encode(a),
                );
                println!("{}", insert_sql);
                sqlx::query(&insert_sql).execute(&mut *db).await.unwrap();
            },
            AnnotatedMoveValue::Struct(s) => {
                let id = struct_to_sql(s, db).await;
                let insert_sql = format!(
                    "INSERT INTO {} (parent_id, slot) VALUES ({}, {})",
                    name,
                    pid,
                    id,
                );
                println!("{}", insert_sql);
                sqlx::query(&insert_sql).execute(&mut *db).await.unwrap();
            },
            AnnotatedMoveValue::Bytes(b) => {
                let insert_sql = format!(
                    "INSERT INTO {} (parent_id, slot) VALUES ({}, x'{}')",
                    name,
                    pid,
                    hex::encode(b),
                );
                println!("{}", insert_sql);
                sqlx::query(&insert_sql).execute(&mut *db).await.unwrap();
            },

            AnnotatedMoveValue::Vector(_vty, _vval) => todo!(),
            _ => unreachable!(),
        }
    }
    
}

fn vector_to_bytes(v: &[AnnotatedMoveValue]) -> Vec<u8> {
    v.iter().flat_map(|value| {
        match value {
            AnnotatedMoveValue::Bool(b) => vec![if *b { 1u8 } else { 0u8 }],
            AnnotatedMoveValue::U8(i) => vec![*i],
            AnnotatedMoveValue::U64(i) => i.to_be_bytes().to_vec(),
            AnnotatedMoveValue::U128(i) => i.to_be_bytes().to_vec(),
            _ => unreachable!(),
        }
    }).collect()
}

fn type_param_to_sql(param: &TypeTag) -> String {
    match param {
        TypeTag::Bool => "Bool".to_string(),
        TypeTag::U8 => "U8".to_string(),
        TypeTag::U64 => "U64".to_string(),
        TypeTag::U128 => "U128".to_string(),
        TypeTag::Address => "Address".to_string(),
        TypeTag::Signer => unreachable!(),
        TypeTag::Vector(type_tag) => format!("Vector__t_{}_t", type_param_to_sql(&type_tag)),
        TypeTag::Struct(struct_tag) => struct_tag_to_sql(struct_tag),
    }
}

fn type_params_to_sql(params: &[TypeTag]) -> String {
    let result: Vec<_> = params.iter().map(|tt| type_param_to_sql(tt)).collect();
    result.join("__")
}

pub fn struct_tag_to_sql(tag: &StructTag) -> String {
    let type_params_str = if !tag.type_params.is_empty() {
        format!("__t_{}_t", type_params_to_sql(&tag.type_params))
    } else {
        "".to_string()
    };
    format!("x{}__{}__{}{}",
            tag.address.short_str_lossless(),
            tag.module,
            tag.name,
            type_params_str)
}

fn vector_table_name(tag: &StructTag, field_name: &Identifier) -> String {
    format!("{}__{}__elements", struct_tag_to_sql(tag), field_name)
}

pub fn fetch_struct<'a>(
    tag: &'a StructTag,
    id: i64,
    db: &'a mut PoolConnection<Sqlite>,
) -> Pin<Box<dyn Future<Output=MoveValue> + 'a>> {
    Box::pin(async move {
        // Find the fields to query for the struct
        let struct_ = resolver::resolve_struct(tag, &mut *db).await.unwrap();
        println!("resolved struct: {:?}", struct_);

        let select_sql = format!(
            "SELECT {} FROM {} WHERE __id = {}",
            struct_columns(tag, &struct_).join(", "),
            struct_tag_to_sql(tag),
            id,
        );
        println!("{}", select_sql);
        let row = sqlx::query(&select_sql)
            .fetch_one(&mut *db)
            .await
            .unwrap();

        let mut fields = vec![];
        let mut column_index = 0;
        for field in &struct_.fields {
            println!("column_index = {}, field.name = {}, field.type = {:?}", column_index, field.name, field.type_);
            match field.type_ {
                // these field types cannot appear in storage
                Type::Signer |
                Type::Reference(_) |
                Type::MutableReference(_) => unreachable!(),

                // vectors (other than Vector<u8>) have no corresponding column in the struct's table
                Type::Vector(ref sub_type) => {
                    match **sub_type {
                        Type::U8 => {
                            let bytes: Vec<u8> = row.get(column_index);
                            let v: Vec<MoveValue> = bytes.into_iter().map(|b| MoveValue::U8(b)).collect();
                            fields.push(MoveValue::Vector(v));
                            column_index += 1;
                        },

                        _ => {
                            let v = fetch_vector(tag, id, &field.name, &*sub_type, &mut *db).await;
                            fields.push(MoveValue::Vector(v));
                            // don't change column index
                        },
                    }
                },

                // type parameters need to check the concrete type
                Type::TypeParameter(i) => {
                    match tag.type_params[i as usize] {
                        // signers can not appear in storage
                        TypeTag::Signer => unreachable!(),

                        // vectors (other than Vector<u8>) have no corresponding column in the struct's table
                        TypeTag::Vector(_) => {
                            todo!("vectors todo");
                        },

                        // these types all have fields
                        TypeTag::Bool => {
                            fields.push(MoveValue::Bool(row.get(column_index)));
                            column_index += 1;
                        },
                        TypeTag::U8 => {
                            fields.push(MoveValue::U8(row.get::<i64, _>(column_index) as u8));
                            column_index += 1;
                        },
                        TypeTag::U64 => {
                            fields.push(MoveValue::U64(row.get::<i64, _>(column_index) as u64));
                            column_index += 1;
                        },
                        TypeTag::U128 => {
                            let bytes: Vec<u8> = row.get(column_index);
                            let v = u128::from_be_bytes(bytes.try_into().unwrap());
                            fields.push(MoveValue::U128(v));
                            column_index += 1;
                        },
                        TypeTag::Address => {
                            let bytes: Vec<u8> = row.get(column_index);
                            fields.push(MoveValue::Address(AccountAddress::try_from(bytes).unwrap()));
                            column_index += 1;
                        },
                        TypeTag::Struct(ref sub_tag) => {
                            let sub_id: i64 = row.get(column_index);
                            let value = fetch_struct(&sub_tag, sub_id, &mut *db).await;
                            fields.push(value);
                            column_index += 1;
                        },
                    }
                },

                // these types all have fields
                Type::Bool => {
                    fields.push(MoveValue::Bool(row.get(column_index)));
                    column_index += 1;
                },
                Type::U8 => {
                    fields.push(MoveValue::U8(row.get::<i64, _>(column_index) as u8));
                    column_index += 1;
                },
                Type::U64 => {
                    fields.push(MoveValue::U64(row.get::<i64, _>(column_index) as u64));
                    column_index += 1;
                },
                Type::U128 => {
                    let bytes: Vec<u8> = row.get(column_index);
                    let v = u128::from_be_bytes(bytes.try_into().unwrap());
                    fields.push(MoveValue::U128(v));
                    column_index += 1;
                },
                Type::Address => {
                    let bytes: Vec<u8> = row.get(column_index);
                    fields.push(MoveValue::Address(AccountAddress::try_from(bytes).unwrap()));
                    column_index += 1;
                },
                Type::Struct { ref address, ref module, ref name, ref type_arguments } => {
                    let sub_tag = StructTag {
                        address: address.clone(),
                        module: module.clone(),
                        name: name.clone(),
                        type_params: type_arguments
                            .into_iter()
                            .map(|t| t.clone().into_type_tag().unwrap())
                            .collect(),
                    };
                    let sub_id = row.get(column_index);
                    let value = fetch_struct(&sub_tag, sub_id, &mut *db).await;
                    fields.push(value);
                    column_index += 1;
                },
            }
        }

        MoveValue::Struct(MoveStruct::new(fields))
    })
}

/// Return the set of columns in a struct's table. This will be a subset of
/// columns as Vector fields do not have a column.
fn struct_columns<'a>(tag: &'a StructTag, struct_: &'a Struct) -> Vec<&'a str> {
    struct_.fields.iter().filter_map(|field| {
        match field.type_ {
            // these field types cannot appear in storage
            Type::Signer |
            Type::Reference(_) |
            Type::MutableReference(_) => unreachable!(),

            // vectors (other than Vector<u8>) have no corresponding column in the struct's table
            Type::Vector(ref sub_type) => {
                match **sub_type {
                    Type::U8 => Some(field.name.as_str()),
                    _ => None,
                }
            },

            // type parameters need to check the concrete type
            Type::TypeParameter(i) => {
                match tag.type_params[i as usize] {
                    // signers can not appear in storage
                    TypeTag::Signer => unreachable!(),

                    // vectors (other than Vector<u8>) have no corresponding column in the struct's table
                    TypeTag::Vector(ref sub_tag) => {
                        match **sub_tag {
                            TypeTag::U8 => Some(field.name.as_str()),
                            _ => None,
                        }
                    },

                    // these types all have fields
                    TypeTag::Bool |
                    TypeTag::U8 |
                    TypeTag::U64 |
                    TypeTag::U128 |
                    TypeTag::Address |
                    TypeTag::Struct(_) => Some(field.name.as_str()),
                }
            },

            // these types all have fields
            Type::Bool |
            Type::U8 |
            Type::U64 |
            Type::U128 |
            Type::Address |
            Type::Struct { .. } => Some(field.name.as_str()),
        }
    }).collect()
}

fn fetch_vector<'a>(
    tag: &'a StructTag,
    id: i64,
    field_name: &'a Identifier,
    elem_type: &'a Type,
    db: &'a mut PoolConnection<Sqlite>,
) -> Pin<Box<dyn Future<Output=Vec<MoveValue>> + 'a>> {
    Box::pin(async move {
        let table_name = vector_table_name(tag, field_name);
        let kind = match elem_type {
            // these field types cannot appears in storage
            Type::Signer |
            Type::Reference(_) |
            Type::MutableReference(_) => unreachable!(),

            // vectors (other than Vector<u8>) have no corresponding column in the elements table
            Type::Vector(ref sub_type) => {
                match **sub_type {
                    Type::Signer |
                    Type::Reference(_) |
                    Type::MutableReference(_) => unreachable!(),

                    Type::U8 => ElementKind::Bytes,
                    _ => ElementKind::Vector(sub_type.clone().into_type_tag().unwrap()), 
                }
            },

            // type parameters need to check the concrete type
            Type::TypeParameter(i) => {
                match tag.type_params[*i as usize] {
                    // signers cannot appears in storage
                    TypeTag::Signer => unreachable!(),

                    // vectors (other than Vector<u8>) have no corresponding column in the elements table
                    TypeTag::Vector(ref sub_type) => {
                        match **sub_type {
                            TypeTag::U8 => ElementKind::Bytes,
                            _ => ElementKind::Vector((**sub_type).clone()),
                        }
                    },

                    // these types are all stored inline in the `slot` column
                    TypeTag::Bool => ElementKind::Bool,
                    TypeTag::U8 => ElementKind::U8,
                    TypeTag::U64 => ElementKind::U64,
                    TypeTag::U128 => ElementKind::U128,
                    TypeTag::Address => ElementKind::Address,
                    TypeTag::Struct(_) => ElementKind::Struct,
                }
            },

            // these types are all stored inline in the `slot` column
            Type::Bool => ElementKind::Bool,
            Type::U8 => ElementKind::U8,
            Type::U64 => ElementKind::U64,
            Type::U128 => ElementKind::U128,
            Type::Address => ElementKind::Address,
            Type::Struct { .. } => ElementKind::Struct,
        };

        match kind {
            ElementKind::Vector(_) => todo!(),
            kind => {
                let select_sql = format!(
                    "SELECT slot FROM {} WHERE parent_id = {} ORDER BY rowid",
                    table_name,
                    id,
                );
                println!("ELEMENTS QUERY: {}", select_sql);
                let rows = sqlx::query(&select_sql)
                    .fetch_all(&mut *db)
                    .await
                    .unwrap();
                rows
                    .into_iter()
                    .map(|row| {
                        match kind {
                            ElementKind::Bool => MoveValue::Bool(row.get(0)),
                            ElementKind::U8 => MoveValue::U8(row.get::<i64,_>(0) as u8),
                            ElementKind::U64 => MoveValue::U64(row.get::<i64,_>(0) as u64),
                            ElementKind::U128 => {
                                let bytes: Vec<u8> = row.get(0);
                                let v = u128::from_be_bytes(bytes.try_into().unwrap());
                                MoveValue::U128(v)
                            },
                            ElementKind::Address => {
                                let bytes: Vec<u8> = row.get(0);
                                MoveValue::Address(AccountAddress::try_from(bytes).unwrap())
                            },
                            ElementKind::Bytes => {
                                let bytes: Vec<u8> = row.get(0);
                                let v: Vec<MoveValue> = bytes
                                    .into_iter()
                                    .map(|b| MoveValue::U8(b)).collect();
                                MoveValue::Vector(v)
                            },
                            ElementKind::Struct => {
                                todo!()
                            },
                            ElementKind::Vector(_) => unreachable!(),
                        }
                    })
                    .collect()
            },
        }
    })
}

enum ElementKind {
    Bool,
    U8,
    U64,
    U128,
    Address,
    Struct,
    Bytes,
    Vector(TypeTag),
}