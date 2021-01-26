use anyhow::{anyhow, Result};
use diem_types::{
    access_path::Path,
    account_address::AccountAddress,
    write_set::{WriteOp, WriteSet},
};
use move_core_types::{
    identifier::IdentStr,
    language_storage::{ModuleId, StructTag, TypeTag},
};
use std::{
    cell::RefCell,
    collections::HashMap,
    future::Future,
    pin::Pin,
    rc::Rc,
};
use sqlx::{
    Row,
    sqlite::SqlitePool,
};
use vm::{
    access::ModuleAccess,
    file_format::{CompiledModule, SignatureToken, StructDefinitionIndex, StructFieldInformation, StructHandleIndex},
};

use crate::{
    fat_type::{FatStructType, FatType},
};

pub struct Resolver {
    pool: SqlitePool,
    cache: RefCell<HashMap<ModuleId, Rc<CompiledModule>>>,
}

impl Resolver {
    pub fn from_pool(pool: SqlitePool) -> Self {
        let cache = RefCell::new(HashMap::new());
        Resolver {
            pool,
            cache,
        }
    }

    /// Pre-fill the internal cache with the modules published in the write
    /// set. This should only be needed when resolving the genesis
    /// transaction.
    pub fn from_pool_and_genesis_write_set(pool: SqlitePool, write_set: &WriteSet) -> Self {
        let mut map = HashMap::new();
        for (access_path, write_op) in write_set {
            let path: Path = bcs::from_bytes(&access_path.path).unwrap();
            match (&path, write_op) {
                (Path::Code(module_id), WriteOp::Value(value)) => {
                    let module = CompiledModule::deserialize(value).unwrap();
                    map.insert(module_id.clone(), Rc::new(module));
                },
                _ => {},
            }
        }

        let cache = RefCell::new(map);
        Resolver {
            pool,
            cache,
        }
    }

    pub async fn get_module(&self, address: &AccountAddress, name: &IdentStr) -> Result<Rc<CompiledModule>> {
        let mut db = self.pool.acquire().await?;
        let module_id = ModuleId::new(address.clone(), name.to_owned());
        let cached = self.cache.borrow().get(&module_id).cloned();
        if let Some(module) = cached {
            Ok(module)
        } else {
            let result = sqlx::query("SELECT data FROM __module WHERE address = ? AND name = ?")
                .bind(address.as_ref())
                .bind(name.as_str())
                .fetch_optional(&mut db)
                .await?;
            match result {
                None => Err(anyhow!("module {}::{} not found", address.short_str(), name)),
                Some(row) => {
                    let data: Vec<u8> = row.get(0);
                    let module = CompiledModule::deserialize(&data)
                        .map_err(|e| anyhow!("module {}::{} failed deserialization: {}", address.short_str(), name, e))?;
                    let module = Rc::new(module);
                    self.cache.borrow_mut().insert(module_id, module.clone());
                    Ok(module)
                },
            }
        }
    }

    pub fn resolve_type<'a>(&'a self, type_tag: &'a TypeTag) -> Pin<Box<dyn Future<Output=Result<FatType>> + 'a>> {
        Box::pin(async move {
            Ok(match type_tag {
                TypeTag::Address => FatType::Address,
                TypeTag::Signer => return Err(anyhow!("cannot resolve Signer types")),
                TypeTag::Bool => FatType::Bool,
                TypeTag::Struct(struct_) => FatType::Struct(Box::new(self.resolve_struct(struct_).await?)),
                TypeTag::U8 => FatType::U8,
                TypeTag::U64 => FatType::U64,
                TypeTag::U128 => FatType::U128,
                TypeTag::Vector(type_) => FatType::Vector(Box::new(self.resolve_type(type_).await?)),
            })
        })
    }

    pub fn resolve_struct<'a>(&'a self, struct_tag: &'a StructTag) -> Pin<Box<dyn Future<Output=Result<FatStructType>> + 'a>> {
        Box::pin(async move {
            let module = self.get_module(&struct_tag.address, &struct_tag.module).await?;
            let struct_def = find_struct_def_in_module(&module, &struct_tag.name)?;
            let mut ty_args = vec![];
            for ty in &struct_tag.type_params {
                ty_args.push(self.resolve_type(ty).await?);
            }
            let ty_body = self.resolve_struct_definition(&module, struct_def).await?;
            ty_body.subst(&ty_args).map_err(|e| anyhow!("struct {:?} cannot be resolved {:?}", struct_tag, e))
        })
    }

    fn resolve_signature<'a>(
        &'a self,
        module: &'a CompiledModule,
        sig: &'a SignatureToken,
    ) -> Pin<Box<dyn Future<Output=Result<FatType>> + 'a>> {
        Box::pin(async move {
            Ok(match sig {
                SignatureToken::Reference(_) |
                SignatureToken::MutableReference(_) => return Err(anyhow!("unexpected reference type")),
                SignatureToken::Bool => FatType::Bool,
                SignatureToken::U8 => FatType::U8,
                SignatureToken::U64 => FatType::U64,
                SignatureToken::U128 => FatType::U128,
                SignatureToken::Address => FatType::Address,
                SignatureToken::Signer => return Err(anyhow!("unexpected Signer type")),
                SignatureToken::Vector(ty) => {
                    FatType::Vector(Box::new(self.resolve_signature(module, ty).await?))
                }
                SignatureToken::Struct(idx) => {
                    FatType::Struct(Box::new(self.resolve_struct_handle(module, *idx).await?))
                }
                SignatureToken::StructInstantiation(idx, toks) => {
                    let struct_ty = self.resolve_struct_handle(module, *idx).await?;
                    let mut args = vec![];
                    for tok in toks {
                        args.push(self.resolve_signature(module, tok).await?);
                    }
                    FatType::Struct(Box::new(
                        struct_ty
                            .subst(&args)
                            .map_err(|status| anyhow!("substitution failure: {:?}", status))?,
                    ))
                }
                SignatureToken::TypeParameter(idx) => FatType::TyParam(*idx as usize),
            })
        })
    }

    async fn resolve_struct_handle(&self, module: &CompiledModule, idx: StructHandleIndex) -> Result<FatStructType> {
        let struct_handle = module.struct_handle_at(idx);
        let target_module = {
            let module_handle = module.module_handle_at(struct_handle.module);
            self.get_module(
                module.address_identifier_at(module_handle.address),
                module.identifier_at(module_handle.name),
            ).await?
        };
        let target_idx = find_struct_def_in_module(
            &target_module,
            module.identifier_at(struct_handle.name),
        )?;
        self.resolve_struct_definition(&target_module, target_idx).await
    }

    async fn resolve_struct_definition(&self, module: &CompiledModule, idx: StructDefinitionIndex) -> Result<FatStructType> {
        let struct_def = module.struct_def_at(idx);
        let struct_handle = module.struct_handle_at(struct_def.struct_handle);
        let address = module.address().clone();
        let module_name = module.name().to_owned();
        let name = module.identifier_at(struct_handle.name).to_owned();
        let is_resource = struct_handle.is_nominal_resource;
        let ty_args = (0..struct_handle.type_parameters.len())
            .map(FatType::TyParam)
            .collect();
        match &struct_def.field_information {
            StructFieldInformation::Native => Err(anyhow!("unexpected native struct")),
            StructFieldInformation::Declared(defs) => {
                let mut fields = vec![];
                for field_def in defs {
                    let name = module.identifier_at(field_def.name).to_owned();
                    let type_ = self.resolve_signature(module, &field_def.signature.0).await?;
                    fields.push((name, type_));
                }
                Ok(FatStructType {
                    address,
                    module: module_name,
                    name,
                    is_resource,
                    ty_args,
                    fields,
                })
            },
        }
    }
}

fn find_struct_def_in_module(module: &CompiledModule, name: &IdentStr) -> Result<StructDefinitionIndex> {
    for (i, defs) in module.struct_defs().iter().enumerate() {
        let st_handle = module.struct_handle_at(defs.struct_handle);
        if module.identifier_at(st_handle.name) == name {
            return Ok(StructDefinitionIndex::new(i as u16));
        }
    }
    Err(anyhow!("struct {} not found in {}", name, module.self_id()))
}
