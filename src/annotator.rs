use anyhow::{anyhow, Result};
use diem_types::{
    account_address::AccountAddress,
    contract_event::ContractEvent,
};
use move_core_types::{
    identifier::Identifier,
    language_storage::{StructTag, TypeTag},
    value::{MoveStruct, MoveValue},
};
use std::{
    convert::TryInto,
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
};
use vm::errors::{
    Location, PartialVMError,
};

use crate::{
    fat_type::{FatStructType, FatType},
    resolver::Resolver,
};

#[derive(Debug, Eq, PartialEq)]
pub struct AnnotatedMoveStruct {
    pub is_resource: bool,
    pub type_: StructTag,
    pub value: Vec<(Identifier, AnnotatedMoveValue)>,
}

/// AnnotatedMoveValue is a fully expanded version of on chain Move data. This should only be used
/// for debugging/client purpose right now and just for a better visualization of on chain data. In
/// the long run, we would like to transform this struct to a Json value so that we can have a cross
/// platform interpretation of the on chain data.
#[derive(Debug, Eq, PartialEq)]
pub enum AnnotatedMoveValue {
    U8(u8),
    U64(u64),
    U128(u128),
    Bool(bool),
    Address(AccountAddress),
    Vector(TypeTag, Vec<AnnotatedMoveValue>),
    Bytes(Vec<u8>),
    Struct(AnnotatedMoveStruct),
}

pub struct MoveValueAnnotator {
    resolver: Resolver,
}

impl MoveValueAnnotator {
    pub fn new(resolver: Resolver) -> Self {
        Self {
            resolver
        }
    }

    pub async fn view_resource(&self, tag: &StructTag, blob: &[u8]) -> Result<AnnotatedMoveStruct> {
        let ty = self.resolver.resolve_struct(tag).await?;
        let struct_def = (&ty)
            .try_into()
            .map_err(|e: PartialVMError| e.finish(Location::Undefined).into_vm_status())?;
        let move_struct = MoveStruct::simple_deserialize(blob, &struct_def)?;
        self.annotate_struct(&move_struct, &ty).await
    }

    #[allow(dead_code)]
    pub async fn view_contract_event(&self, event: &ContractEvent) -> Result<AnnotatedMoveValue> {
        let ty = self.resolver.resolve_type(event.type_tag()).await?;
        let move_ty = (&ty)
            .try_into()
            .map_err(|e: PartialVMError| e.finish(Location::Undefined).into_vm_status())?;

        let move_value = MoveValue::simple_deserialize(event.event_data(), &move_ty)?;
        self.annotate_value(&move_value, &ty).await
    }

    pub async fn annotate_struct(
        &self,
        move_struct: &MoveStruct,
        ty: &FatStructType,
    ) -> Result<AnnotatedMoveStruct> {
        let struct_tag = ty
            .struct_tag()
            .map_err(|e| e.finish(Location::Undefined).into_vm_status())?;
        let mut annotated_fields = vec![];
        for ((id, ty), v) in ty.fields.iter().zip(move_struct.fields().iter()) {
            annotated_fields.push((id.clone(), self.annotate_value(v, ty).await?));
        }
        Ok(AnnotatedMoveStruct {
            is_resource: ty.is_resource,
            type_: struct_tag,
            value: annotated_fields,
        })
    }

    fn annotate_value<'a>(&'a self, value: &'a MoveValue, ty: &'a FatType) -> Pin<Box<dyn Future<Output=Result<AnnotatedMoveValue>> + 'a>> {
        Box::pin(async move {
            Ok(match (value, ty) {
                (MoveValue::Bool(b), FatType::Bool) => AnnotatedMoveValue::Bool(*b),
                (MoveValue::U8(i), FatType::U8) => AnnotatedMoveValue::U8(*i),
                (MoveValue::U64(i), FatType::U64) => AnnotatedMoveValue::U64(*i),
                (MoveValue::U128(i), FatType::U128) => AnnotatedMoveValue::U128(*i),
                (MoveValue::Address(a), FatType::Address) => AnnotatedMoveValue::Address(*a),
                (MoveValue::Vector(a), FatType::Vector(ty)) => match ty.as_ref() {
                    FatType::U8 => AnnotatedMoveValue::Bytes(
                        a.iter()
                            .map(|v| match v {
                                MoveValue::U8(i) => Ok(*i),
                                _ => Err(anyhow!("unexpected value type")),
                            })
                            .collect::<Result<_>>()?,
                    ),
                    _ => AnnotatedMoveValue::Vector(
                        ty.type_tag().unwrap(),
                        {
                            let mut values = vec![];
                            for v in a {
                                values.push(self.annotate_value(v, ty.as_ref()).await?);
                            }
                            values
                        },
                    ),
                },
                (MoveValue::Struct(s), FatType::Struct(ty)) => {
                    AnnotatedMoveValue::Struct(self.annotate_struct(s, ty.as_ref()).await?)
                }
                _ => {
                    return Err(anyhow!(
                        "Cannot annotate value {:?} with type {:?}",
                        value,
                        ty
                    ))
                }
            })
        })
    }
}

fn write_indent(f: &mut Formatter, indent: u64) -> std::fmt::Result {
    for _i in 0..indent {
        write!(f, " ")?;
    }
    Ok(())
}

fn pretty_print_value(
    f: &mut Formatter,
    value: &AnnotatedMoveValue,
    indent: u64,
) -> std::fmt::Result {
    match value {
        AnnotatedMoveValue::Bool(b) => write!(f, "{}", b),
        AnnotatedMoveValue::U8(v) => write!(f, "{}u8", v),
        AnnotatedMoveValue::U64(v) => write!(f, "{}", v),
        AnnotatedMoveValue::U128(v) => write!(f, "{}u128", v),
        AnnotatedMoveValue::Address(a) => write!(f, "{}", a.short_str_lossless()),
        AnnotatedMoveValue::Vector(_, v) => {
            writeln!(f, "[")?;
            for value in v.iter() {
                write_indent(f, indent + 4)?;
                pretty_print_value(f, value, indent + 4)?;
                writeln!(f, ",")?;
            }
            write_indent(f, indent)?;
            write!(f, "]")
        }
        AnnotatedMoveValue::Bytes(v) => write!(f, "{}", hex::encode(&v)),
        AnnotatedMoveValue::Struct(s) => pretty_print_struct(f, s, indent),
    }
}

fn pretty_print_struct(
    f: &mut Formatter,
    value: &AnnotatedMoveStruct,
    indent: u64,
) -> std::fmt::Result {
    writeln!(
        f,
        "{}{} {{",
        if value.is_resource { "resource " } else { "" },
        value.type_
    )?;
    for (field_name, v) in value.value.iter() {
        write_indent(f, indent + 4)?;
        write!(f, "{}: ", field_name)?;
        pretty_print_value(f, v, indent + 4)?;
        writeln!(f)?;
    }
    write_indent(f, indent)?;
    write!(f, "}}")
}

impl Display for AnnotatedMoveValue {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        pretty_print_value(f, self, 0)
    }
}

impl Display for AnnotatedMoveStruct {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        pretty_print_struct(f, self, 0)
    }
}
