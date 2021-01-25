use diem_types::access_path::{AccessPath, Path};
use move_core_types::account_address::AccountAddress;

pub fn decode_access_path(access_path: &AccessPath) -> (AccountAddress, Path) {
    let address = access_path.address.clone();
    let path = bcs::from_bytes(&access_path.path).unwrap();
    (address, path)
}
