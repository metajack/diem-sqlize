use anyhow::Result;
use diem_crypto::HashValue;
use diem_types::{
    account_state::AccountState,
    account_state_blob::AccountStateBlob,
};
use libflate::gzip::Decoder;
use std::{
    cell::RefCell,
    convert::{TryFrom, TryInto},
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

pub struct Backup {
    reader: RefCell<Decoder<BufReader<File>>>,
    buffer: RefCell<Vec<u8>>,
}

impl Backup {
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let reader = RefCell::new(Decoder::new(BufReader::new(File::open(path)?)).unwrap());
        let buffer = RefCell::new(Vec::with_capacity(4096*4));
        Ok(Self {
            reader,
            buffer,
        })
    }
}

impl Iterator for Backup {
    type Item = AccountState;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_buf = vec![0u8; 4];
        if let Err(_) = self.reader.borrow_mut().read_exact(len_buf.as_mut_slice()) {
            return None;
        }
        let blob_len = u32::from_be_bytes(len_buf.try_into().unwrap()) as usize;
        let mut buffer = self.buffer.borrow_mut();
        buffer.resize(blob_len, 0);

        if let Err(_) = self.reader.borrow_mut().read_exact(&mut buffer.as_mut_slice()[..blob_len]) {
            return None;
        }

        let (_, asb): (HashValue, AccountStateBlob) = match bcs::from_bytes(&buffer[0..blob_len]) {
            Err(_) => return None,
            Ok(r) => r,
        };

        AccountState::try_from(&asb).ok()
    }
}
