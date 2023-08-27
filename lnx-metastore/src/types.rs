use std::borrow::Cow;

use lnx_tools::consistent_hash;

pub trait Key {
    fn to_hash(&self) -> u64;
}

impl Key for u64 {
    fn to_hash(&self) -> u64 {
        let bytes = self.to_le_bytes();
        consistent_hash(bytes)
    }
}

impl Key for u32 {
    fn to_hash(&self) -> u64 {
        let bytes = self.to_le_bytes();
        consistent_hash(bytes)
    }
}

impl Key for u16 {
    fn to_hash(&self) -> u64 {
        let bytes = self.to_le_bytes();
        consistent_hash(bytes)
    }
}

impl Key for String {
    fn to_hash(&self) -> u64 {
        let bytes = self.as_bytes();
        consistent_hash(bytes)
    }
}

impl Key for &str {
    fn to_hash(&self) -> u64 {
        let bytes = self.as_bytes();
        consistent_hash(bytes)
    }
}

impl<'a> Key for Cow<'a, str> {
    fn to_hash(&self) -> u64 {
        let bytes = self.as_bytes();
        consistent_hash(bytes)
    }
}

impl Key for &[u8] {
    fn to_hash(&self) -> u64 {
        consistent_hash(self)
    }
}

impl Key for str {
    fn to_hash(&self) -> u64 {
        consistent_hash(self)
    }
}