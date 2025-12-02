use bincode::{Decode, Encode};
use rapidhash::v3::rapidhash_v3;

use crate::errors::LatticeError;

/// Value representation when stored inside the Graph.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum Primitive {
    UInt(u64),
    Text(String),
}

const PRIMITIVE_UINT: u64 = 1 << 56;
const PRIMITIVE_TEXT: u64 = 2 << 56;

impl Primitive {
    /// Verify the value can be used as a Value for the graph.
    pub fn verify(&self) -> Result<(), LatticeError> {
        match self {
            Primitive::UInt(n) => {
                if (*n & 0xFF00000000000000) != 0 {
                    return Err(LatticeError::NumberTooBig(n.to_string()));
                }
            }
            Primitive::Text(_) => {}
        }
        Ok(())
    }

    /// Hashes the value.
    /// * Value lookups are stored as hashes inside the database.
    pub fn hash(&self) -> u64 {
        match self {
            Primitive::UInt(n) => *n | PRIMITIVE_UINT,
            Primitive::Text(t) => {
                (rapidhash_v3(t.as_bytes()) & 0x00FFFFFFFFFFFFFF) | PRIMITIVE_TEXT
            }
        }
    }
}

/// Allows for storage inside the Graph.
pub trait Value {
    fn to_primitive(self) -> Primitive;
}

impl Value for u64 {
    fn to_primitive(self) -> Primitive {
        Primitive::UInt(self)
    }
}

impl Value for u32 {
    fn to_primitive(self) -> Primitive {
        Primitive::UInt(self as u64)
    }
}

impl Value for u16 {
    fn to_primitive(self) -> Primitive {
        Primitive::UInt(self as u64)
    }
}

impl Value for u8 {
    fn to_primitive(self) -> Primitive {
        Primitive::UInt(self as u64)
    }
}

impl Value for &str {
    fn to_primitive(self) -> Primitive {
        Primitive::Text(self.to_string())
    }
}
