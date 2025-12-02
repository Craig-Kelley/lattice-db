use bincode::{Decode, Encode, config};
use redb::ReadableTable;

use crate::{
    LatticeReader, LatticeWriter,
    errors::LatticeError,
    lattice_db::tables::{PROP_NAMES, PROPERTIES},
};

pub(crate) const QUERY_MATCH: u64 = u64::MAX;

pub(crate) type PropertyId = u64;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Encode, Decode)]
pub struct PropertyHandle(pub(crate) PropertyId);

impl LatticeWriter {
    /// Register a property and return the handle.
    /// * If an alias is provided, creates a fast property lookup.
    /// * If the alias is taken, returns an error.
    pub fn register_property<'a, A, M>(
        &mut self,
        alias: A,
        meta: &M,
    ) -> Result<PropertyHandle, LatticeError>
    where
        A: Into<Option<&'a str>>,
        M: Encode,
    {
        // incr id
        let id = self.property_id_cursor;
        self.property_id_cursor += 1;

        // assign alias, prevents collisions
        let alias = alias.into();
        if let Some(name) = alias {
            let mut name_table = self.wt.open_table(PROP_NAMES)?;
            if name_table.get(name)?.is_some() {
                return Err(LatticeError::AliasAlreadyExists);
            }

            name_table.insert(name, id)?;
        }

        // write metadata
        let mut meta_table = self.wt.open_table(PROPERTIES)?;
        let meta_bytes = bincode::encode_to_vec(meta, config::standard())?;
        meta_table.insert(id, meta_bytes)?;
        Ok(PropertyHandle(id))
    }
}

impl LatticeReader {
    /// Get a property handle from an alias.
    pub fn get_property_handle(&self, alias: &str) -> Result<Option<PropertyHandle>, LatticeError> {
        let table = self.rt.open_table(PROP_NAMES)?;
        Ok(table.get(alias)?.map(|v| PropertyHandle(v.value())))
    }

    /// Get the property metadata.
    pub fn get_property_meta<M: Decode<()>>(
        &self,
        handle: PropertyHandle,
    ) -> Result<M, LatticeError> {
        let table = self.rt.open_table(PROPERTIES)?;
        let bytes = table
            .get(handle.0)?
            .ok_or_else(|| LatticeError::PropertyNotFound)?
            .value();
        let (meta, _) = bincode::decode_from_slice(&bytes, config::standard())?;
        Ok(meta)
    }
}
