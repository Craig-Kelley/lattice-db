use bincode::{Decode, Encode, config};
use redb::ReadableTable;

use crate::{
    LatticeReader, LatticeWriter, PreparedQuery, QueryBuilder,
    errors::LatticeError,
    lattice_db::tables::{QUERIES, QUERY_METAS, QUERY_NAMES},
};

pub struct QueryHandle(u64);

impl LatticeWriter {
    pub fn save_query<'a, M, A>(
        &mut self,
        query: &QueryBuilder,
        alias: A,
        meta: &M,
    ) -> Result<QueryHandle, LatticeError>
    where
        A: Into<Option<&'a str>>,
        M: Encode,
    {
        // incr id
        let id = self.query_id_cursor;
        self.query_id_cursor += 1;

        // assign alias, prevents collisions
        let alias = alias.into();
        if let Some(name) = alias {
            let mut name_table = self.wt.open_table(QUERY_NAMES)?;
            if name_table.get(name)?.is_some() {
                return Err(LatticeError::AliasAlreadyExists);
            }
            name_table.insert(name, id)?;
        }

        // write metadata
        let mut meta_table = self.wt.open_table(QUERY_METAS)?;
        let meta_bytes = bincode::encode_to_vec(meta, config::standard())?;
        meta_table.insert(id, meta_bytes)?;

        // write query
        let query = query.compile()?;
        let mut table = self.wt.open_table(QUERIES)?;
        let query_bytes = bincode::encode_to_vec(query, config::standard())?;
        table.insert(id, query_bytes)?;

        Ok(QueryHandle(id))
    }

    /// Return a prepared query.
    pub fn get_prepared_query(&self, handle: QueryHandle) -> Result<PreparedQuery, LatticeError> {
        let table = self.wt.open_table(QUERIES)?;
        let bytes = table
            .get(handle.0)?
            .ok_or(LatticeError::QueryNotFound)?
            .value();
        let prepared = bincode::decode_from_slice(&bytes, config::standard())?.0;
        Ok(prepared)
    }

    /// Get all saved query handles.
    pub fn get_all_queries(&self) -> Result<Vec<QueryHandle>, LatticeError> {
        let mut handles = vec![];
        let table = self.wt.open_table(QUERIES)?;
        for i in 0..self.query_id_cursor {
            if table.get(i)?.is_some() {
                handles.push(QueryHandle(i));
            }
        }
        Ok(handles)
    }

    // /// Return all loaded saved queries.
    // pub(crate) fn load_all_saved_queries(&self) -> Result<Vec<PreparedQuery>, LatticeError> {
    //     let mut pq = vec![];
    //     for h in self.get_all_queries()? {
    //         pq.push(self.get_prepared_query(h)?);
    //     }
    //     Ok(pq)
    // }
}

impl LatticeReader {
    /// Get a query handle from the alias.
    pub fn get_query_handle(&self, alias: &str) -> Result<Option<QueryHandle>, LatticeError> {
        let table = self.rt.open_table(QUERY_NAMES)?;
        Ok(table.get(alias)?.map(|v| QueryHandle(v.value())))
    }

    /// Retrieve query metadata from the database.
    pub fn get_query_meta<M: Decode<()>>(&self, handle: QueryHandle) -> Result<M, LatticeError> {
        let table = self.rt.open_table(QUERY_METAS)?;
        let bytes = table
            .get(handle.0)?
            .ok_or_else(|| LatticeError::QueryNotFound)?
            .value();
        let (meta, _) = bincode::decode_from_slice(&bytes, config::standard())?;
        Ok(meta)
    }

    /// Retrieve a compiled query from the database.
    pub fn get_prepared_query(&self, handle: QueryHandle) -> Result<PreparedQuery, LatticeError> {
        let table = self.rt.open_table(QUERIES)?;
        let bytes = table
            .get(handle.0)?
            .ok_or(LatticeError::QueryNotFound)?
            .value();
        let prepared = bincode::decode_from_slice(&bytes, config::standard())?.0;
        Ok(prepared)
    }
}
