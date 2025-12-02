use std::collections::HashMap;

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use redb::{ReadableTable, TableDefinition, WriteTransaction};
use roaring::RoaringTreemap;

use crate::{
    errors::LatticeError,
    graph::{
        graph_builder::GraphBuilder,
        graph_prepared::{GraphCommitData, PreparedGraph},
    },
    lattice_db::tables::{
        GRAPHS, INDEX_FORWARD, INDEX_REVERSE, INDEX_SCALAR, SEQ_GRAPH_ID, SEQ_PROPERTY_ID,
        SEQ_QUERY_ID, SEQ_VERTEX_ID, SEQUENCES, VERTEX_GRAPH_MAP,
    },
};

pub struct LatticeWriter {
    pub(crate) wt: WriteTransaction,
    graph_id_cursor: u64,
    vertex_id_cursor: u64,
    pub(crate) property_id_cursor: u64,
    pub(crate) query_id_cursor: u64,

    scalar_cache: HashMap<(u64, u64), RoaringTreemap>,
    forward_cache: HashMap<(u64, u64), RoaringTreemap>,
    reverse_cache: HashMap<(u64, u64), RoaringTreemap>,
}

impl LatticeWriter {
    pub(crate) fn new(wt: WriteTransaction) -> Result<Self, LatticeError> {
        let graph_id_cursor;
        let vertex_id_cursor;
        let property_id_cursor;
        let query_id_cursor;
        {
            let seq_table = wt.open_table(SEQUENCES)?;
            graph_id_cursor = seq_table.get(SEQ_GRAPH_ID)?.map(|v| v.value()).unwrap_or(0);
            vertex_id_cursor = seq_table
                .get(SEQ_VERTEX_ID)?
                .map(|v| v.value())
                .unwrap_or(0);
            property_id_cursor = seq_table
                .get(SEQ_PROPERTY_ID)?
                .map(|v| v.value())
                .unwrap_or(0);
            query_id_cursor = seq_table.get(SEQ_QUERY_ID)?.map(|v| v.value()).unwrap_or(0);
        }
        Ok(Self {
            wt,
            graph_id_cursor,
            vertex_id_cursor,
            property_id_cursor,
            query_id_cursor,
            scalar_cache: HashMap::new(),
            forward_cache: HashMap::new(),
            reverse_cache: HashMap::new(),
        })
    }

    /// Takes in a number of ids needed and returns the starting global id of the range.
    fn reserve_vertex_ids(&mut self, count: u64) -> u64 {
        let id = self.vertex_id_cursor;
        self.vertex_id_cursor += count;
        id
    }

    pub fn save_graphs_parallel(
        &mut self,
        builders: Vec<GraphBuilder>,
    ) -> Result<(), LatticeError> {
        // reserve ids
        let mut new_vertex_count = vec![];
        let mut ids = Vec::with_capacity(builders.len());
        for b in &builders {
            ids.push((
                {
                    let count = b.count_new_vertices();
                    new_vertex_count.push(count);
                    self.reserve_vertex_ids(count)
                }, // ids for vertexes
                if let Some(old_graph_data) = &b.old_graph_data {
                    // id for graph
                    old_graph_data.id
                } else {
                    let id = self.graph_id_cursor;
                    self.graph_id_cursor += 1;
                    id
                },
            ));
        }

        // open tables
        let mut graph_table = self.wt.open_table(GRAPHS)?;
        let mut vg_map_table = self.wt.open_table(VERTEX_GRAPH_MAP)?;

        // add new vertices to graph mappings
        for (idx, (start_id, graph_id)) in ids.iter().enumerate() {
            for v_id in *start_id..start_id + new_vertex_count[idx] {
                vg_map_table.insert(v_id, *graph_id)?;
            }
        }

        // possible future impl
        // let auto_queries = self.load_all_saved_queries()?;
        let auto_queries = vec![];

        // get graph data for every graph
        let commit_data: Vec<Result<GraphCommitData, LatticeError>> = builders
            .into_par_iter()
            .zip(ids.into_par_iter())
            .map(|(builder, (start_id, graph_id))| {
                PreparedGraph::commit_data_from_builder(builder, start_id, graph_id, &auto_queries)
            })
            .collect();

        // update cache with the graph changes
        for result in commit_data {
            let data = result?;

            // add graph
            graph_table.insert(data.graph_id, data.prepared_graph)?;

            // remove vertex to graph mappings
            for v_id in data.deleted_vertices {
                vg_map_table.remove(v_id)?;
            }

            // cache changes to the indexes
            for (vertex, property, hash) in data.add_attrs {
                Self::update_bitmap(
                    &self.wt,
                    &mut self.scalar_cache,
                    INDEX_SCALAR,
                    (property, hash),
                    vertex,
                    true,
                )?;
            }
            for (vertex, property, hash) in data.rem_attrs {
                Self::update_bitmap(
                    &self.wt,
                    &mut self.scalar_cache,
                    INDEX_SCALAR,
                    (property, hash),
                    vertex,
                    false,
                )?;
            }
            for (from, label, to) in data.add_edges {
                Self::update_bitmap(
                    &self.wt,
                    &mut self.forward_cache,
                    INDEX_FORWARD,
                    (from, label),
                    to,
                    true,
                )?;
                Self::update_bitmap(
                    &self.wt,
                    &mut self.reverse_cache,
                    INDEX_REVERSE,
                    (to, label),
                    from,
                    true,
                )?;
            }
            for (from, label, to) in data.rem_edges {
                Self::update_bitmap(
                    &self.wt,
                    &mut self.forward_cache,
                    INDEX_FORWARD,
                    (from, label),
                    to,
                    false,
                )?;
                Self::update_bitmap(
                    &self.wt,
                    &mut self.reverse_cache,
                    INDEX_REVERSE,
                    (to, label),
                    from,
                    false,
                )?;
            }
        }
        Ok(())
    }

    // helper fn to update cache bitmap
    fn update_bitmap(
        wt: &WriteTransaction,
        cache: &mut HashMap<(u64, u64), RoaringTreemap>,
        table_def: redb::TableDefinition<(u64, u64), Vec<u8>>,
        key: (u64, u64),
        id: u64,
        is_add: bool,
    ) -> Result<(), LatticeError> {
        // cache hit
        if let Some(bitmap) = cache.get_mut(&key) {
            if is_add {
                bitmap.insert(id);
            } else {
                bitmap.remove(id);
            }
            return Ok(());
        }

        // cache miss, load from db or create a new bitmap
        let mut bitmap = {
            let table = wt.open_table(table_def)?;
            if let Some(bytes) = table.get(key)? {
                RoaringTreemap::deserialize_from(&bytes.value()[..])
                    .map_err(|e| bincode::error::EncodeError::OtherString(e.to_string()))?
            } else {
                RoaringTreemap::new()
            }
        };
        if is_add {
            bitmap.insert(id);
        } else {
            bitmap.remove(id);
        }
        cache.insert(key, bitmap);
        Ok(())
    }

    pub fn commit(self) -> Result<(), LatticeError> {
        Self::commit_cache(&self.wt, self.scalar_cache, INDEX_SCALAR)?;
        Self::commit_cache(&self.wt, self.forward_cache, INDEX_FORWARD)?;
        Self::commit_cache(&self.wt, self.reverse_cache, INDEX_REVERSE)?;
        {
            let mut seq_table = self.wt.open_table(SEQUENCES)?;
            seq_table.insert(SEQ_GRAPH_ID, self.graph_id_cursor)?;
            seq_table.insert(SEQ_VERTEX_ID, self.vertex_id_cursor)?;
            seq_table.insert(SEQ_PROPERTY_ID, self.property_id_cursor)?;
            seq_table.insert(SEQ_QUERY_ID, self.query_id_cursor)?;
        }
        self.wt.commit()?;
        Ok(())
    }

    // writes cache to the table
    fn commit_cache(
        wt: &WriteTransaction,
        cache: HashMap<(u64, u64), RoaringTreemap>,
        table_def: TableDefinition<(u64, u64), Vec<u8>>,
    ) -> Result<(), LatticeError> {
        if cache.is_empty() {
            return Ok(());
        }
        let mut table = wt.open_table(table_def)?;
        let mut keys: Vec<_> = cache.keys().collect();
        keys.sort_unstable(); // prevent disk thrashing
        for key in keys {
            let bitmap = cache.get(key).unwrap();
            let mut bytes = Vec::new();
            bitmap.serialize_into(&mut bytes)?;
            // if bitmap is empty, remove it from the db
            if bitmap.is_empty() {
                table.remove(key)?;
            } else {
                table.insert(key, bytes)?;
            }
        }
        Ok(())
    }
}
