use std::collections::HashMap;

use bincode::config;
use redb::ReadTransaction;
use roaring::RoaringTreemap;

use crate::{
    PreparedQuery,
    errors::LatticeError,
    graph::graph_builder::GraphBuilder,
    lattice_db::tables::{GRAPHS, INDEX_FORWARD, INDEX_REVERSE, INDEX_SCALAR, VERTEX_GRAPH_MAP},
    properties::QUERY_MATCH,
    query::{query_builder::EdgeDirection, query_prepared::Node},
};

pub struct LatticeReader {
    pub(crate) rt: ReadTransaction,
}

impl LatticeReader {
    pub(crate) fn new(rt: ReadTransaction) -> Self {
        Self { rt }
    }

    /// Return the graph each vertex id belongs to.
    pub fn get_graph_ids_from_vertices(
        &self,
        vertex_ids: &[u64],
    ) -> Result<Vec<Option<u64>>, LatticeError> {
        let table = self.rt.open_table(VERTEX_GRAPH_MAP)?;
        let mut results = Vec::with_capacity(vertex_ids.len());
        for vid in vertex_ids {
            let gid = table.get(vid)?.map(|v| v.value());
            results.push(gid);
        }
        Ok(results)
    }

    pub fn load_graph(&self, graph_id: u64) -> Result<GraphBuilder, LatticeError> {
        let table = self.rt.open_table(GRAPHS)?;
        let bytes = table
            .get(graph_id)?
            .ok_or(LatticeError::GraphNotFound)?
            .value();
        let prepared = bincode::decode_from_slice(&bytes, config::standard())?.0;
        Ok(GraphBuilder::from_prepared(prepared))
    }

    pub fn search(&self, query: &PreparedQuery) -> Result<Vec<u64>, LatticeError> {
        let mut results = HashMap::with_capacity(query.nodes.len());

        let table_scl = self.rt.open_table(INDEX_SCALAR)?;
        let table_fwd = self.rt.open_table(INDEX_FORWARD)?;
        let table_rev = self.rt.open_table(INDEX_REVERSE)?;

        for (idx, node) in query.nodes.iter().enumerate() {
            let bitmap = match node {
                Node::Union(children) => {
                    let mut res = RoaringTreemap::new();
                    for child_idx in children {
                        if let Some(child_bitmap) = results.get(child_idx) {
                            res |= child_bitmap;
                        }
                    }
                    res
                }
                Node::Intersect(children) => {
                    // get first child then intersect it sequentially with other children
                    if children.is_empty() {
                        RoaringTreemap::new()
                    } else {
                        let mut bitmaps: Vec<&RoaringTreemap> =
                            children.iter().filter_map(|id| results.get(id)).collect();

                        if bitmaps.is_empty() {
                            RoaringTreemap::new()
                        } else {
                            bitmaps.sort_by_key(|b| b.len());
                            let mut res = bitmaps[0].clone();
                            for other in &bitmaps[1..] {
                                res &= *other;
                                if res.is_empty() {
                                    break;
                                }
                            }
                            res
                        }
                    }
                }
                Node::Difference(a, b) => {
                    let a = results.get(a).unwrap();
                    let b = results.get(b).unwrap();
                    let mut res = a.clone();
                    res -= b; // subtract bitmap
                    res
                }
                Node::Attribute { attr, value } => {
                    let key = (attr.0, *value);
                    // read from the index for all vertices with the value
                    if let Some(bytes) = table_scl.get(key)? {
                        RoaringTreemap::deserialize_from(&bytes.value()[..])
                            .map_err(|e| bincode::error::EncodeError::OtherString(e.to_string()))?
                    } else {
                        RoaringTreemap::new()
                    }
                }
                Node::Edge { dir, label, target } => {
                    let ids = results.get(target).unwrap();
                    let mut res = RoaringTreemap::new();
                    let table = match dir {
                        EdgeDirection::Outgoing => &table_fwd, // find all vertices that are pointed to by target
                        EdgeDirection::Incoming => &table_rev, // find all vertices that point to target
                    };
                    for id in ids {
                        let key = (id, label.0);
                        if let Some(bytes) = table.get(key)? {
                            let connected_nodes = RoaringTreemap::deserialize_from(
                                &bytes.value()[..],
                            )
                            .map_err(|e| bincode::error::EncodeError::OtherString(e.to_string()))?;
                            res |= connected_nodes;
                        }
                    }
                    res
                }
                Node::SavedQuery(query) => {
                    // similar to attribute lookup for pre-saved queries
                    let key = (QUERY_MATCH, *query);
                    if let Some(bytes) = table_scl.get(key)? {
                        RoaringTreemap::deserialize_from(&bytes.value()[..])?
                    } else {
                        RoaringTreemap::new()
                    }
                }
            };
            results.insert(idx, bitmap);
        }
        let bitmap = results.get(&query.root).cloned().unwrap_or_default();
        Ok(bitmap.into_iter().collect())
    }
}
