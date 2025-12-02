use std::{collections::HashMap, mem};

use bincode::{Decode, Encode, config};

use crate::{
    PreparedQuery,
    errors::LatticeError,
    graph::graph_builder::GraphBuilder,
    graph_builder::{GlobalVertexId, GraphId},
    properties::PropertyHandle,
    utils::{generational_vector::Handle, values::Primitive},
};

#[derive(Encode, Decode)]
pub(crate) struct PreparedVertex {
    pub(crate) id: GlobalVertexId,
    pub(crate) attrs: Vec<(PropertyHandle, Primitive)>,
}

#[derive(Encode, Decode, PartialEq)]
pub(crate) struct PreparedEdge {
    pub(crate) from: GlobalVertexId,
    pub(crate) label: PropertyHandle,
    pub(crate) to: GlobalVertexId,
}

#[derive(Encode, Decode)]
pub(crate) struct PreparedGraph {
    pub(crate) id: GraphId,
    pub(crate) vertices: Vec<PreparedVertex>,
    pub(crate) edges: Vec<PreparedEdge>,
}

#[derive(Debug)]
pub(crate) struct GraphCommitData {
    pub(crate) graph_id: u64,
    pub(crate) prepared_graph: Vec<u8>, // serialized PreparedGraph
    pub(crate) add_attrs: Vec<(u64, u64, u64)>, // (vertex id, prop id, value hash)
    pub(crate) rem_attrs: Vec<(u64, u64, u64)>,
    pub(crate) add_edges: Vec<(u64, u64, u64)>, // (from, label, to)
    pub(crate) rem_edges: Vec<(u64, u64, u64)>,
    pub(crate) deleted_vertices: Vec<u64>, // vertex id
}

impl PreparedGraph {
    // relies on GraphBuilder populating the vertices and edges in the same order as the PreparedGraph's edges and vertices
    pub(crate) fn commit_data_from_builder(
        graph: GraphBuilder,
        start_id: u64,
        graph_id: u64,
        _auto_queries: &[PreparedQuery], // future implementation to add a query check to an item automatically
    ) -> Result<GraphCommitData, LatticeError> {
        let mut global_id_cursor = start_id;
        let GraphBuilder {
            mut vertices,
            edges,
            old_graph_data,
            ..
        } = graph;
        let (old_vertices, old_edges) = if let Some(ogd) = old_graph_data {
            (Some(ogd.graph.vertices), Some(ogd.graph.edges))
        } else {
            (None, None)
        };

        let mut add_attrs = vec![];
        let mut rem_attrs = vec![];
        let mut add_edges = vec![];
        let mut rem_edges = vec![];
        let mut deleted_vertices = vec![];

        let mut proc_vertices = vec![];
        let mut proc_edges = vec![];
        let mut new_vertices_start = 0;

        // maps vertex GenVec index to global id
        let mut idx_to_global = HashMap::new();

        // iterates through vertices that might be old
        if let Some(mut old_vertices) = old_vertices {
            new_vertices_start = old_vertices.len();

            // iterate through the vertexes that might be continued from the old_graph
            for (idx, old_vertex) in old_vertices.iter_mut().enumerate() {
                if let Some(continued_vertex) = vertices.get_mut(Handle {
                    generation: 0,
                    index: idx,
                }) {
                    // vertex was not deleted
                    if continued_vertex.attributes == old_vertex.attrs {
                        // vertex was unchanged
                        idx_to_global.insert(idx, continued_vertex.global_id.unwrap());
                        proc_vertices.push(PreparedVertex {
                            id: continued_vertex.global_id.unwrap(),
                            attrs: mem::take(&mut old_vertex.attrs),
                        });
                    } else {
                        // vertex was changed
                        let mut new_attrs = mem::take(&mut continued_vertex.attributes);
                        new_attrs.sort_unstable_by_key(|(attr, val)| (attr.0, val.hash())); // sort by attribute id

                        let global_id = continued_vertex.global_id.unwrap();

                        // find which attributes were changed
                        let mut old_iter = old_vertex.attrs.iter().peekable();
                        let mut new_iter = new_attrs.iter().peekable();
                        loop {
                            match (old_iter.peek(), new_iter.peek()) {
                                // both iters done
                                (None, None) => break,
                                // no more new attrs, so remaining old attrs were removed
                                (Some((old_attr, old_val)), None) => {
                                    rem_attrs.push((global_id, old_attr.0, old_val.hash()));
                                    old_iter.next();
                                }
                                // no more old attrs, so remaining new attrs were added
                                (None, Some((new_attr, new_val))) => {
                                    add_attrs.push((global_id, new_attr.0, new_val.hash()));
                                    new_iter.next();
                                }
                                // compare attrs
                                (Some((old_attr, old_val)), Some((new_attr, new_val))) => {
                                    let old_key = (old_attr.0, old_val.hash());
                                    let new_key = (new_attr.0, new_val.hash());

                                    if old_key == new_key {
                                        // old and new attrs are the same, so the attr was unchanged
                                        old_iter.next();
                                        new_iter.next();
                                    } else if old_key < new_key {
                                        // old attr doesn't have a match (new attr past match value, so no match value exists), so old value was removed
                                        rem_attrs.push((global_id, old_attr.0, old_val.hash()));
                                        old_iter.next();
                                    } else {
                                        // new attr doesn't have a match (old attr past match value, so no match value exists), so new value was added
                                        add_attrs.push((global_id, new_attr.0, new_val.hash()));
                                        new_iter.next();
                                    }
                                }
                            }
                        }

                        idx_to_global.insert(idx, global_id);
                        proc_vertices.push(PreparedVertex {
                            id: global_id,
                            attrs: new_attrs,
                        });
                    }
                } else {
                    // vertex was deleted
                    deleted_vertices.push(old_vertex.id);
                    for (attr, value) in &old_vertex.attrs {
                        rem_attrs.push((old_vertex.id, attr.0, value.hash())); // remove all old attributes
                    }
                    if let Some(new_vertex) = vertices.get_mut_index(idx) {
                        // new vertex created in freed slot
                        let mut new_attrs = mem::take(&mut new_vertex.attributes);
                        new_attrs.sort_unstable_by_key(|(attr, val)| (attr.0, val.hash()));
                        for (attr, value) in &new_attrs {
                            add_attrs.push((global_id_cursor, attr.0, value.hash())); // add all new attributes
                        }
                        idx_to_global.insert(idx, global_id_cursor);
                        proc_vertices.push(PreparedVertex {
                            id: global_id_cursor,
                            attrs: new_attrs,
                        });
                        global_id_cursor += 1; // next new vertex will have a new global id
                    }
                }
            }
        }

        // iterates through vertices that are guarenteed new
        for (h, new_vertex) in vertices.iter_mut_from(new_vertices_start) {
            let global_id = global_id_cursor;
            global_id_cursor += 1;

            let mut new_attrs = mem::take(&mut new_vertex.attributes);
            new_attrs.sort_unstable_by_key(|(attr, val)| (attr.0, val.hash()));
            for (attr, value) in &new_attrs {
                add_attrs.push((global_id, attr.0, value.hash())); // add all new attributes
            }

            idx_to_global.insert(h.index, global_id);
            proc_vertices.push(PreparedVertex {
                id: global_id,
                attrs: new_attrs,
            });
        }

        // iterates through edges that might be old
        let mut new_edges_start = 0;
        if let Some(old_edges) = old_edges {
            new_edges_start = old_edges.len();

            // iterate through the edges that might be continued from the old_graph
            for (idx, old_edge) in old_edges.iter().enumerate() {
                if let Some(continued_edge) = edges.get(Handle {
                    generation: 0,
                    index: idx,
                }) {
                    // get global ids
                    let from = *idx_to_global
                        .get(&continued_edge.from.0.index)
                        .expect("Source vertex missing");
                    let to = *idx_to_global
                        .get(&continued_edge.to.0.index)
                        .expect("Destination vertex missing");
                    let current_label = continued_edge.label.0;

                    // compare global ids
                    let unchanged = from == old_edge.from
                        && current_label == old_edge.label.0
                        && to == old_edge.to;

                    if unchanged {
                        // edge was unchanged
                        proc_edges.push(PreparedEdge {
                            from: old_edge.from,
                            label: old_edge.label,
                            to: old_edge.to,
                        });
                    } else {
                        // edge was changed
                        rem_edges.push((old_edge.from, old_edge.label.0, old_edge.to)); // remove old edge
                        add_edges.push((from, current_label, to)); // add new edge
                        proc_edges.push(PreparedEdge {
                            from,
                            label: continued_edge.label,
                            to,
                        });
                    }
                } else {
                    // edge was delted
                    rem_edges.push((old_edge.from, old_edge.label.0, old_edge.to));

                    if let Some(new_edge) = edges.get_index(idx) {
                        // new edge created in freed slot
                        let from = *idx_to_global
                            .get(&new_edge.from.0.index)
                            .expect("Source vertex missing");
                        let to = *idx_to_global
                            .get(&new_edge.to.0.index)
                            .expect("Destination vertex missing");
                        add_edges.push((from, new_edge.label.0, to));
                        proc_edges.push(PreparedEdge {
                            from,
                            label: new_edge.label,
                            to,
                        });
                    }
                }
            }
        }

        // iterates through edges that are guarenteed new
        for (_, new_edge) in edges.iter_from(new_edges_start) {
            let from = *idx_to_global
                .get(&new_edge.from.0.index)
                .expect("Source vertex missing");
            let to = *idx_to_global
                .get(&new_edge.to.0.index)
                .expect("Destination vertex missing");
            add_edges.push((from, new_edge.label.0, to));
            proc_edges.push(PreparedEdge {
                from,
                label: new_edge.label,
                to,
            });
        }

        // return computed changes
        let prepared_graph = bincode::encode_to_vec(
            PreparedGraph {
                id: graph_id,
                vertices: proc_vertices,
                edges: proc_edges,
            },
            config::standard(),
        )?;
        Ok(GraphCommitData {
            graph_id,
            prepared_graph,
            add_attrs,
            rem_attrs,
            add_edges,
            rem_edges,
            deleted_vertices,
        })
    }
}
