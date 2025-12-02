use std::collections::HashMap;

use bincode::{Decode, Encode};

use crate::{errors::LatticeError, properties::PropertyHandle};

use super::query_builder::*;

type NodeIdx = usize;

#[derive(Hash, PartialEq, Eq, Clone, Debug, Encode, Decode)]
pub(crate) enum Node {
    // set logic
    Union(Vec<NodeIdx>),
    Intersect(Vec<NodeIdx>),
    Difference(NodeIdx, NodeIdx),
    // search for values
    Attribute {
        attr: PropertyHandle,
        value: u64, // hashed value
    },
    // search for linked nodes
    Edge {
        dir: EdgeDirection,
        label: PropertyHandle,
        target: NodeIdx,
    },
    // saved query
    SavedQuery(u64),
}

#[derive(Encode, Decode)]
pub struct PreparedQuery {
    pub(crate) nodes: Vec<Node>,
    pub(crate) root: NodeIdx,
}

impl QueryBuilder {
    // assumes query has no dangling node links
    pub fn compile(&self) -> Result<PreparedQuery, LatticeError> {
        let root = self.root.ok_or(LatticeError::RootNotFound)?;
        let build_order = self.get_build_order(root);

        let mut output = vec![]; // ordered nodes to query for
        let mut visited = HashMap::new(); // maps self.nodes index to output index

        // extremely primitive node de-duplication (exact matches)
        // overhead may not be worth it if query is optimized
        // removes nodes with different indices but exact match on children or attributes
        let mut dup_cache = HashMap::new();

        for handle in build_order {
            let src_node = self
                .nodes
                .get(handle.0)
                .ok_or(LatticeError::VertexNotFound)?;

            let compiled_node = match src_node {
                QueryNode::Union(handles) => {
                    let mut ids = vec![];
                    for h in handles {
                        ids.push(*visited.get(&h.0.index).unwrap());
                    }
                    ids.sort_unstable(); // these lines help dup_cache
                    ids.dedup(); //
                    Node::Union(ids)
                }
                QueryNode::Intersect(handles) => {
                    let mut ids = vec![];
                    for h in handles {
                        ids.push(*visited.get(&h.0.index).unwrap());
                    }
                    ids.sort_unstable(); // these lines help dup_cache
                    ids.dedup(); //
                    Node::Intersect(ids)
                }
                QueryNode::Difference(a, b) => {
                    let a = *visited.get(&a.0.index).unwrap();
                    let b = *visited.get(&b.0.index).unwrap();
                    Node::Difference(a, b)
                }
                QueryNode::Attribute { attr, value } => {
                    let value_hash = value.hash();
                    Node::Attribute {
                        attr: *attr,
                        value: value_hash,
                    }
                }
                QueryNode::Edge { dir, label, target } => {
                    let target_id = *visited.get(&target.0.index).unwrap();
                    Node::Edge {
                        dir: *dir,
                        label: *label,
                        target: target_id,
                    }
                }
                QueryNode::SavedQuery(id) => Node::SavedQuery(*id),
            };

            let idx = if let Some(&idx) = dup_cache.get(&compiled_node) {
                idx
            } else {
                let new_idx = output.len();
                dup_cache.insert(compiled_node.clone(), new_idx);
                output.push(compiled_node);
                new_idx
            };

            // dedup visiting twice (diamond case)
            visited.insert(handle.0.index, idx);
        }

        let root_idx = *visited.get(&root.0.index).unwrap();
        Ok(PreparedQuery {
            nodes: output,
            root: root_idx,
        })
    }

    // children before parents
    fn get_build_order(&self, root: NodeHandle) -> Vec<NodeHandle> {
        let mut order = vec![];
        let mut visited = HashMap::new();
        let mut stack = vec![(root, false)];

        while let Some((handle, children_processed)) = stack.pop() {
            // nodes only visited once (prevent dup visits on diamond formations)
            if visited.contains_key(&handle.0.index) {
                continue;
            }

            if children_processed {
                visited.insert(handle.0.index, true);
                order.push(handle);
            } else {
                stack.push((handle, true)); // children will be processed

                // add children to be processed
                if let Some(node) = self.nodes.get(handle.0) {
                    match node {
                        QueryNode::Union(children) | QueryNode::Intersect(children) => {
                            for c in children {
                                stack.push((*c, false));
                            }
                        }
                        QueryNode::Difference(a, b) => {
                            stack.extend_from_slice(&[(*a, false), (*b, false)]);
                        }
                        QueryNode::Edge { target, .. } => {
                            stack.push((*target, false));
                        }
                        QueryNode::Attribute { .. } => {}
                        QueryNode::SavedQuery(_) => {}
                    }
                }
            }
        }
        order
    }
}
