use bincode::{Decode, Encode};

use crate::{
    errors::LatticeError,
    properties::PropertyHandle,
    utils::{
        generational_vector::{GenVec, Handle},
        values::{Primitive, Value},
    },
};

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug, Encode, Decode)]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
}

#[derive(Clone, Copy)]
pub struct NodeHandle(pub(crate) Handle);

pub enum QueryNode {
    // set logic
    Union(Vec<NodeHandle>),
    Intersect(Vec<NodeHandle>),
    Difference(NodeHandle, NodeHandle),
    // search for values
    Attribute {
        attr: PropertyHandle,
        value: Primitive,
    },
    // search for linked nodes
    Edge {
        dir: EdgeDirection,
        label: PropertyHandle,
        target: NodeHandle,
    },
    // saved query
    SavedQuery(u64),
}

impl QueryNode {
    pub fn attribute<V: Value>(attr: PropertyHandle, value: V) -> Self {
        Self::Attribute {
            attr,
            value: value.to_primitive(),
        }
    }
}

pub struct QueryBuilder {
    pub(crate) nodes: GenVec<QueryNode>,
    pub(crate) root: Option<NodeHandle>,
}

impl QueryBuilder {
    /// Find vertices by attribute value.
    pub fn match_attr<V: Value>(
        &mut self,
        attr: PropertyHandle,
        value: V,
    ) -> Result<NodeHandle, LatticeError> {
        let value = value.to_primitive();
        value.verify()?;
        let handle = self.nodes.add(QueryNode::Attribute { attr, value });
        Ok(NodeHandle(handle))
    }

    /// All vertices that are pointed to by `subject` via label.
    pub fn match_outgoing(
        &mut self,
        label: PropertyHandle,
        subject: NodeHandle,
    ) -> Result<NodeHandle, LatticeError> {
        self.nodes
            .get(subject.0)
            .ok_or(LatticeError::EdgeNotFound)?;
        let handle = self.nodes.add(QueryNode::Edge {
            dir: EdgeDirection::Outgoing,
            label,
            target: subject,
        });
        Ok(NodeHandle(handle))
    }

    /// All vertices that point to `target` via label.
    pub fn match_incoming(
        &mut self,
        label: PropertyHandle,
        target: NodeHandle,
    ) -> Result<NodeHandle, LatticeError> {
        self.nodes.get(target.0).ok_or(LatticeError::EdgeNotFound)?;
        let handle = self.nodes.add(QueryNode::Edge {
            dir: EdgeDirection::Incoming,
            label,
            target,
        });
        Ok(NodeHandle(handle))
    }

    /// Find a vertex that satisfies multiple features within children.
    pub fn group_and(&mut self, children: Vec<NodeHandle>) -> Result<NodeHandle, LatticeError> {
        for c in &children {
            self.nodes.get(c.0).ok_or(LatticeError::EdgeNotFound)?;
        }
        let handle = self.nodes.add(QueryNode::Intersect(children));
        Ok(NodeHandle(handle))
    }

    /// Find a vertex that satisfies one of multiple features within children.
    pub fn group_or(&mut self, children: Vec<NodeHandle>) -> Result<NodeHandle, LatticeError> {
        for c in &children {
            self.nodes.get(c.0).ok_or(LatticeError::EdgeNotFound)?;
        }
        let handle = self.nodes.add(QueryNode::Union(children));
        Ok(NodeHandle(handle))
    }

    /// Find a vertex that satisfies include, but does not satisfy exclude.
    pub fn difference(
        &mut self,
        include: NodeHandle,
        exclude: NodeHandle,
    ) -> Result<NodeHandle, LatticeError> {
        self.nodes
            .get(include.0)
            .ok_or(LatticeError::EdgeNotFound)?;
        self.nodes
            .get(exclude.0)
            .ok_or(LatticeError::EdgeNotFound)?;
        let handle = self.nodes.add(QueryNode::Difference(include, exclude));
        Ok(NodeHandle(handle))
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self {
            nodes: GenVec::new(),
            root: None,
        }
    }

    pub fn set_root(&mut self, handle: NodeHandle) {
        self.root = Some(handle);
    }

    pub fn get_root(&self) -> Option<NodeHandle> {
        self.root
    }
}
