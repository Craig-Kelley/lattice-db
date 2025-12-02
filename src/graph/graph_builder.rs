use std::collections::HashMap;

use crate::{
    errors::LatticeError,
    graph::graph_prepared::PreparedGraph,
    properties::PropertyHandle,
    utils::{
        generational_vector::{GenVec, Handle},
        values::{Primitive, Value},
    },
};

pub(crate) type GlobalVertexId = u64;
pub(crate) type GraphId = u64;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct VertexHandle(pub(crate) Handle);

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct EdgeHandle(Handle);

pub struct VertexData {
    pub global_id: Option<GlobalVertexId>,
    pub attributes: Vec<(PropertyHandle, Primitive)>,
    pub incoming_edges: Vec<EdgeHandle>,
    pub outgoing_edges: Vec<EdgeHandle>,
}

pub struct EdgeData {
    pub from: VertexHandle,
    pub to: VertexHandle,
    pub label: PropertyHandle,
}

pub(crate) struct OldGraphData {
    pub(crate) id: GraphId,
    pub(crate) graph: PreparedGraph,
}

pub struct GraphBuilder {
    new_vertex_count: u64,
    pub(crate) old_graph_data: Option<OldGraphData>,
    pub(crate) vertices: GenVec<VertexData>,
    pub(crate) edges: GenVec<EdgeData>,
}

impl Default for GraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphBuilder {
    pub fn new_vertex<'a>(&'a mut self) -> VertexBuilder<'a> {
        self.new_vertex_count += 1;
        let handle = self.vertices.add(VertexData {
            global_id: None,
            attributes: vec![],
            incoming_edges: vec![],
            outgoing_edges: vec![],
        });
        VertexBuilder::new(self, handle)
    }

    /// Removes a vertex and all attatched edges.
    pub fn remove_vertex(&mut self, handle: VertexHandle) -> Result<(), LatticeError> {
        let removed = self
            .vertices
            .remove(handle.0)
            .ok_or(LatticeError::VertexNotFound)?;
        if removed.global_id.is_none() {
            self.new_vertex_count -= 1;
        } // removing a new vertex decreases new vertex count

        // remove all edges, ignore errors from self loop edges
        let edges = [removed.incoming_edges, removed.outgoing_edges].concat();
        for edge in edges {
            let _ = self.remove_edge(edge);
        }
        Ok(())
    }

    /// Return a VertexBuilder to edit the vertex.
    pub fn edit_vertex<'a>(
        &'a mut self,
        handle: VertexHandle,
    ) -> Result<VertexBuilder<'a>, LatticeError> {
        self.vertices
            .get(handle.0)
            .ok_or(LatticeError::VertexNotFound)?;
        Ok(VertexBuilder::new(self, handle.0))
    }

    /// Get vertex data.
    pub fn get_vertex(&self, handle: VertexHandle) -> Option<&VertexData> {
        self.vertices.get(handle.0)
    }

    /// Create a new edge to link vertices.
    pub fn new_edge(
        &mut self,
        from: VertexHandle,
        label: PropertyHandle,
        to: VertexHandle,
    ) -> Result<&mut Self, LatticeError> {
        self.vertices
            .get(from.0)
            .ok_or(LatticeError::VertexNotFound)?; // confirm 'from' exists
        let to_vertex = self
            .vertices
            .get_mut(to.0)
            .ok_or(LatticeError::VertexNotFound)?;

        // create edge
        let edge_handle = EdgeHandle(self.edges.add(EdgeData { from, label, to }));

        // add edge
        to_vertex.incoming_edges.push(edge_handle);
        let from_vertex = self.vertices.get_mut(from.0).unwrap();
        from_vertex.outgoing_edges.push(edge_handle);
        Ok(self)
    }

    /// Removes an edge from the graph.
    pub fn remove_edge(&mut self, handle: EdgeHandle) -> Result<(), LatticeError> {
        let edge = self
            .edges
            .remove(handle.0)
            .ok_or(LatticeError::EdgeNotFound)?;

        // detatch from source
        if let Some(from) = self.vertices.get_mut(edge.from.0)
            && let Some(idx) = from.outgoing_edges.iter().position(|p| *p == handle)
        {
            from.outgoing_edges.swap_remove(idx);
        }

        // detatch from destination
        if let Some(to) = self.vertices.get_mut(edge.to.0)
            && let Some(idx) = to.incoming_edges.iter().position(|p| *p == handle)
        {
            to.incoming_edges.swap_remove(idx);
        }

        Ok(())
    }

    /// Return an EdgeBuilder to edit the edge.
    pub fn edit_edge<'a>(
        &'a mut self,
        handle: EdgeHandle,
    ) -> Result<EdgeBuilder<'a>, LatticeError> {
        self.edges.get(handle.0).ok_or(LatticeError::EdgeNotFound)?;
        Ok(EdgeBuilder::new(self, handle.0))
    }

    /// Get edge data.
    pub fn get_edge(&self, handle: EdgeHandle) -> Option<&EdgeData> {
        self.edges.get(handle.0)
    }
}

pub struct EdgeBuilder<'a> {
    graph: &'a mut GraphBuilder,
    handle: Handle,
}

impl<'a> EdgeBuilder<'a> {
    fn new(graph: &'a mut GraphBuilder, handle: Handle) -> Self {
        Self { graph, handle }
    }

    fn get_self(&mut self) -> &mut EdgeData {
        self.graph.edges.get_mut(self.handle).unwrap() // must exist to be using the fn
    }

    /// Change the source (from) of the edge.
    pub fn set_source(&mut self, handle: VertexHandle) -> Result<&mut Self, LatticeError> {
        self.graph
            .edges
            .get(handle.0)
            .ok_or(LatticeError::VertexNotFound)?;
        self.get_self().from = handle;
        Ok(self)
    }

    /// Change the destination (to) of the edge.
    pub fn set_destination(&mut self, handle: VertexHandle) -> Result<&mut Self, LatticeError> {
        self.graph
            .edges
            .get(handle.0)
            .ok_or(LatticeError::VertexNotFound)?;
        self.get_self().to = handle;
        Ok(self)
    }

    /// Change the edge label.
    pub fn set_label(&mut self, label: PropertyHandle) -> Result<&mut Self, LatticeError> {
        self.get_self().label = label;
        Ok(self)
    }
}

pub struct VertexBuilder<'a> {
    graph: &'a mut GraphBuilder,
    handle: Handle,
}

impl<'a> VertexBuilder<'a> {
    fn new(graph: &'a mut GraphBuilder, handle: Handle) -> Self {
        Self { graph, handle }
    }

    fn get_self(&mut self) -> &mut VertexData {
        self.graph.vertices.get_mut(self.handle).unwrap() // must exist to be using the fn
    }

    /// Create a new attribute with a value.
    pub fn new_attribute<V: Value>(
        &mut self,
        attr: PropertyHandle,
        value: V,
    ) -> Result<&mut Self, LatticeError> {
        let v = value.to_primitive();
        v.verify()?;
        self.get_self().attributes.push((attr, v));
        Ok(self)
    }

    /// Create a new edge to link vertices.
    pub fn new_edge(
        &mut self,
        label: PropertyHandle,
        handle: VertexHandle,
    ) -> Result<&mut Self, LatticeError> {
        let to = self
            .graph
            .vertices
            .get_mut(handle.0)
            .ok_or(LatticeError::VertexNotFound)?;

        // create edge
        let edge_handle = EdgeHandle(self.graph.edges.add(EdgeData {
            from: VertexHandle(self.handle),
            label,
            to: handle,
        }));

        // add edge
        to.incoming_edges.push(edge_handle);
        self.get_self().outgoing_edges.push(edge_handle);
        Ok(self)
    }

    /// Returns the vertex handle.
    pub fn handle(&self) -> VertexHandle {
        VertexHandle(self.handle)
    }
}

impl GraphBuilder {
    pub fn get_vertex_global_id(
        &self,
        handle: VertexHandle,
    ) -> Result<Option<GlobalVertexId>, LatticeError> {
        Ok(self
            .vertices
            .get(handle.0)
            .ok_or(LatticeError::VertexNotFound)?
            .global_id)
    }

    pub fn get_mut_attributes(
        &mut self,
        handle: VertexHandle,
    ) -> Result<&mut Vec<(PropertyHandle, Primitive)>, LatticeError> {
        Ok(&mut self
            .vertices
            .get_mut(handle.0)
            .ok_or(LatticeError::VertexNotFound)?
            .attributes)
    }

    pub fn get_incoming_edges(
        &self,
        handle: VertexHandle,
    ) -> Result<&Vec<EdgeHandle>, LatticeError> {
        Ok(&self
            .vertices
            .get(handle.0)
            .ok_or(LatticeError::VertexNotFound)?
            .incoming_edges)
    }

    pub fn get_outgoing_edges(
        &self,
        handle: VertexHandle,
    ) -> Result<&Vec<EdgeHandle>, LatticeError> {
        Ok(&self
            .vertices
            .get(handle.0)
            .ok_or(LatticeError::VertexNotFound)?
            .outgoing_edges)
    }

    /// Iterate through all graph vertices.
    pub fn iter_vertices(&self) -> impl Iterator<Item = (Handle, &VertexData)> {
        self.vertices.iter()
    }

    /// Iterate through all graph edges.
    pub fn iter_edges(&self) -> impl Iterator<Item = (Handle, &EdgeData)> {
        self.edges.iter()
    }
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            new_vertex_count: 0,
            old_graph_data: None,
            vertices: GenVec::new(),
            edges: GenVec::new(),
        }
    }

    pub(crate) fn count_new_vertices(&self) -> u64 {
        self.new_vertex_count
    }

    pub(crate) fn from_prepared(graph: PreparedGraph) -> Self {
        let mut builder = Self::new();
        let mut id_map = HashMap::new(); // maybe an actual use for Rapid

        // populate vertices
        for v in &graph.vertices {
            let handle = builder.vertices.add(VertexData {
                global_id: Some(v.id),
                attributes: v.attrs.clone(),
                incoming_edges: vec![],
                outgoing_edges: vec![],
            });
            id_map.insert(v.id, VertexHandle(handle));
        }

        // populate edges
        for e in &graph.edges {
            let from = *id_map.get(&e.from).expect("Edge source vertex missing");
            let to = *id_map.get(&e.to).expect("Edge destination vertex missing");
            let handle = builder.edges.add(EdgeData {
                from,
                to,
                label: e.label,
            });
            builder
                .vertices
                .get_mut(from.0)
                .unwrap()
                .outgoing_edges
                .push(EdgeHandle(handle));
            builder
                .vertices
                .get_mut(to.0)
                .unwrap()
                .incoming_edges
                .push(EdgeHandle(handle));
        }

        builder.old_graph_data = Some(OldGraphData {
            id: graph.id,
            graph,
        });
        builder
    }
}
