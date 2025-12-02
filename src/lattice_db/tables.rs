use redb::TableDefinition;

// SEQUENCES (u64 Counters)
pub const SEQUENCES: TableDefinition<u8, u64> = TableDefinition::new("_lattice_seq");
pub const SEQ_GRAPH_ID: u8 = 1;
pub const SEQ_VERTEX_ID: u8 = 2;
pub const SEQ_PROPERTY_ID: u8 = 3;
pub const SEQ_QUERY_ID: u8 = 4;

// STORAGE (Blob)
// GraphId -> PreparedGraph (encoded)
pub const GRAPHS: TableDefinition<u64, Vec<u8>> = TableDefinition::new("_lattice_graphs");
// VertexId -> GraphId
pub const VERTEX_GRAPH_MAP: TableDefinition<u64, u64> =
    TableDefinition::new("_lattice_vert_graph_map");

// PROPERTIES
// PropertyId -> Metadata
pub const PROPERTIES: TableDefinition<u64, Vec<u8>> = TableDefinition::new("_lattice_props");
// PropertyName -> PropertyId
pub const PROP_NAMES: TableDefinition<&str, u64> = TableDefinition::new("_lattice_prop_name_to_id");

// QUERIES
// QueryId -> PreparedQuery (encoded)
pub const QUERIES: TableDefinition<u64, Vec<u8>> = TableDefinition::new("_lattice_saved_queries");
// QueryName (str) -> QueryId
pub const QUERY_NAMES: TableDefinition<&str, u64> = TableDefinition::new("_lattice_query_names");
// QueryId -> Metadata
pub const QUERY_METAS: TableDefinition<u64, Vec<u8>> = TableDefinition::new("_lattice_query_metas");

// INDEXES (RoaringTreemap)
// Scalar: (PropertyId, ValueHash) -> VertexId
pub const INDEX_SCALAR: TableDefinition<(u64, u64), Vec<u8>> =
    TableDefinition::new("_lattice_idx_s");
// Forward: (from VertexId, PropertyId) -> to VertexId
pub const INDEX_FORWARD: TableDefinition<(u64, u64), Vec<u8>> =
    TableDefinition::new("_lattice_idx_f");
// Reverse: (to VertexId, PropertyId) -> from VertexId
pub const INDEX_REVERSE: TableDefinition<(u64, u64), Vec<u8>> =
    TableDefinition::new("_lattice_idx_r");
