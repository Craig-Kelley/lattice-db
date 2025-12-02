use std::io;

use bincode::error::DecodeError;
use redb::{CommitError, TransactionError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LatticeError {
    #[error("Vertex does not exist")]
    VertexNotFound,
    #[error("Edge does not exist")]
    EdgeNotFound,
    #[error("Numeric overflow: {0}")]
    NumberTooBig(String),
    #[error("Query root not assigned")]
    RootNotFound,
    #[error("Serialization error: {0}")]
    BincodeError(#[from] bincode::error::EncodeError),
    #[error("Database IO error: {0}")]
    DbError(#[from] redb::Error),
    #[error("Database table error: {0}")]
    TableError(#[from] redb::TableError),
    #[error("Storage error: {0}")]
    StorageError(#[from] redb::StorageError),
    #[error("Value missing from table")]
    MissingValue,
    #[error("IO error: {0}")]
    IOError(#[from] io::Error),
    #[error("Transaction error: {0}")]
    TransactionError(#[from] TransactionError),
    #[error("Commit error: {0}")]
    CommitError(#[from] CommitError),
    #[error("Graph does not exist")]
    GraphNotFound,
    #[error("Error decoding data")]
    DecodeError(#[from] DecodeError),
    #[error("Alias already exists")]
    AliasAlreadyExists,
    #[error("Property ID does not exist")]
    PropertyNotFound,
    #[error("Query node not found")]
    QueryNodeNotFound,
    #[error("Query not found")]
    QueryNotFound,
}
