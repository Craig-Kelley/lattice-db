use std::path::Path;

use redb::{Database, ReadableDatabase};
use tempfile::NamedTempFile;

use crate::{
    errors::LatticeError,
    lattice_db::{
        reader::LatticeReader,
        tables::{
            GRAPHS, INDEX_FORWARD, INDEX_REVERSE, INDEX_SCALAR, PROP_NAMES, PROPERTIES, QUERIES,
            QUERY_METAS, QUERY_NAMES, SEQUENCES, VERTEX_GRAPH_MAP,
        },
        writer::LatticeWriter,
    },
};

pub struct LatticeDb {
    db: Database,
}

impl LatticeDb {
    /// Creates or opens the specified file as a database.
    /// * Creates the file if it does not exist.
    /// * Returns an error if the existing file is an invalid db format.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, redb::Error> {
        let p = path.as_ref();
        let db = Database::create(p)?;
        let mut me = Self { db };
        me.init_tables()?;
        Ok(me)
    }

    /// Opens the specified existing database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, redb::Error> {
        let p = path.as_ref();
        let db = Database::open(p)?;
        Ok(Self { db })
    }

    /// Creates a temporary volatile database.
    pub fn create_temporary() -> Result<(LatticeDb, NamedTempFile), redb::Error> {
        let file = NamedTempFile::new()?;
        let db = LatticeDb::create(file.path())?;
        Ok((db, file))
    }

    // helper fn to initialize tables on startup
    fn init_tables(&mut self) -> Result<(), redb::Error> {
        let wt = self.db.begin_write()?;
        {
            let _ = wt.open_table(INDEX_SCALAR)?;
            let _ = wt.open_table(INDEX_FORWARD)?;
            let _ = wt.open_table(INDEX_REVERSE)?;
            let _ = wt.open_table(SEQUENCES)?;
            let _ = wt.open_table(GRAPHS)?;
            let _ = wt.open_table(VERTEX_GRAPH_MAP)?;
            let _ = wt.open_table(PROPERTIES)?;
            let _ = wt.open_table(PROP_NAMES)?;
            let _ = wt.open_table(QUERIES)?;
            let _ = wt.open_table(QUERY_NAMES)?;
            let _ = wt.open_table(QUERY_METAS)?;
        }
        wt.commit()?;
        Ok(())
    }

    /// Begins a write transaction.
    pub fn begin_write(&self) -> Result<LatticeWriter, LatticeError> {
        let wt = self.db.begin_write()?;
        LatticeWriter::new(wt)
    }

    /// Begins a read transaction.
    pub fn begin_read(&self) -> Result<LatticeReader, redb::Error> {
        let rt = self.db.begin_read()?;
        Ok(LatticeReader::new(rt))
    }
}
