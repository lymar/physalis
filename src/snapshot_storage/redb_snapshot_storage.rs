use std::sync::Arc;

use redb::{Database, TableDefinition};
use tokio::task::spawn_blocking;

use super::SnapshotStorage;

const TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("snapshots");

#[derive(Debug, Clone)]
pub struct RedbSnapshotStorage {
    db: Arc<Database>,
    durability_level: Option<redb::Durability>,
}

impl RedbSnapshotStorage {
    pub async fn new(
        db_file: std::path::PathBuf,
        durability_level: Option<redb::Durability>,
    ) -> anyhow::Result<Self> {
        let db = spawn_blocking(move || {
            anyhow::Result::<Arc<Database>>::Ok(Arc::new(Database::create(
                db_file,
            )?))
        })
        .await??;

        {
            let db = Arc::clone(&db);
            spawn_blocking(|| init_db(db)).await??;
        }

        Ok(Self {
            db,
            durability_level,
        })
    }
}

fn init_db(db: Arc<Database>) -> anyhow::Result<()> {
    let write_txn = db.begin_write()?;

    let table = write_txn.open_table(TABLE)?;
    drop(table);

    write_txn.commit()?;

    Ok(())
}

fn write(
    db: Arc<Database>,
    key: &str,
    data: Vec<u8>,
    durability_level: Option<redb::Durability>,
) -> Result<(), redb::Error> {
    let mut write_txn = db.begin_write()?;

    if let Some(dl) = durability_level {
        write_txn.set_durability(dl);
    }

    {
        let mut table = write_txn.open_table(TABLE)?;

        table.insert(key, data)?;
    }

    write_txn.commit()?;
    Ok(())
}

fn read(db: Arc<Database>, key: &str) -> Result<Option<Vec<u8>>, redb::Error> {
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;

    let data = table.get(key)?;
    if let Some(data) = data {
        Ok(Some(data.value()))
    } else {
        Ok(None)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RedbSnapshotStorageIOError {
    #[error("redb error: {0}")]
    RedbError(#[from] redb::Error),
    #[error("Other error")]
    Other,
}

#[async_trait::async_trait]
impl SnapshotStorage for RedbSnapshotStorage {
    type IOError = RedbSnapshotStorageIOError;

    async fn put_raw(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), Self::IOError> {
        let db = self.db.clone();
        let key = key.to_owned();
        let dl = self.durability_level;
        spawn_blocking(move || write(db, &key, data, dl))
            .await
            .map_err(|_| RedbSnapshotStorageIOError::Other)??;
        Ok(())
    }

    async fn get_raw(
        &self,
        key: &str,
    ) -> Result<Option<Vec<u8>>, Self::IOError> {
        let db = self.db.clone();
        let key = key.to_owned();
        Ok(spawn_blocking(move || read(db, &key))
            .await
            .map_err(|_| RedbSnapshotStorageIOError::Other)??)
    }
}

#[tokio::test]
async fn init_db_test() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_file = temp_dir.path().join("test.db");
    RedbSnapshotStorage::new(db_file, None).await?;

    Ok(())
}

#[tokio::test]
async fn rw_db_test() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_file = temp_dir.path().join("test.db");
    let db = RedbSnapshotStorage::new(db_file, None).await?;

    let not_exists = db.get_raw("some/key").await?;
    assert_eq!(not_exists, None);

    db.put_raw("some/key", vec![1, 2, 3]).await?;
    let data = db.get_raw("some/key").await?;
    assert_eq!(data, Some(vec![1, 2, 3]));

    Ok(())
}
