use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use super::SnapshotStorage;

#[derive(Debug, Clone)]
pub struct MemSnapshotStorage {
    pub(crate) objs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MemSnapshotStorage {
    pub fn new() -> Self {
        Self {
            objs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum MemSnapshotStorageIOError {}

#[async_trait::async_trait]
impl SnapshotStorage for MemSnapshotStorage {
    type IOError = MemSnapshotStorageIOError;

    async fn put_raw(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), Self::IOError> {
        self.objs.lock().await.insert(key.to_string(), data);
        Ok(())
    }

    async fn get_raw(
        &self,
        key: &str,
    ) -> Result<Option<Vec<u8>>, Self::IOError> {
        Ok(self.objs.lock().await.get(key).cloned())
    }
}
