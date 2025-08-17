use serde::{Deserialize, Serialize};

use super::{event_storage::EventPosition, Aggregate, Identity, SerializationVersion};

pub mod mem_snapshot_storage;
pub mod redb_snapshot_storage;

/// Хранилище снапшотов
#[async_trait::async_trait]
pub trait SnapshotStorage: Send + Sync {
    type IOError: std::error::Error + Send;

    async fn put_raw(&self, key: &str, data: Vec<u8>) -> Result<(), Self::IOError>;

    async fn get_raw(&self, key: &str) -> Result<Option<Vec<u8>>, Self::IOError>;
}

#[derive(Debug, Serialize)]
struct SnapshotSer<'a, T> {
    position: EventPosition,
    state: &'a T,
}

#[derive(Debug, Deserialize)]
pub struct Snapshot<T> {
    pub position: EventPosition,
    pub state: T,
}

async fn save_snapshot_inner<A: Aggregate, S: SnapshotStorage>(
    storage: &S,
    id: &Identity,
    state: &A,
    position: EventPosition,
) -> Result<(), SnapshotStorageError<S::IOError>> {
    let ser_data = SnapshotSer { position, state };
    let ser_data = serialize_with_ver(A::snapshot_serialization_version(), &ser_data)
        .map_err(|err| SnapshotStorageError::SerializationError(err))?;

    storage.put_raw(&id.key(), ser_data).await?;

    Ok(())
}

pub async fn save_snapshot<A: Aggregate, S: SnapshotStorage>(
    storage: &S,
    id: &Identity,
    state: &A,
    position: EventPosition,
) {
    if let Err(err) = save_snapshot_inner(storage, id, state, position).await {
        tracing::error!(?err, "Error saving snapshot");
    }
}

async fn load_snapshot_inner<A: Aggregate, S: SnapshotStorage>(
    storage: &S,
    id: &Identity,
) -> Result<DeserializeWithVer<Snapshot<A>>, SnapshotStorageError<S::IOError>> {
    let Some(ser_data) = storage.get_raw(&id.key()).await? else {
        return Ok(DeserializeWithVer::NotFound);
    };

    let des_data = deserialize_with_ver(A::snapshot_serialization_version(), &ser_data)
        .map_err(|err| SnapshotStorageError::SerializationError(err))?;

    Ok(des_data)
}

pub async fn load_snapshot<A: Aggregate, S: SnapshotStorage>(
    storage: &S,
    id: &Identity,
) -> Option<Snapshot<A>> {
    match load_snapshot_inner(storage, id).await {
        Ok(DeserializeWithVer::Data(d)) => Some(d),
        Ok(DeserializeWithVer::NotFound) => None,
        Ok(DeserializeWithVer::VersionMismatch) => {
            tracing::info!(?id, "Snapshot version mismatch");
            None
        }
        Err(err) => {
            tracing::error!(?err, "Error loading snapshot");
            None
        }
    }
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum SnapshotStorageError<IOE> {
    #[error("IO error")]
    IOError(#[from] IOE),
    #[error("Serialization error")]
    SerializationError(SerializationError),
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum SerializationError {
    #[error("Wrong length of serialized data")]
    WrongLength,
    #[error("MessagePack serialization error")]
    MessagePackEncodeFailed(Box<str>),
    #[error("MessagePack deserialize error")]
    MessagePackDecodeFailed(Box<str>),
}

fn serialize_with_ver<T: serde::Serialize>(
    version: SerializationVersion,
    data: &T,
) -> Result<Vec<u8>, SerializationError> {
    let mut buff = version.to_le_bytes().to_vec();
    rmp_serde::encode::write(&mut buff, data)
        .map_err(|e| SerializationError::MessagePackEncodeFailed(format!("{e:?}").into()))?;

    Ok(buff)
}

fn deserialize_with_ver<T: serde::de::DeserializeOwned>(
    version: SerializationVersion,
    data: &[u8],
) -> Result<DeserializeWithVer<T>, SerializationError> {
    let ver_len = std::mem::size_of::<SerializationVersion>();
    if data.len() < ver_len {
        return Err(SerializationError::WrongLength);
    }

    let data_version = &data[0..ver_len];
    let data_version = SerializationVersion::from_le_bytes(data_version.try_into().unwrap());

    if data_version != version {
        return Ok(DeserializeWithVer::VersionMismatch);
    }

    let res_data = rmp_serde::from_slice::<T>(&data[ver_len..])
        .map_err(|e| SerializationError::MessagePackDecodeFailed(format!("{e:?}").into()))?;

    Ok(DeserializeWithVer::Data(res_data))
}

#[derive(Debug, PartialEq, Eq)]
pub enum DeserializeWithVer<T> {
    NotFound,
    Data(T),
    VersionMismatch,
}

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};

    use crate::snapshot_storage::{
        deserialize_with_ver, serialize_with_ver, DeserializeWithVer, SerializationError,
    };

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
    struct TestStruct {
        pub data: u32,
    }

    #[test]
    fn serialize_with_ver_test() -> anyhow::Result<()> {
        let version = 123456789;
        let data = TestStruct { data: 987654321 };
        let buff = serialize_with_ver(version, &data).unwrap();
        assert_eq!(buff, vec![21, 205, 91, 7, 145, 206, 58, 222, 104, 177]);

        let data_2: DeserializeWithVer<TestStruct> = deserialize_with_ver(version, &buff)?;
        assert_eq!(DeserializeWithVer::Data(data), data_2);

        let data_3: DeserializeWithVer<TestStruct> = deserialize_with_ver(version + 1, &buff)?;
        assert_eq!(DeserializeWithVer::VersionMismatch, data_3);

        let wrong_len = deserialize_with_ver::<TestStruct>(version, &vec![1, 2, 3]);
        assert!(matches!(wrong_len, Err(SerializationError::WrongLength)));

        let wrong_data =
            deserialize_with_ver::<TestStruct>(version, &vec![21, 205, 91, 7, 145, 206]);
        assert!(matches!(
            wrong_data,
            Err(SerializationError::MessagePackDecodeFailed(_))
        ));

        Ok(())
    }
}
