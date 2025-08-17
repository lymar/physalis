use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::Identity;

pub mod redb_event_storage;

/// Обертка для потока событий.
#[async_trait::async_trait]
pub trait EventStream {
    type EventData: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone;
    type Error: std::error::Error + Send + Sync;

    async fn next(
        &mut self,
    ) -> Result<Option<Arc<Event<Self::EventData>>>, Self::Error>;

    async fn read_all(
        &mut self,
    ) -> Result<Vec<Arc<Event<Self::EventData>>>, Self::Error> {
        let mut events = vec![];
        while let Some(event) = self.next().await? {
            events.push(event);
        }
        Ok(events)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event<D: Send> {
    pub pos: EventPosition,
    pub id: Identity,
    pub data: D,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
    Default,
)]
pub struct EventPosition(pub u64);

impl EventPosition {
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

/// Хранилище событий.
// #[async_trait::async_trait]
// pub trait EventStorage: Clone + Sync + Send {
//     type EventData: Serialize + for<'de> Deserialize<'de> + Send + Clone;
//     type Error: std::error::Error + Sync + Send + 'static;
//     type Stream: EventStream<EventData = Self::EventData, Error = Self::Error>;

//     async fn read_stream_by_id(
//         &self,
//         id: &Identity,
//         from_position: EventPosition,
//     ) -> Result<Self::Stream, Self::Error>;

//     async fn read_stream_by_namespace(
//         &self,
//         ns: &Namespace,
//         from_position: EventPosition,
//     ) -> Result<Self::Stream, Self::Error>;

//     async fn append(
//         &self,
//         id: &Identity,
//         events: Vec<Self::EventData>,
//     ) -> Result<EventPosition, Self::Error>;
// }

/// Хранилище событий.
#[async_trait::async_trait]
pub trait EventStorage2: Clone + Sync + Send + std::fmt::Debug {
    type EventData: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone;
    type Error: std::error::Error + Sync + Send + std::fmt::Debug + 'static;
    type Stream: EventStream<EventData = Self::EventData, Error = Self::Error>;

    /// Добавить события в хранилище.
    /// Возвращает позицию последнего события. None для передан пустой
    /// вектор.
    async fn append(
        &self,
        id: &Identity,
        events: Vec<Self::EventData>,
    ) -> Result<Option<EventPosition>, Self::Error>;

    async fn read_stream_by_id(
        &self,
        id: &Identity,
        from_position: EventPosition,
    ) -> Self::Stream;

    async fn listen<F>(
        &self,
        from_position: EventPosition,
        check_id: F,
    ) -> Self::Stream
    where
        F: Fn(&str, &str) -> bool + Send + Clone + 'static;
}
