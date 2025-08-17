use std::{collections::HashMap, fmt::Debug, sync::Arc};

use tokio::sync::{Mutex, RwLock};

use super::{
    event_storage::{EventPosition, EventStorage2, EventStream},
    snapshot_storage::{load_snapshot, save_snapshot, SnapshotStorage},
    try_as_ref::TryAsRef,
    Aggregate, AsyncCommandHandler, CommandHandler, Identity,
};

type AggregateMapValue<A> = Arc<Mutex<Option<AggregateState<A>>>>;

#[derive(Clone)]
pub struct Repository<Agg, EvStor, EvStorData, SnapStor>
where
    Agg: Aggregate,
    EvStorData: From<Agg::Event> + TryAsRef<Agg::Event> + Send + Sync,
    EvStor: EventStorage2<EventData = EvStorData>,
    SnapStor: SnapshotStorage,
{
    repo: Arc<RwLock<HashMap<Identity, AggregateMapValue<Agg>>>>,
    event_storage: EvStor,
    snapshot_storage: SnapStor,
    snapshoting_strategy: SnapshotingStrategy,
}

#[derive(Debug, Clone, Copy)]
pub enum SnapshotingStrategy {
    Always,
    OnePerNEvents(usize),
}

struct AggregateState<A: Aggregate> {
    data: A,
    position: Option<EventPosition>,
    processed_events: u64,
}

impl<Agg, EvStor, EvStorData, SnapStor> Repository<Agg, EvStor, EvStorData, SnapStor>
where
    Agg: Aggregate,
    EvStorData: From<Agg::Event> + TryAsRef<Agg::Event> + Send + Sync + std::fmt::Debug,
    EvStor: EventStorage2<EventData = EvStorData>,
    SnapStor: SnapshotStorage,
{
    pub fn new(
        event_storage: EvStor,
        snapshot_storage: SnapStor,
        snapshoting_strategy: SnapshotingStrategy,
    ) -> Self {
        Self {
            repo: Arc::new(RwLock::new(HashMap::new())),
            event_storage,
            snapshot_storage,
            snapshoting_strategy,
        }
    }

    async fn get_or_new(
        &self,
        id: &Identity,
    ) -> Result<AggregateMapValue<Agg>, LoadingError<EvStor>> {
        let repo = self.repo.read().await;
        if let Some(aggregate) = repo.get(id) {
            return Ok(aggregate.clone());
        }
        drop(repo);

        let mut repo = self.repo.write().await;
        if let Some(aggregate) = repo.get(id) {
            return Ok(aggregate.clone());
        }

        let aggregate = Arc::new(Mutex::new(None));
        repo.insert(id.clone(), aggregate.clone());
        let mut aggregate_instance = aggregate.lock().await;
        drop(repo);

        *aggregate_instance = Some(self.load_aggregate_state(id).await?);

        drop(aggregate_instance);
        Ok(aggregate)
    }

    async fn load_aggregate_state(
        &self,
        id: &Identity,
    ) -> Result<AggregateState<Agg>, LoadingError<EvStor>> {
        let snapshot = load_snapshot::<Agg, _>(&self.snapshot_storage, id).await;

        let (mut inst, mut position) = match snapshot {
            None => (Agg::default(), None),
            Some(snapshot) => (snapshot.state, Some(snapshot.position.next())),
        };

        let mut processed_events = 0;
        let mut stream = self
            .event_storage
            .read_stream_by_id(id, position.unwrap_or(EventPosition(0)))
            .await;
        while let Some(event) = stream
            .next()
            .await
            .map_err(|e| LoadingError::StorageError(e))?
        {
            if let Some(data) = event.data.try_as_ref() {
                inst = inst.apply(data);
                position = Some(event.pos);
                processed_events += 1;
            } else {
                return Err(LoadingError::EventConversationError);
            }
        }

        Ok(AggregateState {
            data: inst,
            position,
            processed_events,
        })
    }

    pub async fn apply_and_read<H, F, R>(
        &self,
        id: &Identity,
        input: H::Input,
        context: &H::Context,
        read_func: F,
    ) -> Result<R, ApplyError<EvStor, H>>
    where
        H: AsyncCommandHandler<Aggregate = Agg>,
        F: FnOnce(&Agg) -> R,
    {
        let agg = self.get_or_new(id).await.map_err(|e| match e {
            LoadingError::StorageError(e) => ApplyError::StorageError(e),
            LoadingError::EventConversationError => ApplyError::EventConversationError,
        })?;
        let mut agg = agg.lock().await;

        if agg.is_none() {
            return Err(ApplyError::ConsistencyError);
        }

        match H::handle_async(&agg.as_ref().unwrap().data, input, context).await {
            Ok(events) => {
                let prev = std::mem::replace(&mut *agg, None).unwrap();
                let next = prev.data.apply_all(&events);
                let read_result = read_func(&next);

                let ev_stor_events = events.into_iter().map(|e| e.into()).collect();

                match self.event_storage.append(id, ev_stor_events).await {
                    Err(e) => {
                        // в этом месте остается None в repository
                        Err(ApplyError::StorageError(e))
                    }
                    Ok(None) => {
                        panic!("Event storage should return position for non-empty events");
                    }
                    Ok(Some(pos)) => {
                        let processed_events = prev.processed_events + 1;
                        let new_state = AggregateState {
                            data: next,
                            position: Some(pos),
                            processed_events,
                        };

                        self.save_snapshot(
                            id,
                            &new_state.data,
                            new_state.position.unwrap(),
                            processed_events,
                        )
                        .await;
                        *agg = Some(new_state);
                        Ok(read_result)
                    }
                }
            }
            Err(e) => Err(ApplyError::HandlerError(e)),
        }
    }

    async fn save_snapshot(
        &self,
        id: &Identity,
        data: &Agg,
        position: EventPosition,
        processed_events: u64,
    ) {
        if let SnapshotingStrategy::OnePerNEvents(n) = self.snapshoting_strategy {
            if processed_events % n as u64 != 0 {
                return;
            }
        }

        save_snapshot::<Agg, _>(&self.snapshot_storage, id, data, position).await;
    }

    pub async fn apply<H>(
        &self,
        id: &Identity,
        input: H::Input,
        context: &H::Context,
    ) -> Result<(), ApplyError<EvStor, H>>
    where
        H: AsyncCommandHandler<Aggregate = Agg>,
    {
        self.apply_and_read(id, input, context, |_| ()).await
    }

    pub async fn read<F, R>(&self, id: &Identity, func: F) -> Result<R, ReadingError<EvStor>>
    where
        F: FnOnce(&Agg) -> R,
    {
        let agg = self.get_or_new(id).await.map_err(|e| match e {
            LoadingError::StorageError(e) => ReadingError::StorageError(e),
            LoadingError::EventConversationError => ReadingError::EventConversationError,
        })?;
        let agg = agg.lock().await;

        if let Some(agg) = agg.as_ref() {
            Ok(func(&agg.data))
        } else {
            Err(ReadingError::ConsistencyError)
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ApplyError<S: EventStorage2, H: CommandHandler> {
    #[error("consistency error")]
    ConsistencyError,
    #[error("event conversation error")]
    EventConversationError,
    #[error("storage error: {0:?}")]
    StorageError(S::Error),
    #[error("handler error: {0:?}")]
    HandlerError(H::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum LoadingError<S: EventStorage2> {
    #[error("storage error: {0:?}")]
    StorageError(S::Error),
    #[error("event conversation error")]
    EventConversationError,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadingError<S>
where
    S: EventStorage2,
{
    #[error("consistency error")]
    ConsistencyError,
    #[error("event conversation error")]
    EventConversationError,
    #[error("storage error: {0:?}")]
    StorageError(S::Error),
}

#[cfg(test)]
mod test {
    use rand::Rng;
    use serde::{Deserialize, Serialize};

    use crate::{
        event_storage::redb_event_storage::RedbEventStorage,
        snapshot_storage::redb_snapshot_storage::RedbSnapshotStorage,
        test::{
            aggregate::{Counter, CounterEvent},
            handlers::{
                decrement::{DecrementCmdHandler, SomeExternalService2},
                increment::{IncrementCmdHandler, SomeExternalService1},
            },
        },
    };

    use super::*;

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub enum AppEvent {
        CounterEvent(CounterEvent),
    }

    impl TryAsRef<CounterEvent> for AppEvent {
        fn try_as_ref(&self) -> Option<&CounterEvent> {
            match self {
                AppEvent::CounterEvent(e) => Some(e),
            }
        }
    }

    impl From<CounterEvent> for AppEvent {
        fn from(e: CounterEvent) -> Self {
            AppEvent::CounterEvent(e)
        }
    }

    #[tokio::test]
    async fn apply_est() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;

        let ev_stor: RedbEventStorage<AppEvent> =
            RedbEventStorage::new(temp_dir.path().join("ev_stor.db"), 100, None).await?;

        let snap_stor =
            RedbSnapshotStorage::new(temp_dir.path().join("snap_stor.db"), None).await?;

        let repo =
            Repository::<Counter, RedbEventStorage<AppEvent>, AppEvent, RedbSnapshotStorage>::new(
                ev_stor.clone(),
                snap_stor.clone(),
                SnapshotingStrategy::OnePerNEvents(1000),
            );

        let test_id = Identity::new("some_ns", "id")?;

        let svc1 = SomeExternalService1(1);
        let svc2 = SomeExternalService2(2.5);

        repo.apply::<IncrementCmdHandler>(&test_id, 10, &svc1)
            .await
            .unwrap();
        let state_1 = repo.read(&test_id, |agg| agg.val).await?;
        assert_eq!(state_1, 10);

        let dec_read_res = repo
            .apply_and_read::<DecrementCmdHandler, _, _>(&test_id, 5, &svc2, |state| state.val)
            .await?;
        assert_eq!(dec_read_res, 5);

        let state_2 = repo.read(&test_id, |agg| agg.val).await?;
        assert_eq!(state_2, 5);

        drop(repo);

        let repo =
            Repository::<Counter, RedbEventStorage<AppEvent>, AppEvent, RedbSnapshotStorage>::new(
                ev_stor.clone(),
                snap_stor.clone(),
                SnapshotingStrategy::Always,
            );

        let reloading_state = repo.read(&test_id, |agg| agg.val).await?;
        assert_eq!(reloading_state, 5);

        let dec_read_res = repo
            .apply_and_read::<DecrementCmdHandler, _, _>(&test_id, 4, &svc2, |state| state.val)
            .await?;
        assert_eq!(dec_read_res, 1);

        drop(repo);

        let repo =
            Repository::<Counter, RedbEventStorage<AppEvent>, AppEvent, RedbSnapshotStorage>::new(
                ev_stor.clone(),
                snap_stor.clone(),
                SnapshotingStrategy::Always,
            );

        let reloading_state = repo.read(&test_id, |agg| agg.val).await?;
        assert_eq!(reloading_state, 1);

        Ok(())
    }

    #[tokio::test]
    async fn read_empty_test() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;

        let ev_stor: RedbEventStorage<AppEvent> =
            RedbEventStorage::new(temp_dir.path().join("ev_stor.db"), 100, None).await?;

        let snap_stor =
            RedbSnapshotStorage::new(temp_dir.path().join("snap_stor.db"), None).await?;

        let repo =
            Repository::<Counter, RedbEventStorage<AppEvent>, AppEvent, RedbSnapshotStorage>::new(
                ev_stor.clone(),
                snap_stor.clone(),
                SnapshotingStrategy::Always,
            );

        let test_id = Identity::new("some_ns", "id")?;

        let loaded_val = repo.read(&test_id, |agg| agg.val).await.unwrap();
        assert_eq!(loaded_val, 0);

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn random_test() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let ev_stor_path = temp_dir.path().join("ev_stor.db");
        let snap_stor_path = temp_dir.path().join("snap_stor.db");
        let svc1 = SomeExternalService1(1);
        let svc2 = SomeExternalService2(2.5);

        let test_id = Identity::new("some_ns", "id")?;

        let mut current_res = 0;

        for _ in 0..100 {
            let ev_stor: RedbEventStorage<AppEvent> = RedbEventStorage::new(
                ev_stor_path.clone(),
                rand::rng().random_range(1..=100),
                Some(redb::Durability::None),
            )
            .await?;

            let snap_stor =
                RedbSnapshotStorage::new(snap_stor_path.clone(), Some(redb::Durability::None))
                    .await?;

            let repo = Repository::<
                Counter,
                RedbEventStorage<AppEvent>,
                AppEvent,
                RedbSnapshotStorage,
            >::new(
                ev_stor.clone(),
                snap_stor.clone(),
                if rand::random() {
                    SnapshotingStrategy::Always
                } else {
                    SnapshotingStrategy::OnePerNEvents(rand::rng().random_range(2..=100))
                },
            );

            let reader_res = repo.read(&test_id, |agg| agg.val).await?;
            assert_eq!(reader_res, current_res);

            let add_nums = rand::rng().random_range(0..=100);

            for _ in 0..add_nums {
                let n = rand::rng().random_range(-100..=100);
                if rand::random() {
                    current_res += n;
                    let res = repo
                        .apply_and_read::<IncrementCmdHandler, _, _>(&test_id, n, &svc1, |state| {
                            state.val
                        })
                        .await?;
                    assert_eq!(current_res, res);
                } else {
                    current_res -= n;
                    let res = repo
                        .apply_and_read::<DecrementCmdHandler, _, _>(&test_id, n, &svc2, |state| {
                            state.val
                        })
                        .await?;
                    assert_eq!(current_res, res);
                }
            }

            let reader_res = repo.read(&test_id, |agg| agg.val).await?;
            assert_eq!(reader_res, current_res);
        }

        Ok(())
    }
}
