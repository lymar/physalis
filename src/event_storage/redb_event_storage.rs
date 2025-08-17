use std::{ops::Deref, sync::Arc};

use redb::{Database, ReadableTable, Result, TableDefinition};
use rpds::RedBlackTreeMapSync;
use serde::{Deserialize, Serialize};
use tokio::{
    spawn,
    sync::{mpsc, oneshot, watch, Mutex},
    task::{spawn_blocking, JoinHandle},
};

use crate::Identity;

use super::{Event, EventPosition, EventStorage2, EventStream};

#[cfg(test)]
mod test;

pub trait EventStoreData:
    Clone + Send + Sync + std::fmt::Debug + Serialize + for<'de> Deserialize<'de> + 'static
{
}

impl<T> EventStoreData for T where
    T: Clone + Send + Sync + std::fmt::Debug + Serialize + for<'de> Deserialize<'de> + 'static
{
}

#[derive(Clone, Debug)]
pub struct RedbEventStorage<T: EventStoreData> {
    inner: Arc<RedbEventStorageInner<T>>,
}

#[derive(Debug)]
struct RedbEventStorageInner<T: EventStoreData> {
    db: Database,
    all_events_table: Box<str>,
    task_snd: mpsc::Sender<Task<T>>,
    task_rcv: Mutex<mpsc::Receiver<Task<T>>>,
    cache_max_size: usize,
    cache_snd: watch::Sender<EvCache<T>>,
    cache_rcv: watch::Receiver<EvCache<T>>,
    durability_level: Option<redb::Durability>,
}

struct Task<T> {
    id: Identity,
    data: Vec<T>,
    done: oneshot::Sender<Option<EventPosition>>,
}

type EvCache<T> = RedBlackTreeMapSync<EventPosition, Arc<Event<T>>>;

const TABLE_PREFIX: &str = "evs";

impl<T: EventStoreData> RedbEventStorage<T> {
    pub async fn new(
        db_file: std::path::PathBuf,
        cache_max_size: usize,
        durability_level: Option<redb::Durability>,
    ) -> Result<Self, RedbEventStorageError> {
        let all_events_table = format!("{}_all", TABLE_PREFIX).into_boxed_str();

        spawn_blocking(move || {
            let db = Database::create(db_file)
                .map_err(|e| RedbEventStorageError::RedbError(e.into()))?;
            let (task_snd, task_rcv) = mpsc::channel(256);
            let (cache_snd, cache_rcv) = watch::channel(Default::default());
            let this = Self {
                inner: Arc::new(RedbEventStorageInner {
                    db,
                    all_events_table,
                    task_snd,
                    task_rcv: Mutex::new(task_rcv),
                    cache_max_size,
                    cache_snd,
                    cache_rcv,
                    durability_level,
                }),
            };

            let mut write_txn = this
                .inner
                .db
                .begin_write()
                .map_err(|e| RedbEventStorageError::RedbError(e.into()))?;
            if let Some(dl) = this.inner.durability_level {
                write_txn.set_durability(dl);
            }
            write_txn
                .open_table(this.all_evs_table_def())
                .map_err(|e| RedbEventStorageError::RedbError(e.into()))?;
            write_txn
                .commit()
                .map_err(|e| RedbEventStorageError::RedbError(e.into()))?;

            Ok(this)
        })
        .await
        .map_err(|e| RedbEventStorageError::Other(e.into()))?
    }

    fn all_evs_table_def<'a>(&self) -> TableDefinition<(u64, &'a str, &'a str), &'a [u8]> {
        TableDefinition::new(&self.inner.all_events_table)
    }

    fn do_append(self) -> Result<(), RedbEventStorageError> {
        let mut write_txn = self.inner.db.begin_write().map_err(to_db_err)?;
        if let Some(dl) = self.inner.durability_level {
            write_txn.set_durability(dl);
        }

        let mut table = write_txn
            .open_table(self.all_evs_table_def())
            .map_err(to_db_err)?;

        let next_id = if let Some((last_k, _)) = table.last().map_err(to_db_err)? {
            last_k.value().0 + 1
        } else {
            0
        };
        let mut pos = EventPosition(next_id);

        let mut done = vec![];

        let mut task_rcv = self.inner.task_rcv.blocking_lock();

        let mut cache = self.inner.cache_snd.borrow().clone();

        loop {
            match task_rcv.try_recv() {
                Ok(task) => {
                    let mut latest_pos = None;
                    for e in task.data {
                        let key: (u64, &str, &str) = (pos.0, &task.id.namespace, &task.id.id);
                        let data = bincode::serde::encode_to_vec(&e, bincode::config::standard())?;
                        table.insert(&key, data.as_slice()).map_err(to_db_err)?;

                        cache.insert_mut(
                            pos,
                            Arc::new(Event {
                                pos,
                                id: task.id.clone(),
                                data: e,
                            }),
                        );
                        latest_pos = Some(pos);
                        pos = pos.next();
                    }
                    done.push((task.done, latest_pos));

                    while cache.size() > self.inner.cache_max_size {
                        let oldest = cache.first().unwrap().0.clone();
                        cache.remove_mut(&oldest);
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    break;
                }
                Err(e) => {
                    return Err(RedbEventStorageError::Other(e.into()));
                }
            }
        }

        drop(table);

        write_txn.commit().map_err(to_db_err)?;

        self.inner.cache_snd.send_replace(cache);

        for d in done {
            let _ = d.0.send(d.1);
        }

        Ok(())
    }

    /// Reads events from cache or database
    /// Returns latest read position or None if no events were read
    async fn read_events<F>(
        self,
        start_pos: EventPosition,
        check_id: F,
        result_ch: mpsc::Sender<Arc<Event<T>>>,
    ) -> Result<Option<EventPosition>, RedbEventStorageError>
    where
        F: Fn(&str, &str) -> bool + Send + 'static,
    {
        let cache = self.inner.cache_rcv.borrow().clone();

        let first = cache.first();
        if let Some((cache_first, _)) = first {
            // Events are in cache
            if start_pos >= *cache_first {
                let mut current_pos = None;
                for elem in cache.range(start_pos..) {
                    current_pos = Some(*elem.0);
                    if check_id(&elem.1.id.namespace, &elem.1.id.id) {
                        result_ch.send(elem.1.clone()).await.map_err(to_err)?;
                    }
                }
                return Ok(current_pos);
            }
        }
        drop(cache);

        let latest_db_read_pos =
            spawn_blocking(move || self.read_db_events(start_pos, check_id, result_ch))
                .await
                .map_err(to_err)??;

        Ok(latest_db_read_pos)
    }

    /// Reads events from the database
    /// Returns latest read position or None if no events were read
    fn read_db_events<F>(
        self,
        start_pos: EventPosition,
        check_id: F,
        result_ch: mpsc::Sender<Arc<Event<T>>>,
    ) -> Result<Option<EventPosition>, RedbEventStorageError>
    where
        F: Fn(&str, &str) -> bool,
    {
        let read_txn = self.inner.db.begin_read().map_err(to_db_err)?;
        let table = read_txn
            .open_table(self.all_evs_table_def())
            .map_err(to_db_err)?;

        let mut iter = table
            .range((start_pos.0, "", "")..(u64::MAX, "", ""))
            .map_err(to_db_err)?;

        let mut latest_read_pos = None;

        loop {
            match iter.next() {
                None => return Ok(latest_read_pos),
                Some(Err(e)) => return Err(RedbEventStorageError::RedbError(e.into())),
                Some(Ok((key, val))) => {
                    let key = key.value();
                    latest_read_pos = Some(EventPosition(key.0));
                    if check_id(key.1, key.2) {
                        let (data, _) = bincode::serde::decode_from_slice::<T, _>(
                            val.value(),
                            bincode::config::standard(),
                        )?;

                        result_ch
                            .blocking_send(Arc::new(Event {
                                pos: EventPosition(key.0),
                                id: Identity::new(key.1, key.2).map_err(to_err)?,
                                data,
                            }))
                            .map_err(to_err)?;
                    }
                }
            }
        }
    }

    async fn listen_events<F>(
        self,
        start_pos: EventPosition,
        check_id: F,
        result_ch: mpsc::Sender<Arc<Event<T>>>,
    ) -> Result<(), RedbEventStorageError>
    where
        F: Fn(&str, &str) -> bool + Send + Clone + 'static,
    {
        let mut cache_read = self.inner.cache_snd.subscribe();
        let mut latest_pos = self
            .clone()
            .read_events(start_pos, check_id.clone(), result_ch.clone())
            .await?;
        loop {
            cache_read.changed().await.map_err(to_err)?;
            let start_pos = match latest_pos {
                Some(p) => p.next(),
                None => start_pos,
            };
            latest_pos = self
                .clone()
                .read_events(start_pos, check_id.clone(), result_ch.clone())
                .await?;
        }
    }
}

#[async_trait::async_trait]
impl<T: EventStoreData> EventStorage2 for RedbEventStorage<T> {
    type EventData = T;
    type Error = RedbEventStorageError;
    type Stream = RedbEventStorageStream<T>;

    async fn append(
        &self,
        id: &Identity,
        events: Vec<Self::EventData>,
    ) -> Result<Option<EventPosition>, Self::Error> {
        let (done_snd, done_rcv) = oneshot::channel();

        self.inner
            .task_snd
            .send(Task {
                id: id.clone(),
                data: events,
                done: done_snd,
            })
            .await
            .map_err(to_err)?;

        let self = self.clone();

        spawn_blocking(move || self.do_append())
            .await
            .map_err(to_err)??;

        let ep = done_rcv.await.map_err(to_err)?;

        Ok(ep)
    }

    async fn read_stream_by_id(&self, id: &Identity, from_position: EventPosition) -> Self::Stream {
        let (snd, rcv) = mpsc::channel::<Arc<Event<Self::EventData>>>(256);

        let self_ = self.clone();
        let id = id.clone();
        let task = spawn(async move {
            self_
                .read_events(
                    from_position,
                    move |tns: &str, tid: &str| tns == id.namespace.deref() && tid == id.id.deref(),
                    snd,
                )
                .await
                .map(|_| ())
        });

        Self::Stream {
            task: Some(task),
            result_ch: rcv,
        }
    }

    async fn listen<F>(&self, from_position: EventPosition, check_id: F) -> Self::Stream
    where
        F: Fn(&str, &str) -> bool + Send + Clone + 'static,
    {
        let (snd, rcv) = mpsc::channel::<Arc<Event<Self::EventData>>>(256);

        let self_ = self.clone();

        let task = spawn(async move { self_.listen_events(from_position, check_id, snd).await });

        Self::Stream {
            task: Some(task),
            result_ch: rcv,
        }
    }
}

pub struct RedbEventStorageStream<T: EventStoreData> {
    task: Option<JoinHandle<Result<(), RedbEventStorageError>>>,
    result_ch: mpsc::Receiver<Arc<Event<T>>>,
}

#[async_trait::async_trait]
impl<D> EventStream for RedbEventStorageStream<D>
where
    D: EventStoreData,
{
    type EventData = D;
    type Error = RedbEventStorageError;

    async fn next(&mut self) -> Result<Option<Arc<Event<Self::EventData>>>, Self::Error> {
        match self.result_ch.recv().await {
            Some(e) => Ok(Some(e)),
            None => {
                if let Some(task) = self.task.take() {
                    task.await.map_err(to_err)??;
                }
                Ok(None)
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RedbEventStorageError {
    #[error("redb error: {0}")]
    RedbError(#[from] redb::Error),
    #[error("event encode error: {0}")]
    EncodeError(#[from] bincode::error::EncodeError),
    #[error("event decode error: {0}")]
    DecodeError(#[from] bincode::error::DecodeError),
    #[error("other error: {0}")]
    Other(anyhow::Error),
}

fn to_err<T: Into<anyhow::Error>>(e: T) -> RedbEventStorageError {
    RedbEventStorageError::Other(e.into())
}

fn to_db_err<T: Into<redb::Error>>(e: T) -> RedbEventStorageError {
    RedbEventStorageError::RedbError(e.into())
}
