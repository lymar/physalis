use tokio::{spawn, sync::mpsc};

use crate::{
    event_storage::{
        redb_event_storage::RedbEventStorage, Event, EventPosition, EventStorage2, EventStream,
    },
    test::aggregate::CounterEvent,
    Identity,
};

#[tokio::test]
async fn redb_ev_stor() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_file = temp_dir.path().join("redb_ev_stor.db");

    let ev_stor: RedbEventStorage<CounterEvent> = RedbEventStorage::new(db_file, 3, None).await?;

    let read_cache = |ev_stor: &RedbEventStorage<CounterEvent>| {
        ev_stor
            .inner
            .cache_rcv
            .borrow()
            .iter()
            .map(|(k, v)| (k.clone(), v.as_ref().clone()))
            .collect::<Vec<_>>()
    };

    let id = Identity::new("some_ns", "id_1").unwrap();

    let pos_1 = ev_stor
        .append(&id, vec![CounterEvent::Incremented(1)])
        .await?;
    assert_eq!(pos_1, Some(EventPosition(0)));

    let pos_2 = ev_stor
        .append(
            &id,
            vec![CounterEvent::Incremented(2), CounterEvent::Decremented(1)],
        )
        .await?;
    assert_eq!(pos_2, Some(EventPosition(2)));

    let cache = read_cache(&ev_stor);
    assert_eq!(
        vec![
            (
                EventPosition(0),
                Event {
                    pos: EventPosition(0),
                    id: id.clone(),
                    data: CounterEvent::Incremented(1)
                }
            ),
            (
                EventPosition(1),
                Event {
                    pos: EventPosition(1),
                    id: id.clone(),
                    data: CounterEvent::Incremented(2)
                }
            ),
            (
                EventPosition(2),
                Event {
                    pos: EventPosition(2),
                    id: id.clone(),
                    data: CounterEvent::Decremented(1)
                }
            ),
        ],
        cache
    );

    let pos_3 = ev_stor
        .append(
            &id,
            vec![CounterEvent::Incremented(3), CounterEvent::Incremented(4)],
        )
        .await?;
    assert_eq!(pos_3, Some(EventPosition(4)));

    let cache = read_cache(&ev_stor);
    assert_eq!(
        vec![
            (
                EventPosition(2),
                Event {
                    pos: EventPosition(2),
                    id: id.clone(),
                    data: CounterEvent::Decremented(1)
                }
            ),
            (
                EventPosition(3),
                Event {
                    pos: EventPosition(3),
                    id: id.clone(),
                    data: CounterEvent::Incremented(3)
                }
            ),
            (
                EventPosition(4),
                Event {
                    pos: EventPosition(4),
                    id: id.clone(),
                    data: CounterEvent::Incremented(4)
                }
            ),
        ],
        cache
    );

    Ok(())
}

async fn read_all_events_from(
    ev_stor: RedbEventStorage<CounterEvent>,
    start_pos: EventPosition,
) -> anyhow::Result<Vec<(u64, String, String, CounterEvent)>> {
    let (snd, mut rcv) = mpsc::channel(100);

    let mut res = vec![];

    let h = spawn(async move { ev_stor.read_events(start_pos, |_, _| true, snd).await });

    while let Some(e) = rcv.recv().await {
        res.push((
            e.pos.0,
            e.id.namespace.to_string(),
            e.id.id.to_string(),
            e.data.clone(),
        ));
    }

    h.await??;

    Ok(res)
}

#[tokio::test]
async fn read_ev_test() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_file = temp_dir.path().join("redb_ev_stor.db");

    let ev_stor: RedbEventStorage<CounterEvent> = RedbEventStorage::new(db_file, 3, None).await?;

    let id = Identity::new("some_ns", "id_1").unwrap();

    ev_stor
        .append(
            &id,
            vec![CounterEvent::Incremented(1), CounterEvent::Incremented(2)],
        )
        .await?;

    let evs = read_all_events_from(ev_stor.clone(), EventPosition(0)).await?;

    assert_eq!(
        evs,
        vec![
            (
                0,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(1)
            ),
            (
                1,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(2)
            )
        ]
    );

    ev_stor
        .append(&id, vec![CounterEvent::Incremented(3)])
        .await?;

    let evs = read_all_events_from(ev_stor.clone(), EventPosition(0)).await?;

    assert_eq!(
        evs,
        vec![
            (
                0,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(1)
            ),
            (
                1,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(2)
            ),
            (
                2,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(3)
            )
        ]
    );

    ev_stor
        .append(&id, vec![CounterEvent::Incremented(4)])
        .await?;

    let evs = read_all_events_from(ev_stor.clone(), EventPosition(0)).await?;

    assert_eq!(
        evs,
        vec![
            (
                0,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(1)
            ),
            (
                1,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(2)
            ),
            (
                2,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(3)
            ),
            (
                3,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(4)
            )
        ]
    );

    let evs = read_all_events_from(ev_stor.clone(), EventPosition(1)).await?;

    assert_eq!(
        evs,
        vec![
            (
                1,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(2)
            ),
            (
                2,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(3)
            ),
            (
                3,
                "some_ns".into(),
                "id_1".into(),
                CounterEvent::Incremented(4)
            )
        ]
    );

    Ok(())
}

#[tokio::test]
async fn read_ev_stream_by_id_test() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_file = temp_dir.path().join("redb_ev_stor.db");

    let ev_stor: RedbEventStorage<CounterEvent> = RedbEventStorage::new(db_file, 3, None).await?;

    let id = Identity::new("some_ns", "id_1").unwrap();

    ev_stor
        .append(
            &id,
            vec![
                CounterEvent::Incremented(1),
                CounterEvent::Incremented(2),
                CounterEvent::Incremented(3),
            ],
        )
        .await?;

    let mut reader = ev_stor.read_stream_by_id(&id, EventPosition(0)).await;
    let all_evs = reader.read_all().await?;

    assert_eq!(all_evs[0].data, CounterEvent::Incremented(1));
    assert_eq!(all_evs[1].data, CounterEvent::Incremented(2));
    assert_eq!(all_evs[2].data, CounterEvent::Incremented(3));

    Ok(())
}

#[tokio::test]
async fn listen_test() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_file = temp_dir.path().join("redb_ev_stor.db");

    let ev_stor: RedbEventStorage<CounterEvent> = RedbEventStorage::new(db_file, 3, None).await?;

    let id = Identity::new("some_ns", "id_1").unwrap();

    let mut reader_1 = ev_stor.listen(EventPosition(0), |_, _| true).await;
    let mut res_1 = vec![];

    ev_stor
        .append(&id, vec![CounterEvent::Incremented(1)])
        .await?;

    res_1.push(reader_1.next().await?.unwrap());

    ev_stor
        .append(
            &id,
            vec![
                CounterEvent::Incremented(2),
                CounterEvent::Incremented(3),
                CounterEvent::Incremented(4),
                CounterEvent::Incremented(5),
                CounterEvent::Incremented(6),
            ],
        )
        .await?;

    for _ in 0..5 {
        res_1.push(reader_1.next().await?.unwrap());
    }

    assert_eq!(res_1[0].data, CounterEvent::Incremented(1));
    assert_eq!(res_1[1].data, CounterEvent::Incremented(2));
    assert_eq!(res_1[2].data, CounterEvent::Incremented(3));
    assert_eq!(res_1[3].data, CounterEvent::Incremented(4));
    assert_eq!(res_1[4].data, CounterEvent::Incremented(5));
    assert_eq!(res_1[5].data, CounterEvent::Incremented(6));

    let mut reader_2 = ev_stor.listen(EventPosition(0), |_, _| true).await;

    ev_stor
        .append(
            &id,
            vec![CounterEvent::Incremented(7), CounterEvent::Incremented(8)],
        )
        .await?;

    for i in 0..8 {
        assert_eq!(
            reader_2.next().await?.unwrap().data,
            CounterEvent::Incremented(i + 1)
        );
    }

    Ok(())
}

#[test]
fn by_prefix_range_test() -> anyhow::Result<()> {
    use redb::{Database, TableDefinition};

    let temp_dir = tempfile::tempdir()?;
    let db_file = temp_dir.path().join("test.db");

    let tdef: TableDefinition<(&str, u64), String> = TableDefinition::new("evs");

    let db = Database::create(db_file)?;

    {
        let mut write_txn = db.begin_write()?;
        write_txn.set_durability(redb::Durability::None);
        let mut table = write_txn.open_table(tdef)?;

        for i in 0..10000 {
            table.insert(&("абвгд", i), format!("а - {i}"))?;
            table.insert(&("я", i), format!("я - {i}"))?;
        }

        drop(table);
        write_txn.commit()?;
    }

    {
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(tdef)?;

        let mut iter = table.range(("абвгд", 5)..("абвгд", 8000))?;
        for r in 5..8000 {
            let (key, value) = iter.next().unwrap()?;
            assert_eq!(key.value(), ("абвгд", r));
            assert_eq!(value.value(), format!("а - {r}"));
        }
    }

    {
        let read_txn = db.begin_read()?;
        let table = read_txn.open_table(tdef)?;

        let mut iter = table.range(("я", 5000)..("я", 8000))?;
        for r in 5000..8000 {
            let (key, value) = iter.next().unwrap()?;
            assert_eq!(key.value(), ("я", r));
            assert_eq!(value.value(), format!("я - {r}"));
        }
    }

    Ok(())
}
