use serde::{Deserialize, Serialize};

pub mod error_notifier;
pub mod event_storage;
pub mod identity;
pub mod repository;
pub mod snapshot_storage;
#[cfg(test)]
mod test;
pub mod try_as_ref;

pub use identity::Identity;

/// Базовый трейт для агрегата. Вернее для его главного состояния.
/// Используем event sourcing, состояние агрегата меняется только
///  через применение событий.
/// Посылкой сообщение занимаются команды.
pub trait Aggregate:
    Sized + Default + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>
{
    type Event: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone;

    // TODO: тут надо всё-таки вычислять какой-то хеш, иначе буду
    // постоянно натыкаться на несовпадение версий. Особенно если не
    // serde использовать, а bincode. Можно тупо хеш файла при билде
    // считать.
    // https://github.com/GREsau/schemars
    fn snapshot_serialization_version() -> SerializationVersion {
        1
    }

    fn apply(self, event: &Self::Event) -> Self;

    fn apply_all<'a, I>(self, events: I) -> Self
    where
        I: IntoIterator<Item = &'a Self::Event> + 'a,
        Self::Event: 'a,
    {
        events
            .into_iter()
            .fold(self, |state, event| state.apply(&event))
    }

    fn new_from_events<'a, I>(events: I) -> Self
    where
        I: IntoIterator<Item = &'a Self::Event> + 'a,
        Self::Event: 'a,
    {
        Self::default().apply_all(events)
    }
}

pub type SerializationVersion = u32;

/// Базовый трейт для обработчика команды. Каждый обработчик имеет
/// собственный вход, контекст и тип ошибки.
pub trait CommandHandler {
    type Aggregate: Aggregate;
    type Input: Send;
    type Context;
    type Error: std::error::Error + Send;
}

// https://github.com/rust-lang/rust/issues/38078#issuecomment-1672202401
pub type CommandHandlerToEvent<A> = <<A as CommandHandler>::Aggregate as Aggregate>::Event;

/// Синхронный обработчик команды.
pub trait SyncCommandHandler: CommandHandler {
    fn handle_sync(
        aggregate: &Self::Aggregate,
        input: Self::Input,
        context: &Self::Context,
    ) -> Result<Vec<CommandHandlerToEvent<Self>>, Self::Error>;

    fn handle_sync_and_apply(
        aggregate: Self::Aggregate,
        input: Self::Input,
        context: &Self::Context,
    ) -> Result<Self::Aggregate, Self::Error> {
        let events = Self::handle_sync(&aggregate, input, context)?;
        Ok(aggregate.apply_all(&events))
    }
}

/// Асинхронный обработчик команды.
#[async_trait::async_trait]
pub trait AsyncCommandHandler: CommandHandler {
    async fn handle_async(
        aggregate: &Self::Aggregate,
        input: Self::Input,
        context: &Self::Context,
    ) -> Result<Vec<CommandHandlerToEvent<Self>>, Self::Error>;

    async fn handle_async_and_apply(
        aggregate: Self::Aggregate,
        input: Self::Input,
        context: &Self::Context,
    ) -> Result<Self::Aggregate, Self::Error>
    where
        Self::Aggregate: Send,
        Self::Input: Send,
        Self::Context: Send + Sync,
    {
        let events = Self::handle_async(&aggregate, input, &context).await?;
        Ok(aggregate.apply_all(&events))
    }
}

/// Синхронный обработчик можно обернуть в асинхронный
#[async_trait::async_trait]
impl<A: SyncCommandHandler<Aggregate: Send + Sync, Context: Send + Sync>> AsyncCommandHandler
    for A
{
    async fn handle_async(
        aggregate: &Self::Aggregate,
        input: Self::Input,
        context: &Self::Context,
    ) -> Result<Vec<CommandHandlerToEvent<Self>>, Self::Error> {
        A::handle_sync(aggregate, input, context)
    }
}
