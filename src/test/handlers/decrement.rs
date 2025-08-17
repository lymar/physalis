use crate::{
    test::aggregate::{Counter, CounterEvent},
    Aggregate, AsyncCommandHandler, CommandHandler, CommandHandlerToEvent,
};

#[allow(dead_code)]
#[derive(Debug)]
pub struct SomeExternalService2(pub f32);

#[derive(Debug)]
pub struct DecrementCmdHandler;

impl CommandHandler for DecrementCmdHandler {
    type Aggregate = Counter;
    type Input = i64;
    type Context = SomeExternalService2;
    type Error = DecrementError;
}

#[derive(thiserror::Error, Debug)]
pub enum DecrementError {}

#[async_trait::async_trait]
impl AsyncCommandHandler for DecrementCmdHandler {
    async fn handle_async(
        aggregate: &Counter,
        input: i64,
        context: &SomeExternalService2,
    ) -> Result<Vec<CommandHandlerToEvent<Self>>, Self::Error> {
        println!("Decrement handler: {aggregate:?}, {input}, {context:?}");
        Ok(vec![CounterEvent::Decremented(input)])
    }
}

#[tokio::test]
async fn test_decrement_handler() {
    let state = Counter { val: 1 };
    let ctx = SomeExternalService2(123.45);
    let events = DecrementCmdHandler::handle_async(&state, 1, &ctx).await;
    let new_state = state.apply_all(&events.unwrap());
    assert_eq!(new_state.val, 0);
}

#[tokio::test]
async fn test_decrement_handle_and_apply() {
    let state = Counter { val: 1 };
    let ctx = SomeExternalService2(123.45);
    let new_state = DecrementCmdHandler::handle_async_and_apply(state, 1, &ctx)
        .await
        .unwrap();
    assert_eq!(new_state.val, 0);
}
