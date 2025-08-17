use crate::{
    test::aggregate::{Counter, CounterEvent},
    Aggregate, CommandHandler, CommandHandlerToEvent, SyncCommandHandler,
};

#[allow(dead_code)]
#[derive(Debug)]
pub struct SomeExternalService1(pub i32);

#[derive(Debug)]
pub struct IncrementCmdHandler;

impl CommandHandler for IncrementCmdHandler {
    type Aggregate = Counter;
    type Input = i64;
    type Context = SomeExternalService1;
    type Error = IncrementError;
}

#[derive(thiserror::Error, Debug)]
pub enum IncrementError {}

impl SyncCommandHandler for IncrementCmdHandler {
    fn handle_sync(
        aggregate: &Counter,
        input: i64,
        context: &SomeExternalService1,
    ) -> Result<Vec<CommandHandlerToEvent<Self>>, Self::Error> {
        println!(
            "Increment handler: {aggregate:?}, \
                input: {input}, context: {context:?}"
        );
        Ok(vec![CounterEvent::Incremented(input)])
    }
}

#[test]
fn test_increment_handler() {
    let state = Counter { val: 0 };
    let ctx = SomeExternalService1(1);
    let events = IncrementCmdHandler::handle_sync(&state, 1, &ctx);
    let new_state = state.apply_all(&events.unwrap());
    assert_eq!(new_state.val, 1);
}

#[test]
fn test_increment_handle_and_apply() {
    let state = Counter { val: 0 };
    let ctx = SomeExternalService1(1);
    let new_state = IncrementCmdHandler::handle_sync_and_apply(state, 1, &ctx).unwrap();
    assert_eq!(new_state.val, 1);
}
