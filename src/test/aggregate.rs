use serde::{Deserialize, Serialize};

use crate::Aggregate;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Counter {
    pub val: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum CounterEvent {
    Incremented(i64),
    Decremented(i64),
}

impl Aggregate for Counter {
    type Event = CounterEvent;

    fn apply(self, event: &Self::Event) -> Self {
        println!("Counter apply: {event:?}");
        match event {
            CounterEvent::Incremented(v) => Self { val: self.val + v },
            CounterEvent::Decremented(v) => Self { val: self.val - v },
        }
    }
}

#[test]
fn test_counter_state() {
    let state = Counter { val: 0 };
    let events = vec![CounterEvent::Incremented(1), CounterEvent::Incremented(1)];
    let new_state = state.apply_all(&events);
    assert_eq!(new_state.val, 2);

    let events = vec![CounterEvent::Decremented(1)];
    let new_state = new_state.apply_all(&events);
    assert_eq!(new_state.val, 1);
}
