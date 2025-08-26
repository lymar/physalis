use std::rc::Weak;

// struct Link {
//     from: Weak<Node>,
//     to: Weak<Node>,
// }

// struct Node {
//     uid: u64,
//     links_to: Vec<u64>,
//     // input: Vec<String>,
// }

enum SomeEvents {
    Event1,
    Event2,
}

enum SomeEvents2 {
    Event1,
    Event2,
}

fn some_reducer(
    in_1: &[SomeEvents],
    out_1: &mut Vec<SomeEvents2>, /* kv_stor ... */
) {
}

trait Node {
    fn version(&self) -> String;
    fn execute(&mut self);
}

struct Edge {}

struct SomeNode {
    in_link: InLink<SomeEvents>,
}

fn main() -> anyhow::Result<()> {
    Ok(())
}
