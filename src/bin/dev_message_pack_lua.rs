use std::collections::BTreeMap;

use mlua::prelude::*;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(untagged)]
pub enum Document {
    Map(BTreeMap<Document, Document>),
    Array(Vec<Document>),
    I64(i64),
    F64(OrderedFloat<f64>),
    Bool(bool),
    Str(String),
}

fn main() -> anyhow::Result<()> {
    let lua = Lua::new();

    let code = r#"
        local M = {}

        M.state = {
            ["zczc"] = 123,
            [1] = 456
        }

        function M.show()
            print(M.state["zczc"])
        end

        return M
        "#;

    let tst_mod = lua.load(code).eval::<LuaTable>()?;

    let show_fn: mlua::Function = tst_mod.get("show")?;

    show_fn.call::<()>(())?;

    let state_1 = get_state(&lua, &tst_mod)?;

    println!("1: {:#?}", state_1);

    set_state(&lua, &tst_mod, &state_1)?;
    show_fn.call::<()>(())?;

    Ok(())
}

fn get_state(lua: &Lua, tst_mod: &LuaTable) -> anyhow::Result<Document> {
    let state: Document =
        lua.from_value(tst_mod.get::<mlua::Value>("state")?)?;

    Ok(state)
}

fn set_state(
    lua: &Lua,
    tst_mod: &LuaTable,
    state: &Document,
) -> anyhow::Result<()> {
    tst_mod.set("state", lua.to_value(state)?)?;
    Ok(())
}
