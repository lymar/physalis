use mlua::prelude::*;

fn main() -> anyhow::Result<()> {
    let lua = Lua::new();

    let reducer = r#"
        local M = {}

        M.state = {
            val = 0
        }

        function M.apply(ev)
            M.state.val = M.state.val + ev
            print("new state", M.state.val)
        end

        return M
        "#;

    let reducer_mod =
        lua.load(reducer).set_name("reducer").eval::<LuaTable>()?;
    let apply_fn: mlua::Function = reducer_mod.get("apply")?;

    println!("init: {:#?}", get_state(&lua, &reducer_mod)?);

    apply_fn.call::<()>(10)?;
    println!("call 1: {:#?}", get_state(&lua, &reducer_mod)?);

    apply_fn.call::<()>(5)?;
    println!("call 2: {:#?}", get_state(&lua, &reducer_mod)?);

    let saved_state = get_state(&lua, &reducer_mod)?;

    apply_fn.call::<()>(1)?;
    println!(
        "call after save state: {:#?}",
        get_state(&lua, &reducer_mod)?
    );

    set_state(&lua, &reducer_mod, &saved_state)?;
    println!("after restore state: {:#?}", get_state(&lua, &reducer_mod)?);

    apply_fn.call::<()>(10)?;
    println!(
        "call after restore state: {:#?}",
        get_state(&lua, &reducer_mod)?
    );

    Ok(())
}

fn get_state(
    lua: &Lua,
    reducer_mod: &LuaTable,
) -> anyhow::Result<serde_json::Value> {
    let state: serde_json::Value =
        lua.from_value(reducer_mod.get::<mlua::Value>("state")?)?;

    Ok(state)
}

fn set_state(
    lua: &Lua,
    reducer_mod: &LuaTable,
    state: &serde_json::Value,
) -> anyhow::Result<()> {
    reducer_mod.set("state", lua.to_value(state)?)?;
    Ok(())
}
