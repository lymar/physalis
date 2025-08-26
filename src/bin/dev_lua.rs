use mlua::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

fn main() -> LuaResult<()> {
    let lua = Lua::new();

    // Иммутабельное хранилище исходников модулей (можете заменить на Arc<RwLock<..>> если нужно менять на лету)
    let store: Arc<HashMap<String, String>> = Arc::new({
        let mut m = HashMap::new();
        m.insert("a.b".into(), r#"return { x = 42 }"#.into());
        m.insert(
            "hello".into(),
            r#"return { say=function() return "hi" end }"#.into(),
        );
        m
    });

    // --- SEARCHER ---
    // (name) -> при успехе: возвращает loader-функцию (и опционально нормализованное имя);
    // при неуспехе: строку-ошибку (она попадёт в итоговое сообщение require)
    let store_for_searcher = Arc::clone(&store);
    let searcher = lua.create_function(move |lua, name: String| {
        println!("**** {name}");
        if let Some(src) = store_for_searcher.get(&name) {
            let chunk_src = src.clone();

            // loader(name) -> возвращает значение модуля (таблица/функция/…)
            let loader = lua.create_function(move |lua, _name: String| {
                // Простой путь: выполнить текст и вернуть, что вернул модуль (обычно таблицу)
                let value =
                    lua.load(&chunk_src).set_name(&_name).eval::<LuaValue>()?;
                Ok(value)
            })?;

            Ok(LuaMultiValue::from_vec(vec![
                LuaValue::Function(loader),
                LuaValue::String(lua.create_string(&name)?),
            ]))
        } else {
            let msg = format!("custom searcher: no module '{}'", name);
            Ok(LuaMultiValue::from_vec(vec![LuaValue::String(
                lua.create_string(&msg)?,
            )]))
        }
    })?;

    // Достаём package.searchers (Lua 5.2+) или package.loaders (Lua 5.1/LuaJIT)
    let package: LuaTable = lua.globals().get("package")?;
    let searchers: LuaTable = package
        .get("searchers")
        .or_else(|_| package.get("loaders"))?; // совместимость 5.1/5.2+

    // Вставляем наш searcher в начало (индекс 1), сдвигая существующие
    let len: i64 = searchers.len()?;
    for i in (1..=len).rev() {
        let v: LuaValue = searchers.get(i)?;
        searchers.set(i + 1, v)?;
    }
    searchers.set(1, searcher)?;

    // --- Демонстрация ---
    lua.load(
        r#"
        local m1 = require("a.b")
        assert(m1.x == 42)

        local m2 = require("hello")
        assert(m2.say() == "hi")
        print(m2.say())
    "#,
    )
    .exec()?;

    Ok(())
}

/*
use mlua::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

fn main() -> LuaResult<()> {
    let lua = Lua::new();

    // Иммутабельное хранилище исходников модулей (можете заменить на Arc<RwLock<..>> если нужно менять на лету)
    let store: Arc<HashMap<String, String>> = Arc::new({
        let mut m = HashMap::new();
        m.insert("a.b".into(), r#"return { x = 42 }"#.into());
        m.insert(
            "hello".into(),
            r#"return { say=function() return "hi 2" end }"#.into(),
        );
        m
    });

    // --- SEARCHER ---
    // (name) -> при успехе: возвращает loader-функцию (и опционально нормализованное имя);
    // при неуспехе: строку-ошибку (она попадёт в итоговое сообщение require)

    let searcher = lua.create_async_function(move |lua, name: String| {
        let store_for_searcher = Arc::clone(&store);
        async move {
            if let Some(src) = store_for_searcher.get(&name) {
                let chunk_src = src.clone();

                // loader(name) -> возвращает значение модуля (таблица/функция/…)
                let loader =
                    lua.create_function(move |lua, _name: String| {
                        // Простой путь: выполнить текст и вернуть, что вернул модуль (обычно таблицу)
                        let value = lua
                            .load(&chunk_src)
                            .set_name(&_name)
                            .eval::<LuaValue>()?;
                        Ok(value)
                    })?;

                Ok(LuaMultiValue::from_vec(vec![
                    LuaValue::Function(loader),
                    LuaValue::String(lua.create_string(&name)?),
                ]))
            } else {
                let msg = format!("custom searcher: no module '{}'", name);
                Ok(LuaMultiValue::from_vec(vec![LuaValue::String(
                    lua.create_string(&msg)?,
                )]))
            }
        }
    })?;

    // Достаём package.searchers (Lua 5.2+) или package.loaders (Lua 5.1/LuaJIT)
    let package: LuaTable = lua.globals().get("package")?;
    let searchers: LuaTable = package
        .get("searchers")
        .or_else(|_| package.get("loaders"))?; // совместимость 5.1/5.2+

    // Вставляем наш searcher в начало (индекс 1), сдвигая существующие
    let len: i64 = searchers.len()?;
    for i in (1..=len).rev() {
        let v: LuaValue = searchers.get(i)?;
        searchers.set(i + 1, v)?;
    }
    searchers.set(1, searcher)?;

    // --- Демонстрация ---
    lua.load(
        r#"
        local m1 = require("a.b")
        assert(m1.x == 42)

        local m2 = require("hello")
        -- assert(m2.say() == "hi")
        print(m2.say())
    "#,
    )
    .exec()?;

    Ok(())
}

 */
