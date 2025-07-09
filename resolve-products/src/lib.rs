mod products;

use serde_json::Value as JsonValue;
use std::fmt::Debug;
use std::io::{BufReader, Cursor, Read};

pub fn resolve_products(json: impl Read) -> anyhow::Result<impl Read + Debug> {
    let reader = BufReader::new(json);

    let input: JsonValue = serde_json::from_reader(reader)?;

    Ok(Cursor::new(input.to_string()))
}
