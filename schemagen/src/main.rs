#![recursion_limit = "256"]

use clap::Parser;
use json_patch::Patch;
use serde_json::{Value as JsonValue};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    schema_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let Args { schema_url } = args;

    let mut schema = reqwest::get(schema_url).await?.json::<JsonValue>().await?;

    let patches: JsonValue = serde_json::from_str(include_str!("schema_patches.json"))?;

    for (_key, patch_json) in patches.as_object().unwrap() {
        let patch: Patch = serde_json::from_value(patch_json.clone())?;
        json_patch::patch(&mut schema, &patch)?;
    }

    println!("{}", serde_json::to_string_pretty(&schema)?);

    Ok(())
}
