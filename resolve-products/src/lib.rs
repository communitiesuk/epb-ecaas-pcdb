pub mod errors;
pub(crate) mod in_use_factors;
mod products;
mod transform;

use crate::errors::{JsonPathError, JsonValidationError, ResolvePcdbProductsError};
pub use crate::products::FuelType;
use crate::transform::{ResolveProductsResult, transform_json};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use itertools::Itertools;
use jsonpath_rust::JsonPath;
use jsonschema::validator;
use serde_json::Value as JsonValue;
use std::fmt::Debug;
use std::io::{BufReader, Cursor, Read};
use std::sync::LazyLock;

pub async fn resolve_products(
    json: impl Read,
    dynamo_client: &DynamoDbClient,
) -> ResolveProductsResult<impl Read + Debug> {
    let reader = BufReader::new(json);

    let mut input: JsonValue =
        serde_json::from_reader(reader).map_err(|_| ResolvePcdbProductsError::InvalidJson)?;

    // validate first
    if let Err(e) = CombinedSchema::validate(&input) {
        return Err(JsonValidationError::from(e).into());
    }

    transform_json(&mut input, dynamo_client).await?;

    Ok(Cursor::new(input.to_string()))
}

pub const PRODUCT_REFERENCE_FIELD: &str = "product_reference";

const ALL_PRODUCT_REFERENCE_FIELDS: [&str; 2] = [PRODUCT_REFERENCE_FIELD, "heat_network_reference"];

static PRODUCT_REFERENCE_FIELDS_JSON_PATH_QUERY: LazyLock<std::string::String> =
    LazyLock::new(|| {
        format!(
            "$..[{}]",
            ALL_PRODUCT_REFERENCE_FIELDS
                .iter()
                .map(|field| format!("'{field}'"))
                .join(",")
        )
    });

fn extract_product_references(json: &JsonValue) -> ResolveProductsResult<Vec<String>> {
    let instances = match json.query_with_path(&PRODUCT_REFERENCE_FIELDS_JSON_PATH_QUERY) {
        Ok(instances) => instances,
        Err(json_path_error) => {
            return Err(JsonPathError::from(json_path_error).into());
        }
    };

    instances
        .into_iter()
        .map(|v| -> ResolveProductsResult<String> {
            match v.val {
                JsonValue::String(value) => Ok(String::from(value)),
                value => Err(ResolvePcdbProductsError::InvalidProductReferenceJson(
                    value.to_owned(),
                )),
            }
        })
        .collect::<ResolveProductsResult<Vec<String>>>()
        // ensure we get unique product reference
        .map(|v| v.into_iter().unique().collect())
}

#[validator(path = "./src/combined_schema.json")]
struct CombinedSchema;
