pub mod errors;
mod products;
mod transforms;

use crate::errors::{JsonPathError, JsonValidationError, ResolvePcdbProductsError};
use crate::transforms::{ResolveProductsResult, transform_json};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use jsonpath_rust::JsonPath;
use serde_json::Value as JsonValue;
use smartstring::alias::String;
use std::fmt::Debug;
use std::io::{BufReader, Cursor, Read};

pub async fn resolve_products(
    json: impl Read,
    dynamo_client: &DynamoDbClient,
) -> ResolveProductsResult<impl Read + Debug> {
    let reader = BufReader::new(json);

    let mut input: JsonValue =
        serde_json::from_reader(reader).map_err(|_| ResolvePcdbProductsError::InvalidJson)?;

    let schema_validator = {
        let schema = serde_json::from_str(include_str!("./combined_schema.json"))
            .expect("Schema file was not parseable.");
        jsonschema::async_validator_for(&schema).await.expect(
            "Failed to create validator for schema. \
             This is a bug in resolve-products. Please report it.",
        )
    };

    // validate first
    if let Err(e) = schema_validator.validate(&input) {
        return Err(JsonValidationError::from(e).into());
    }

    transform_json(&mut input, dynamo_client).await?;

    Ok(Cursor::new(input.to_string()))
}

pub(crate) const PRODUCT_REFERENCE_FIELD: &str = "product_reference";

fn extract_product_references(json: &JsonValue) -> ResolveProductsResult<Vec<String>> {
    let instances = match json.query_with_path(&format!("$..{PRODUCT_REFERENCE_FIELD}")) {
        Ok(instances) => instances,
        Err(json_path_error) => {
            return Err(JsonPathError::from(json_path_error).into());
        }
    };

    instances
        .into_iter()
        .map(|v| -> ResolveProductsResult<String> {
            match v.val() {
                JsonValue::String(value) => Ok(String::from(value)),
                value => Err(ResolvePcdbProductsError::InvalidProductCategoryReference(
                    value.to_owned(),
                )),
            }
        })
        .collect::<ResolveProductsResult<Vec<String>>>()
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use rstest::*;
//
//     #[rstest]
//     fn test_failing_input_fails_schema_check() {
//         let input = r#"{"foo": "bar"}"#;
//         let result = resolve_products(Cursor::new(input));
//         assert!(result.is_err());
//     }
//
//     #[fixture]
//     fn heat_pump_product_ref_document() -> JsonValue {
//         serde_json::from_str(include_str!("../test/demo_heat_pump_product_ref.json")).unwrap()
//     }
//
//     #[rstest]
//     fn test_extract_product_references_from_document(heat_pump_product_ref_document: JsonValue) {
//         assert_eq!(
//             extract_product_references(&heat_pump_product_ref_document).unwrap(),
//             [String::from("HEATPUMP-MEDIUM")]
//         );
//     }
//
//     #[rstest]
//     fn test_resolve_products_produces_passing_output() {
//         let json = Cursor::new(include_str!("../test/demo_heat_pump_product_ref.json"));
//         let result = resolve_products(json);
//         assert!(result.is_ok(), "Result: {result:#?}");
//         let result_json: JsonValue =
//             serde_json::from_reader(BufReader::new(result.unwrap())).unwrap();
//         let schema = serde_json::from_str(include_str!("../test/target_schema.json"))
//             .expect("Schema file was not parseable.");
//         let validator =
//             jsonschema::validator_for(&schema).expect("Failed to create validator for schema.");
//         for error in validator.iter_errors(&result_json) {
//             eprintln!("Error: {error}");
//             eprintln!("Location: {}", error.instance_path());
//         }
//         assert!(validator.is_valid(&result_json));
//     }
// }
