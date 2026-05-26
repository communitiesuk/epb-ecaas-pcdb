use itertools::Itertools;
use jsonschema::ValidationError;
use resolve_products::PRODUCT_REFERENCE_FIELD;
use resolve_products::errors::ResolvePcdbProductsError;
use rstest::rstest;
use serde_json::{Value, from_str, json, to_string};
use std::io::Cursor;
use std::sync::LazyLock;

mod common;

static TARGET_SCHEMA: LazyLock<Value> =
    LazyLock::new(|| from_str(include_str!("fixtures/target_schema.json")).unwrap());

const INPUT_WITH_PRODUCT_REFS: &str = include_str!("fixtures/input_with_product_refs.json");

async fn validate_against_target_schema(input: &Value) -> Result<(), ValidationError<'_>> {
    let schema_validator = jsonschema::async_validator_for(&TARGET_SCHEMA).await?;

    schema_validator.validate(input)
}

// TODO add radiator to input_with_product_refs once wet distribution thermal mass issue (reported to DESNZ) has been resolved
#[tokio::test]
#[rstest]
#[case(include_str!("fixtures/demo_fhs.json"), include_str!("fixtures/demo_fhs.json"))]
#[case(INPUT_WITH_PRODUCT_REFS, include_str!("fixtures/input_transformed.json"))]
async fn test_valid_input_succeeds(#[case] input: &str, #[case] expected_transformed: &str) {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let mut input_reader = Cursor::new(input);

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_ok(), "{}", result.unwrap_err());

    let transformed_input: Value = serde_json::from_reader(result.unwrap()).unwrap();
    let expected: Value = from_str(expected_transformed).unwrap();

    assert!(
        !to_string(&transformed_input)
            .unwrap()
            .contains(PRODUCT_REFERENCE_FIELD)
    );

    let mut actual_keys = transformed_input.as_object().unwrap().keys().collect_vec();
    actual_keys.sort();
    let mut expected_keys = expected.as_object().unwrap().keys().collect_vec();
    expected_keys.sort();

    assert_eq!(actual_keys, expected_keys);
    for key in expected_keys {
        assert_eq!(transformed_input[key], expected[key], "{:?}", key);
    }

    let schema_validation = validate_against_target_schema(&transformed_input).await;

    assert!(
        schema_validation.is_ok(),
        "{:?}",
        schema_validation.unwrap_err()
    );
}

#[tokio::test]
async fn test_input_with_unknown_product_refs_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let mut input_reader = Cursor::new(include_str!(
        "fixtures/input_with_unknown_product_refs.json"
    ));

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::UnknownProductReference(_)
    ));
}

#[tokio::test]
async fn test_input_with_invalid_json_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let invalid_json = r#"{"name": "trailing comma",}"#;
    let mut input_reader = Cursor::new(invalid_json);

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::InvalidJson
    ));
}

#[tokio::test]
async fn test_input_with_product_category_mismatches_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let mut input: Value = from_str(INPUT_WITH_PRODUCT_REFS).unwrap();
    input.as_object_mut().unwrap()["HotWaterSource"]["hw cylinder"]["HeatSource"] = json!({
        "hw only hp": {
            "type": "HeatPump_HWOnly",
            "heater_position": 0.1,
            "product_reference": "smart_tank",
            "thermostat_position": 0.4
        }
    });
    let mut input_reader = Cursor::new(input.to_string());

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::ProductCategoryMismatches(_)
    ));
}

#[tokio::test]
async fn test_input_that_does_not_conform_to_combined_schema_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let valid_json = r#"{"key": "value"}"#;
    let mut input_reader = Cursor::new(valid_json);

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::InvalidRequest(_)
    ));
}

#[tokio::test]
#[ignore = "todo"]
async fn test_input_with_invalid_combination_errors() {}

#[tokio::test]
#[ignore = "todo"]
async fn test_input_referencing_invalid_pcdb_product_errors() {}
