use jsonschema::ValidationError;
use resolve_products::PRODUCT_REFERENCE_FIELD;
use rstest::{fixture, rstest};
use serde_json::{Value, from_str, to_string};

mod common;

#[fixture]
fn input() -> &'static [u8] {
    include_bytes!("./example_input_hp_only.json")
}

async fn validate_against_target_schema(input: &Value) -> Result<(), ValidationError<'_>> {
    let schema = from_str(include_str!("./target_schema.json")).unwrap();
    let schema_validator = jsonschema::async_validator_for(&schema).await?;

    schema_validator.validate(input)
}

#[tokio::test]
#[rstest]
async fn test_setup(input: &[u8]) {
    let client = common::setup().await;

    let result = resolve_products::resolve_products(input, client).await;
    assert!(result.is_ok());
}

#[tokio::test]
#[rstest]
#[ignore = "WIP"]
async fn test_valid_input(input: &[u8]) {
    let client = common::setup().await;

    let result = resolve_products::resolve_products(input, client).await;

    assert!(result.is_ok());

    let transformed_input: Value = serde_json::from_slice(input).unwrap();

    assert!(
        !to_string(&transformed_input)
            .unwrap()
            .contains(PRODUCT_REFERENCE_FIELD)
    );

    let schema_validation = validate_against_target_schema(&transformed_input).await;

    assert!(
        schema_validation.is_ok(),
        "{}",
        schema_validation.unwrap_err().to_string()
    );
}
