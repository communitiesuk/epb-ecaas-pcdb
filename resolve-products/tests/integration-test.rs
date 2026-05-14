use jsonschema::ValidationError;
use resolve_products::PRODUCT_REFERENCE_FIELD;
use rstest::rstest;
use serde_json::{Value, from_str, to_string};
use std::io::Cursor;

mod common;

async fn validate_against_target_schema(input: &Value) -> Result<(), ValidationError<'_>> {
    let schema = from_str(include_str!("./target_schema.json")).unwrap();
    let schema_validator = jsonschema::async_validator_for(&schema).await?;

    schema_validator.validate(input)
}

#[tokio::test]
#[rstest]
#[case(include_str!("./demo_fhs.json"), include_str!("./demo_fhs.json"))]
#[case(include_str!("./input_with_product_refs.json"), include_str!("./input_transformed.json"))]
async fn test_valid_input(#[case] input: &str, #[case] expected_transformed: &str) {
    let client = common::setup().await;
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
    assert_eq!(
        transformed_input,
        expected,
        "actual: {}\nexpected: {}",
        serde_json::to_string_pretty(&transformed_input).unwrap(),
        serde_json::to_string_pretty(&expected).unwrap()
    );

    let schema_validation = validate_against_target_schema(&transformed_input).await;

    assert!(
        schema_validation.is_ok(),
        "{:?}",
        schema_validation.unwrap_err()
    );
}
