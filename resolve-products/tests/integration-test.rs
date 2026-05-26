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
async fn test_unknown_product_ref_errors() {
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
async fn test_invalid_json_errors() {
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
async fn test_product_category_mismatch_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let mut input: Value = from_str(INPUT_WITH_PRODUCT_REFS).unwrap();
    input["HotWaterSource"]["hw cylinder"]["HeatSource"] = json!({
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
async fn test_combined_schema_violation_errors() {
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
async fn test_invalid_combination_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    // smart hot water tank product must have a heat_exchanger_surface_area when its heat source is a heat pump hot water only
    let mut input: Value = from_str(INPUT_WITH_PRODUCT_REFS).unwrap();
    input["HotWaterSource"]["hw cylinder"]["product_reference"] =
        json!("smart_tank_no_heat_exchanger_area");
    let mut input_reader = Cursor::new(input.to_string());

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::InvalidCombination(_)
    ));
}

#[tokio::test]
async fn test_pcdb_product_missing_field_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let mut input: Value = from_str(INPUT_WITH_PRODUCT_REFS).unwrap();
    // reference a PCDB HIU that's invalid due to missing technology_type field
    input["HeatSourceWet"]["HIU"]["product_reference"] = json!("hiu_missing_technology_type");
    let mut input_reader = Cursor::new(input.to_string());

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::DeserializeError(_)
    ));
}

#[tokio::test]
async fn test_invalid_pcdb_product_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let mut input: Value = from_str(INPUT_WITH_PRODUCT_REFS).unwrap();
    // reference a PCDB heat battery that's invalid due to missing test data
    input["HeatSourceWet"]["Heat battery dry core"]["product_reference"] =
        json!("hb_dry_core_empty_test_data");
    let mut input_reader = Cursor::new(input.to_string());

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::InvalidProduct(_, _)
    ));
}

#[tokio::test]
async fn test_fuel_type_no_energy_supply_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let mut input: Value = from_str(INPUT_WITH_PRODUCT_REFS).unwrap();
    // reference a PCDB elec storage heater with fuel type mains gas (input has electricity energy supply only)
    input["SpaceHeatSystem"]["Elec Heater"]["product_reference"] = json!("esh_with_gas");
    let mut input_reader = Cursor::new(input.to_string());

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::NoEnergySupplyProvidedForFuelType(_)
    ));
}

#[tokio::test]
async fn test_unknown_sub_heat_network_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let mut input: Value = from_str(INPUT_WITH_PRODUCT_REFS).unwrap();
    input["HeatSourceWet"]["HIU"]["sub_heat_network_name"] = json!("nonsense");
    let mut input_reader = Cursor::new(input.to_string());

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::SubHeatNetworkNotFoundError(_, _)
    ));
}

#[tokio::test]
async fn test_missing_heat_pump_for_heat_network_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let mut input: Value = from_str(INPUT_WITH_PRODUCT_REFS).unwrap();
    // remove heat pump from input and reference PCDB heat network with booster_heat_pump: true
    let heat_source_wet = input["HeatSourceWet"].as_object_mut().unwrap();
    heat_source_wet["HIU"]["heat_network_reference"] = json!("heat_network_requiring_hp");
    heat_source_wet.remove("Heat pump");
    let mut input_reader = Cursor::new(input.to_string());

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::BoosterHeatPumpNotPresentError
    ));
}

#[tokio::test]
async fn test_vessel_type_missing_from_factors_errors() {
    let environment = common::setup().await;
    let client = environment.dynamo_client();

    let mut input: Value = from_str(INPUT_WITH_PRODUCT_REFS).unwrap();
    input["HotWaterSource"]["hw cylinder"]["HeatSource"]["hw only hp"]["product_reference"] =
        json!("hp_hw_only_unknown_vessel_type");
    let mut input_reader = Cursor::new(input.to_string());

    let result = resolve_products::resolve_products(&mut input_reader, client).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ResolvePcdbProductsError::InUseFactorEntryMissingError
    ));
}
