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

#[cfg(test)]
mod tests {
    use crate::CombinedSchema;
    use assert_ok::assert_ok;
    use json_patch::{PatchOperation, ReplaceOperation, merge, patch};
    use jsonschema::validator;
    use rstest::*;
    use serde_json::{Value, json};
    use std::fs::File;
    use std::io::BufReader;

    #[validator(path = "./tests/fixtures/target_schema.json")]
    pub(crate) struct TargetSchema;

    #[fixture]
    fn base_file() -> Value {
        serde_json::from_str(include_str!(
            "transform/fixtures/base_demo_files/demo_FHS_hp_with_advancedstart.json"
        ))
        .unwrap()
    }

    enum TransformedFileFormat {
        // use the full body of the *_transformed.json file
        Full,
        // the snippets exist as an object/dictionary at the root of the *_transformed.json file
        Dictionary,
    }

    enum Operation {
        Merge,
        Replace,
    }

    #[rstest]
    #[case::air_powered_shower(
        "air_powered_shower_transformed",
        "",
        TransformedFileFormat::Full,
        Operation::Merge,
        None,
        None
    )]
    #[case::boiler(
        "boiler_input_transformed",
        "",
        TransformedFileFormat::Full,
        Operation::Merge,
        None,
        None
    )]
    #[case::combi_boiler(
        "combi_boiler_hot_water_source_transformed",
        "/HotWaterSource/hw cylinder",
        TransformedFileFormat::Dictionary,
        Operation::Replace,
        None,
        None
    )]
    #[case::esh(
        "esh_transformed",
        "/SpaceHeatSystem",
        TransformedFileFormat::Full,
        Operation::Replace,
        Some("esh"),
        None
    )]
    #[case::fancoil(
        "fancoil_transformed",
        "/SpaceHeatSystem/SpaceHeatSystem1/emitters/0",
        TransformedFileFormat::Full,
        Operation::Replace,
        None,
        Some("DESN-H-End-02-HP-cMEV-fan-coil")
    )]
    #[case::heat_battery_input(
        "heat_battery_input_transformed",
        "",
        TransformedFileFormat::Full,
        Operation::Merge,
        None,
        None
    )]
    #[case::heat_network(
        "heat_network_transformed",
        "/HeatSourceWet/hp",
        TransformedFileFormat::Dictionary,
        Operation::Merge,
        None,
        None
    )]
    #[case::heat_pump(
        "heat_pump_input_transformed",
        "/HeatSourceWet",
        TransformedFileFormat::Dictionary,
        Operation::Replace,
        None,
        None
    )]
    #[case::hiu(
        "hiu_transformed",
        "/HeatSourceWet",
        TransformedFileFormat::Full,
        Operation::Replace,
        Some("hiu"),
        None
    )]
    #[case::hp_hw_only(
        "hp_hw_only_transformed",
        "/HotWaterSource/hw cylinder/HeatSource",
        TransformedFileFormat::Full,
        Operation::Merge,
        Some("hp_hw_only"),
        Some("DESN-H-End-02-HP-cMEV-HWOHP-SHWT")
    )]
    #[case::mechanical_ventilation(
        "mechanical_ventilation_input_transformed",
        "/InfiltrationVentilation",
        TransformedFileFormat::Full,
        Operation::Merge,
        None,
        None
    )]
    #[case::radiator(
        "radiator_transformed",
        "/SpaceHeatSystem/radiators/emitters/0",
        TransformedFileFormat::Full,
        Operation::Replace,
        None,
        None
    )]
    #[case::smart_hw_tank(
        "smart_hw_tank_transformed",
        "/HotWaterSource/hw cylinder",
        TransformedFileFormat::Full,
        Operation::Replace,
        None,
        None
    )]
    #[case::wet_distribution(
        "wet_distribution_transformed",
        "/SpaceHeatSystem",
        TransformedFileFormat::Full,
        Operation::Merge,
        Some("wet_distribution"),
        None
    )]
    #[case::wwhrs(
        "wwhrs_transformed",
        "",
        TransformedFileFormat::Dictionary,
        Operation::Merge,
        None,
        None
    )]
    fn check_transformed_fixtures_against_target_schema(
        base_file: Value,
        #[case] fixture: &str,
        #[case] json_pointer: &str,
        #[case] format: TransformedFileFormat,
        #[case] operation: Operation,
        #[case] under_key: Option<&str>,
        #[case] specialised_file: Option<&str>,
    ) {
        let snippet: Value = serde_json::from_reader(BufReader::new(
            File::open(["./src/transform/fixtures/", fixture, ".json"].concat()).unwrap(),
        ))
        .unwrap();

        let mut base_file = specialised_file
            .map(|file| {
                serde_json::from_reader(BufReader::new(
                    File::open(
                        ["./src/transform/fixtures/base_demo_files/", file, ".json"].concat(),
                    )
                    .unwrap(),
                ))
                .unwrap()
            })
            .unwrap_or(base_file);

        let snippets = match format {
            TransformedFileFormat::Full => under_key
                .map(|key| vec![json!({key: snippet})])
                .unwrap_or(vec![snippet]),
            TransformedFileFormat::Dictionary => {
                snippet.as_object().unwrap().values().cloned().collect()
            }
        };

        for snippet in snippets {
            {
                match operation {
                    Operation::Merge => {
                        let pointer = base_file.pointer_mut(json_pointer).unwrap();
                        merge(pointer, &snippet);
                    }
                    Operation::Replace => {
                        let patch_operation = [PatchOperation::Replace(ReplaceOperation {
                            path: json_pointer.parse().unwrap(),
                            value: snippet.clone(),
                        })];
                        patch(&mut base_file, &patch_operation).unwrap();
                    }
                }
            }

            let result = TargetSchema::validate(&base_file);

            if result.is_err() {
                println!(
                    "Validation failed: {}",
                    serde_json::to_string_pretty(&base_file).unwrap()
                );
            }

            assert_ok!(result);
        }
    }

    #[rstest]
    #[case("input_with_product_refs")]
    #[case("input_with_unknown_product_refs")]
    fn test_integration_input_with_product_refs_conforms_to_combined_schema(#[case] file: &str) {
        let input: Value = serde_json::from_reader(BufReader::new(
            File::open(["tests/fixtures/", file, ".json"].concat()).unwrap(),
        ))
        .unwrap();

        assert_ok!(CombinedSchema::validate(&input));
    }

    #[rstest]
    fn test_integration_input_transformed_comforms_to_target_schema() {
        let input: Value =
            serde_json::from_str(include_str!("../tests/fixtures/input_transformed.json")).unwrap();

        assert_ok!(TargetSchema::validate(&input));
    }
}
