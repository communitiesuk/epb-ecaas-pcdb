mod transform_boiler;
mod transform_heat_pump;

use std::collections::HashMap;
use serde_json::{Value as JsonValue};
use smartstring::alias::String;
use crate::errors::ResolvePcdbProductsError;
use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::Product;
use crate::transform::transform_json::ResolveProductsResult;

pub fn transform_heat_source_wet(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
) -> ResolveProductsResult<()> {
    let heat_source_wets = match json.pointer_mut("/HeatSourceWet") {
        Some(node) => {
            if node.is_object() {
                node.as_object_mut().unwrap()
            } else {
                return Ok(());
            }
        }
        _ => return Ok(()),
    };
    for value in heat_source_wets.values_mut() {
        if let JsonValue::Object(heat_source_wet) = value {
            let product_reference = if heat_source_wet.contains_key(PRODUCT_REFERENCE_FIELD) {
                std::string::String::from(
                    heat_source_wet[PRODUCT_REFERENCE_FIELD]
                        .as_str()
                        .ok_or_else(|| {
                            ResolvePcdbProductsError::InvalidProductCategoryReference(
                                heat_source_wet[PRODUCT_REFERENCE_FIELD].clone(),
                            )
                        })?,
                )
                    .into()
            } else {
                None
            };

            if let Some(product_reference) = product_reference {
                if heat_source_wet
                    .get("type")
                    .is_some_and(|v| matches!(v, JsonValue::String(s) if s == "HeatPump"))
                {
                    transform_heat_pump::transform_heat_pump(
                        heat_source_wet,
                        &products[product_reference.as_str()],
                        &product_reference,
                    )?;
                }

                if heat_source_wet
                    .get("type")
                    .is_some_and(|v| matches!(v, JsonValue::String(s) if s == "Boiler"))
                {
                    transform_boiler::transform_boiler(
                        heat_source_wet,
                        &products[product_reference.as_str()],
                        &product_reference,
                    )?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use super::*;
    use rstest::{fixture, rstest};
    use serde_json::{json, Value};
    use crate::transform::transform_heat_source_wet::transform_heat_source_wet;

    #[fixture]
    fn pcdb_heat_pumps() -> HashMap<String, Product> {
        serde_json::from_str(include_str!("../../../test/test_heat_pump_pcdb.json")).unwrap()
    }

    fn heat_pump_input(product_reference: &str) -> JsonValue {
        json!({
            "HeatSourceWet": {
            product_reference: {
                "type": "HeatPump",
                "EnergySupply": "mains elec",
                "product_reference": product_reference,
                "is_heat_network": false
            }
        }
        })
    }

    #[fixture]
    fn expected_heat_pumps() -> JsonValue {
        serde_json::from_str(include_str!(
            "../../../test/test_heat_pump_input_transformed.json"
        ))
            .unwrap()
    }

    fn product_from_pointer(input: &Value, pointer: &str) -> HashMap<String, JsonValue> {
        input
            .pointer(pointer)
            .unwrap()
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (String::from(k), v.clone()))
            .collect()
    }

    fn product_keys_sorted(
        actual_product: &HashMap<String, Value>,
        expected_product: &HashMap<String, Value>,
    ) -> (Vec<String>, Vec<String>) {
        let mut actual_keys = actual_product.keys().cloned().collect_vec();
        let mut expected_keys = expected_product.keys().cloned().collect_vec();
        actual_keys.sort();
        expected_keys.sort();

        (actual_keys, expected_keys)
    }

    #[rstest]
    #[case("hp")]
    #[case("hp_without_modulating_control")]
    #[case("hp_with_modulating_control_numeric")]
    #[case("hp_with_backup_ctrl_type_substitute")]
    #[ignore = "todo: implement test case once boiler mapping has been added"]
    #[case("hp_with_boiler")]
    fn test_transform_single_heat_pump(
        pcdb_heat_pumps: HashMap<String, Product>,
        expected_heat_pumps: JsonValue,
        #[case] example_name: &str,
    ) {
        let mut heat_pump_input = heat_pump_input(example_name);
        let result = transform_heat_source_wet(&mut heat_pump_input, &pcdb_heat_pumps);

        assert!(result.is_ok());

        let pointer = format!("/HeatSourceWet/{}", example_name);
        let actual_hp = product_from_pointer(&heat_pump_input, pointer.as_str());
        let expected_hp = product_from_pointer(&expected_heat_pumps, pointer.as_str());

        let (actual_keys_sorted, expected_keys_sorted) =
            product_keys_sorted(&actual_hp, &expected_hp);

        assert_eq!(actual_keys_sorted, expected_keys_sorted);

        for key in expected_hp.keys() {
            assert_eq!(actual_hp[key], expected_hp[key], "{:?}", key);
        }
    }

    #[rstest]
    fn test_transform_multiple_heat_pumps(
        pcdb_heat_pumps: HashMap<String, Product>,
        expected_heat_pumps: JsonValue,
    ) {
        let mut heat_pump_input = heat_pump_input("hp");

        heat_pump_input["HeatSourceWet"]
            .as_object_mut()
            .unwrap()
            .insert(
                "hp_without_modulating_control".into(),
                json!({
                    "type": "HeatPump",
                    "EnergySupply": "mains elec",
                    "product_reference": "hp_without_modulating_control",
                    "is_heat_network": false
                }),
            );

        let result = transform_heat_source_wet(&mut heat_pump_input, &pcdb_heat_pumps);

        assert!(result.is_ok());

        let pointers = [
            "/HeatSourceWet/hp",
            "/HeatSourceWet/hp_without_modulating_control",
        ];

        for pointer in pointers {
            let actual_hp = product_from_pointer(&heat_pump_input, pointer);
            let expected_hp = product_from_pointer(&expected_heat_pumps, pointer);

            let (actual_keys_sorted, expected_keys_sorted) =
                product_keys_sorted(&actual_hp, &expected_hp);

            assert_eq!(actual_keys_sorted, expected_keys_sorted);

            for key in expected_hp.keys() {
                assert_eq!(actual_hp[key], expected_hp[key], "{:?}", key);
            }
        }
    }

    #[fixture]
    fn pcdb_boilers() -> HashMap<String, Product> {
        serde_json::from_str(include_str!("../../../test/test_boilers_pcdb.json")).unwrap()
    }

    fn boiler_input(product_reference: &str, specified_location: Option<&str>) -> JsonValue {
        let mut input = json!({
            "HeatSourceWet": {
            product_reference: {
                "type": "Boiler",
                "EnergySupply": "mains gas",
                "product_reference": product_reference,
                "is_heat_network": false
            }
        }
        });
        if let Some(location) = specified_location {
            input["HeatSourceWet"][product_reference]["specified_location"] = json!(location);
        }
        input
    }

    #[fixture]
    fn expected_boilers() -> JsonValue {
        serde_json::from_str(include_str!("../../../test/test_boiler_input_transformed.json")).unwrap()
    }

    #[rstest]
    #[case("boiler", None)]
    #[case("boiler_unknown_location", Some("internal"))]
    fn test_transform_boilers(
        pcdb_boilers: HashMap<String, Product>,
        expected_boilers: JsonValue,
        #[case] product_reference: &str,
        #[case] specified_location: Option<&str>,
    ) {
        let mut boiler_input = boiler_input(product_reference, specified_location);
        let result = transform_heat_source_wet(&mut boiler_input, &pcdb_boilers);

        assert!(result.is_ok());

        let pointer = format!("/HeatSourceWet/{}", product_reference);
        let actual_boiler = product_from_pointer(&boiler_input, pointer.as_str());
        let expected_boiler = product_from_pointer(&expected_boilers, pointer.as_str());

        let (actual_keys_sorted, expected_keys_sorted) =
            product_keys_sorted(&actual_boiler, &expected_boiler);

        assert_eq!(actual_keys_sorted, expected_keys_sorted);

        for key in expected_boiler.keys() {
            assert_eq!(actual_boiler[key], expected_boiler[key], "{:?}", key);
        }
    }

    #[rstest]
    fn test_transform_boiler_without_locations(pcdb_boilers: HashMap<String, Product>) {
        let product_reference = "boiler_unknown_location";
        let mut boiler_input = boiler_input(product_reference, None);

        let result = transform_heat_source_wet(&mut boiler_input, &pcdb_boilers);
        assert!(result.is_err());
    }
}