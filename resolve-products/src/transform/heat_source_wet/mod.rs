mod boiler;
mod heat_pump;

use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::Product;
use crate::transform::ResolveProductsResult;
use serde_json::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

pub fn transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
) -> ResolveProductsResult<()> {
    let heat_source_wets = match json.pointer_mut("/HeatSourceWet") {
        Some(node)
            if node.is_object() => {
                node.as_object_mut().unwrap()
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
                    heat_pump::transform(
                        heat_source_wet,
                        &products[product_reference.as_str()],
                        &product_reference,
                    )?;
                }

                if heat_source_wet
                    .get("type")
                    .is_some_and(|v| matches!(v, JsonValue::String(s) if s == "Boiler"))
                {
                    boiler::transform(
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
    use super::*;
    use crate::transform::heat_source_wet::transform;
    use itertools::Itertools;
    use rstest::{fixture, rstest};
    use serde_json::{Value, json};

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

        let result = transform(&mut heat_pump_input, &pcdb_heat_pumps);

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
}
