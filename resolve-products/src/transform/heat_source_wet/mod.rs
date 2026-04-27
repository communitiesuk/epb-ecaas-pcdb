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
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
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
    use rstest::{fixture, rstest};
    use serde_json::json;

    #[fixture]
    fn heat_source_wet_pcdb_products() -> HashMap<String, Product> {
        let hps: HashMap<String, Product> =
            serde_json::from_str(include_str!("../../../test/test_heat_pump_pcdb.json")).unwrap();
        let boilers: HashMap<String, Product> =
            serde_json::from_str(include_str!("../../../test/test_boilers_pcdb.json")).unwrap();
        hps.into_iter().chain(boilers).collect()
    }

    fn heat_source_wet_input() -> JsonValue {
        json!({
            "HeatSourceWet": {
                "hp": {
                    "type": "HeatPump",
                    "EnergySupply": "mains elec",
                    "product_reference": "hp",
                    "is_heat_network": false
                },
                "boiler": {
                    "type": "Boiler",
                    "EnergySupply": "mains gas",
                    "product_reference": "boiler",
                    "is_heat_network": false
                }
            }
        })
    }

    #[rstest]
    fn test_transform_multiple_heat_pumps(heat_source_wet_pcdb_products: HashMap<String, Product>) {
        let mut heat_source_wet_input = heat_source_wet_input();
        let result = transform(&mut heat_source_wet_input, &heat_source_wet_pcdb_products);
        assert!(result.is_ok());

        let pointers = ["/HeatSourceWet/hp", "/HeatSourceWet/boiler"];
        for pointer in pointers {
            assert!(heat_source_wet_input.pointer(pointer).is_some());
        }
    }
}
