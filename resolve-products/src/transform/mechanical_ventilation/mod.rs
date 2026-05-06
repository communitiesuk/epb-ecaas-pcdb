pub mod centralised_mev;
pub mod centralised_mvhr;
pub mod decentralised_mev;

use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::Product;
use crate::transform::{ResolveProductsResult, product_reference_from_json_object};
#[cfg(test)]
use serde_json::Map;
use serde_json::Value as JsonValue;
use smartstring::alias::String as SmartString;
use std::collections::HashMap;

#[allow(dead_code)]
pub fn transform(
    json: &mut JsonValue,
    products: &HashMap<SmartString, Product>,
) -> ResolveProductsResult<()> {
    let number_of_wetrooms_including_kitchen = match json.pointer_mut("/NumberOfWetRooms") {
        Some(node) if node.is_u64() => node.as_u64().unwrap(),
        _ => {
            return Err(
                ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
                    "NumberOfWetRooms was expected to be set as a positive integer",
                ),
            );
        }
    };
    let number_of_wetrooms = number_of_wetrooms_including_kitchen - 1;

    let mechanical_ventilation = match json.pointer_mut("/MechanicalVentilation") {
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
        _ => return Ok(()),
    };

    for mech_vent in mechanical_ventilation.values_mut() {
        if let JsonValue::Object(mech_vent_object) = mech_vent {
            if let Some(vent_type) = mech_vent_object.get("vent_type").and_then(|v| v.as_str()) {
                match vent_type {
                    "Decentralised continuous MEV"
                        if mech_vent_object.contains_key(PRODUCT_REFERENCE_FIELD) =>
                    {
                        let product_reference =
                            product_reference_from_json_object(mech_vent_object)?;

                        decentralised_mev::transform(
                            mech_vent_object,
                            &products[&product_reference],
                            &product_reference,
                        )?
                    }
                    "Centralised continuous MEV"
                        if mech_vent_object.contains_key(PRODUCT_REFERENCE_FIELD) =>
                    {
                        let product_reference =
                            product_reference_from_json_object(mech_vent_object)?;

                        centralised_mev::transform(
                            mech_vent_object,
                            &products[&product_reference],
                            &product_reference,
                            number_of_wetrooms as usize,
                        )?
                    }
                    "MVHR" if mech_vent_object.contains_key(PRODUCT_REFERENCE_FIELD) => {
                        let product_reference =
                            product_reference_from_json_object(mech_vent_object)?;

                        centralised_mvhr::transform(
                            mech_vent_object,
                            &products[&product_reference],
                            &product_reference,
                            number_of_wetrooms as usize,
                        )?
                    }
                    _ => {}
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
    fn mechanical_ventilation_pcdb_products() -> HashMap<SmartString, Product> {
        serde_json::from_str(include_str!(
            "../../../test/test_mechanical_ventilation_pcdb.json"
        ))
        .unwrap()
    }

    fn mechanical_ventilation_input() -> JsonValue {
        json!({
            "NumberOfWetRooms": 2,
            "MechanicalVentilation": {
                "decentralisedMev": {
                    "vent_type": "Decentralised continuous MEV",
                    "EnergySupply": "mains elec",
                    "product_reference": "decentralisedMev",
                    "design_outdoor_air_flow_rate": 80,
                    "installed_under_approved_scheme": true,
                    "installation_type": "in_ceiling",
                    "installation_location": "kitchen",
                    "mid_height_air_flow_path": 2,
                    "orientation360": 0,
                    "pitch": 90
                },
                "centralisedMev": {
                    "vent_type": "Centralised continuous MEV",
                    "EnergySupply": "mains elec",
                    "product_reference": "centralisedMev",
                    "design_outdoor_air_flow_rate": 80,
                    "installed_under_approved_scheme": true,
                    "measured_fan_power": 12.26,
                    "measured_air_flow_rate": 37,
                    "mid_height_air_flow_path": 1.5,
                    "orientation360": 90,
                    "pitch": 60
                },
                "centralisedMvhr": {
                    "vent_type": "MVHR",
                    "EnergySupply": "mains elec",
                    "product_reference": "centralisedMvhr",
                    "design_outdoor_air_flow_rate": 80,
                    "installed_under_approved_scheme": true,
                    "mvhr_location": "inside",
                    "ductwork": [],
                    "position_intake": {
                        "mid_height_air_flow_path": 1.5,
                        "orientation360": 90,
                        "pitch": 60
                    },
                    "position_exhaust": {
                        "mid_height_air_flow_path": 1.6,
                        "orientation360": 90,
                        "pitch": 60
                    }
                }
            }
        })
    }

    #[rstest]
    fn test_transform_mechanical_ventilation_products(
        mechanical_ventilation_pcdb_products: HashMap<SmartString, Product>,
    ) {
        let mut mechanical_ventilation_input = mechanical_ventilation_input();
        let result = transform(
            &mut mechanical_ventilation_input,
            &mechanical_ventilation_pcdb_products,
        );
        assert!(result.is_ok());

        let pointers = [
            "/MechanicalVentilation/decentralisedMev",
            "/MechanicalVentilation/centralisedMev",
            "/MechanicalVentilation/centralisedMvhr",
        ];

        for pointer in pointers {
            assert!(mechanical_ventilation_input.pointer(pointer).is_some());
            assert!(
                mechanical_ventilation_input
                    .pointer(&format!("{pointer}/product_reference"))
                    .is_none(),
                "mechanical_ventilation_input still has a product_reference at pointer {pointer}"
            );
        }
    }
}

#[cfg(test)]
fn mechanical_ventilation_pcdb_products() -> HashMap<String, Product> {
    serde_json::from_str(include_str!(
        "../../../test/test_mechanical_ventilation_pcdb.json"
    ))
    .unwrap()
}

#[cfg(test)]
fn expected_transformed_mech_vent_input(product_reference: &str) -> Map<String, JsonValue> {
    let expected_mechanical_ventilation: JsonValue = serde_json::from_str(include_str!(
        "../../../test/test_mechanical_ventilation_input_transformed.json"
    ))
    .unwrap();

    expected_mechanical_ventilation
        .pointer(&format!("/MechanicalVentilation/{}", product_reference))
        .unwrap()
        .as_object()
        .unwrap()
        .clone()
}
