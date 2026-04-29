mod elec_storage_heater;
mod radiator;

use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::Product;
use crate::transform::{EnergySupplies, ResolveProductsResult};
use serde_json::Value as JsonValue;
use smartstring::SmartString;
use smartstring::alias::String;
use std::collections::HashMap;

pub fn transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
    energy_supplies: &EnergySupplies,
) -> ResolveProductsResult<()> {
    let space_heat_systems = match json.pointer_mut("/SpaceHeatSystem") {
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
        _ => return Ok(()),
    };

    for value in space_heat_systems.values_mut() {
        if let JsonValue::Object(system) = value {
            if let Some(system_type) = system.get("type").and_then(|v| v.as_str()) {
                match system_type {
                    "ElecStorageHeater" => {
                        if system.contains_key(PRODUCT_REFERENCE_FIELD) {
                            let product_ref = SmartString::from(
                                system[PRODUCT_REFERENCE_FIELD].as_str().ok_or_else(|| {
                                    ResolvePcdbProductsError::InvalidProductCategoryReference(
                                        system[PRODUCT_REFERENCE_FIELD].clone(),
                                    )
                                })?,
                            );

                            elec_storage_heater::transform(
                                system,
                                &products[&product_ref],
                                &product_ref,
                                energy_supplies,
                            )?
                        }
                    }
                    "WetDistribution" => {
                        let emitters = system.get_mut("emitters").and_then(|v| v.as_array_mut());
                        for value in emitters.into_iter().flatten() {
                            if let Some(emitter) = value.as_object_mut() {
                                if emitter.contains_key(PRODUCT_REFERENCE_FIELD) {
                                    let product_ref = SmartString::from(
                                        emitter[PRODUCT_REFERENCE_FIELD].as_str().ok_or_else(|| {
                                            ResolvePcdbProductsError::InvalidProductCategoryReference(
                                                emitter[PRODUCT_REFERENCE_FIELD].clone(),
                                            )
                                        })?,
                                    );
                                    if let Some(emitter_type) =
                                        emitter.get("wet_emitter_type").and_then(|v| v.as_str())
                                    {
                                        match emitter_type {
                                            "radiator" => radiator::transform(
                                                emitter,
                                                &products[&product_ref],
                                                &product_ref,
                                            )?,
                                            "ufh" => {}
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
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
    use crate::transform::catalogue::mock_energy_supplies;
    use crate::transform::space_heating::transform;
    use rstest::*;
    use serde_json::from_str;
    use serde_json::json;
    use std::sync::LazyLock;

    #[fixture]
    fn energy_supplies() -> EnergySupplies {
        mock_energy_supplies()
    }

    pub(crate) static SPACE_HEATING_PCDB_PRODUCTS: LazyLock<HashMap<String, Product>> =
        LazyLock::new(|| from_str(include_str!("../../../test/space_heating_pcdb.json")).unwrap());

    #[rstest]
    fn test_transform_space_heating(energy_supplies: EnergySupplies) {
        let mut input = from_str(include_str!("../../../test/space_heating_input.json")).unwrap();
        let expected_esh: JsonValue =
            from_str(include_str!("../../../test/esh_input_transformed.json")).unwrap();
        let expected_radiator: JsonValue = from_str(include_str!(
            "../../../test/test_radiator_input_transformed.json"
        ))
        .unwrap();

        let result = transform(&mut input, &SPACE_HEATING_PCDB_PRODUCTS, &energy_supplies);

        let expected_input = json!({
            "SpaceHeatSystem": {
                "Radiators": {
                    "type": "WetDistribution",
                    "HeatSource": {
                        "name": "boiler",
                        "temp_flow_limit_upper": 65
                    },
                    "Zone": "ThermalZone",
                    "design_flow_temp": 45,
                    "ecodesign_controller": {
                        "ecodesign_control_class": 2,
                        "max_outdoor_temp": 20,
                        "min_flow_temp": 30,
                        "min_outdoor_temp": 0
                    },
                    "emitters": [expected_radiator, expected_radiator],
                    "max_flow_rate": 21,
                    "min_flow_rate": 3.6,
                    "temp_diff_emit_dsgn": 5,
                    "thermal_mass": 0.055946206,
                    "variable_flow": true
                },
                "ElecHeater1": expected_esh,
                "ElecHeater2": expected_esh
            }
        });

        assert!(result.is_ok());

        assert_eq!(input, expected_input);
    }
}
