mod elec_storage_heater;
mod fancoil;
mod radiator;
mod underfloor_heating;

use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::Product;
use crate::transform::{EnergySupplies, ResolveProductsResult, product_reference_from_json_object};
use serde_json::Value as JsonValue;
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
                    "ElecStorageHeater" if system.contains_key(PRODUCT_REFERENCE_FIELD) => {
                        let product_ref = product_reference_from_json_object(system)?;

                        elec_storage_heater::transform(
                            system,
                            &products[&product_ref],
                            &product_ref,
                            energy_supplies,
                        )?
                    }
                    "WetDistribution" => {
                        let emitters = system.get_mut("emitters").and_then(|v| v.as_array_mut());
                        for value in emitters.into_iter().flatten() {
                            if let Some(emitter) = value.as_object_mut() {
                                if emitter.contains_key(PRODUCT_REFERENCE_FIELD) {
                                    let product_ref = product_reference_from_json_object(emitter)?;

                                    if let Some(emitter_type) =
                                        emitter.get("wet_emitter_type").and_then(|v| v.as_str())
                                    {
                                        match emitter_type {
                                            "radiator" => radiator::transform(
                                                emitter,
                                                &products[&product_ref],
                                                &product_ref,
                                            )?,
                                            "ufh" => underfloor_heating::transform(
                                                emitter,
                                                &products[&product_ref],
                                                &product_ref,
                                            )?,
                                            "fancoil" => fancoil::transform(
                                                emitter,
                                                &products[&product_ref],
                                                &product_ref,
                                            )?,
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {} // TODO could add warning about unexpected type being reached
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
    use crate::transform::space_heat_system::transform;
    use rstest::*;
    use serde_json::{from_str, json};
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
            from_str(include_str!("../../../test/esh_transformed.json")).unwrap();
        let expected_wet_distribution: JsonValue = from_str(include_str!(
            "../../../test/wet_distribution_transformed.json"
        ))
        .unwrap();

        let result = transform(&mut input, &SPACE_HEATING_PCDB_PRODUCTS, &energy_supplies);

        let expected_input = json!({
            "SpaceHeatSystem": {
                "ElecHeater1": expected_esh,
                "ElecHeater2": expected_esh,
                "WetDistribution": expected_wet_distribution,
            }
        });

        assert!(result.is_ok());

        assert_eq!(
            input,
            expected_input,
            "actual: {}\nexpected: {}",
            serde_json::to_string_pretty(&input).unwrap(),
            serde_json::to_string_pretty(&expected_input).unwrap()
        );
    }
}
