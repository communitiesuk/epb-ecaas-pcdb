mod boiler;
mod heat_battery_dry_core;
mod heat_battery_pcm;
pub mod heat_network;
mod heat_pump;
mod hiu;

use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, ProductCatalogue};
use crate::transform::{EnergySupplies, ResolveProductsResult, product_reference_from_json_object};
use serde_json::Value as JsonValue;
use smartstring::alias::String as SmartString;
use std::collections::HashMap;

pub async fn transform(
    json: &mut JsonValue,
    products: &HashMap<SmartString, Product>,
    catalogue: &impl ProductCatalogue,
    energy_supplies: &EnergySupplies,
) -> ResolveProductsResult<()> {
    let heat_source_wet = match json.pointer_mut("/HeatSourceWet") {
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
        _ => return Ok(()),
    };

    for heat_source in heat_source_wet.values_mut() {
        if let JsonValue::Object(heat_source_object) = heat_source {
            if let Some(heat_source_type) = heat_source_object.get("type").and_then(|v| v.as_str())
            {
                match heat_source_type {
                    "HeatPump" if heat_source_object.contains_key(PRODUCT_REFERENCE_FIELD) => {
                        let product_reference =
                            product_reference_from_json_object(heat_source_object)?;

                        heat_pump::transform(
                            heat_source_object,
                            &products[&product_reference],
                            &product_reference,
                            catalogue,
                            energy_supplies,
                        )
                        .await?
                    }
                    "Boiler" if heat_source_object.contains_key(PRODUCT_REFERENCE_FIELD) => {
                        let product_reference =
                            product_reference_from_json_object(heat_source_object)?;

                        boiler::transform(
                            heat_source_object,
                            &products[&product_reference],
                            &product_reference,
                            energy_supplies,
                        )?
                    }
                    "HeatBattery" if heat_source_object.contains_key(PRODUCT_REFERENCE_FIELD) => {
                        let product_reference =
                            product_reference_from_json_object(heat_source_object)?;

                        let battery_type = heat_source_object
                            .get("battery_type")
                            .and_then(|battery_type| battery_type.as_str())
                            .ok_or_else(|| {
                                ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck("A HeatBattery heat source wet was expected to have a battery_type.")
                            })?;

                        match battery_type {
                            "pcm" => {
                                heat_battery_pcm::transform(
                                    heat_source_object,
                                    &products[&product_reference],
                                    &product_reference,
                                    energy_supplies,
                                )?;
                            }
                            "dry_core" => heat_battery_dry_core::transform(
                                heat_source_object,
                                &products[&product_reference],
                                &product_reference,
                                energy_supplies,
                            )?,
                            _ => return Err(
                                ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
                                    "A HeatBattery heat source wet was expected to have a valid battery_type (pcm or dry_core).",
                                ),
                            ),
                        }
                    }
                    "HIU" if heat_source_object.contains_key(PRODUCT_REFERENCE_FIELD) => {
                        let product_reference =
                            product_reference_from_json_object(heat_source_object)?;

                        hiu::transform(
                            heat_source_object,
                            &products[&product_reference],
                            &product_reference,
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
    use crate::transform::catalogue::{FixtureBackedProductCatalogue, mock_energy_supplies};
    use rstest::{fixture, rstest};
    use serde_json::{from_str, json};

    #[fixture]
    fn heat_source_wet_pcdb_products() -> HashMap<SmartString, Product> {
        let hps: HashMap<SmartString, Product> =
            from_str(include_str!("../../../test/test_heat_pump_pcdb.json")).unwrap();
        let boilers: HashMap<SmartString, Product> =
            from_str(include_str!("../../../test/test_boilers_pcdb.json")).unwrap();
        let pcm_heat_batteries: HashMap<SmartString, Product> =
            from_str(include_str!("../../../test/test_heat_batteries_pcdb.json")).unwrap();
        let hiu: HashMap<SmartString, Product> =
            from_str(include_str!("../../../test/hiu_pcdb.json")).unwrap();
        hps.into_iter()
            .chain(boilers)
            .chain(pcm_heat_batteries)
            .chain(hiu)
            .collect()
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
                },
                "pcm": {
                    "type": "HeatBattery",
                    "battery_type": "pcm",
                    "product_reference": "pcm",
                    "number_of_units": 2,
                    "is_heat_network": false
                },
                "dry_core": {
                    "type": "HeatBattery",
                    "battery_type": "dry_core",
                    "product_reference": "dry_core",
                    "number_of_units": 2,
                    "is_heat_network": false
                },
                "hiu": {
                    "type": "HIU",
                    "EnergySupply": "mains elec",
                    "product_reference": "hiu",
                    "building_level_distribution_losses": 1,
                    "is_heat_network": false,
                }
            }
        })
    }

    #[fixture]
    fn dummy_catalogue() -> impl ProductCatalogue {
        FixtureBackedProductCatalogue::new()
    }

    #[fixture]
    fn energy_supplies() -> EnergySupplies {
        mock_energy_supplies()
    }

    #[tokio::test]
    #[rstest]
    async fn test_transform_multiple_heat_source_wet_products(
        heat_source_wet_pcdb_products: HashMap<SmartString, Product>,
        dummy_catalogue: impl ProductCatalogue,
        energy_supplies: EnergySupplies,
    ) {
        let mut heat_source_wet_input = heat_source_wet_input();
        let result = transform(
            &mut heat_source_wet_input,
            &heat_source_wet_pcdb_products,
            &dummy_catalogue,
            &energy_supplies,
        )
        .await;
        assert!(result.is_ok());

        let pointers = [
            "/HeatSourceWet/hp",
            "/HeatSourceWet/boiler",
            "/HeatSourceWet/pcm",
            "/HeatSourceWet/dry_core",
            "/HeatSourceWet/hiu",
        ];
        for pointer in pointers {
            assert!(heat_source_wet_input.pointer(pointer).is_some());
            assert!(
                heat_source_wet_input
                    .pointer(&format!("{pointer}/product_reference"))
                    .is_none(),
                "heat_source_wet_input still has a product_reference at pointer {pointer}"
            );
        }
    }

    fn incorrect_heat_pump_input() -> JsonValue {
        // test this for products that have the same structure
        json!({
            "HeatSourceWet": {
                "hp": {
                    "type": "Boiler", // product_reference is for HeatPump
                    "EnergySupply": "mains elec",
                    "product_reference": "hp",
                    "is_heat_network": false
                }
            }
        })
    }

    fn incorrect_boiler_input() -> JsonValue {
        // test this for products that have the same structure
        json!({
            "HeatSourceWet": {
                "boiler": {
                    "type": "HeatPump", // product_reference is for Boiler
                    "EnergySupply": "mains gas",
                    "product_reference": "boiler",
                    "is_heat_network": false
                }
            }
        })
    }

    #[tokio::test]
    #[rstest]
    #[case(incorrect_boiler_input())]
    #[case(incorrect_heat_pump_input())]
    async fn test_tranform_errors_given_product_type_mismatch(
        heat_source_wet_pcdb_products: HashMap<SmartString, Product>,
        dummy_catalogue: impl ProductCatalogue,
        energy_supplies: EnergySupplies,
        #[case] mut input: JsonValue,
    ) {
        let hp_result = transform(
            &mut input,
            &heat_source_wet_pcdb_products,
            &dummy_catalogue,
            &energy_supplies,
        )
        .await;

        assert!(hp_result.is_err());
        let error = hp_result.unwrap_err().to_string();
        assert!(error.contains("There were mismatch errors where provided product references related to incompatible product categories"));
    }
}
