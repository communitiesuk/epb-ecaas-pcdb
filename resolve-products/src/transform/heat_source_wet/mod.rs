mod boiler;
mod heat_battery_pcm;
mod heat_pump;

use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, ProductCatalogue};
use crate::transform::{EnergySupplies, ResolveProductsResult};
use crate::PRODUCT_REFERENCE_FIELD;
use serde_json::{Map, Value as JsonValue};
use smartstring::alias::String as SmartString;
use std::collections::HashMap;

fn product_reference_from_json_object(
    heat_source_wet: &Map<String, JsonValue>,
) -> Result<SmartString, ResolvePcdbProductsError> {
    Ok(SmartString::from(
        heat_source_wet[PRODUCT_REFERENCE_FIELD]
            .as_str()
            .ok_or_else(|| {
                ResolvePcdbProductsError::InvalidProductCategoryReference(
                    heat_source_wet[PRODUCT_REFERENCE_FIELD].clone(),
                )
            })?,
    ))
}

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
                    "HeatPump" => {
                        if heat_source_object.contains_key(PRODUCT_REFERENCE_FIELD) {
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
                    }
                    "Boiler" => {
                        if heat_source_object.contains_key(PRODUCT_REFERENCE_FIELD) {
                            let product_reference =
                                product_reference_from_json_object(heat_source_object)?;

                            boiler::transform(
                                heat_source_object,
                                &products[&product_reference],
                                &product_reference,
                                energy_supplies,
                            )?
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
    use crate::transform::catalogue::{mock_energy_supplies, FixtureBackedProductCatalogue};
    use rstest::{fixture, rstest};
    use serde_json::json;

    #[fixture]
    fn heat_source_wet_pcdb_products() -> HashMap<SmartString, Product> {
        let hps: HashMap<SmartString, Product> =
            serde_json::from_str(include_str!("../../../test/test_heat_pump_pcdb.json")).unwrap();
        let boilers: HashMap<SmartString, Product> =
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
    async fn test_transform_multiple_heat_pumps(
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

        let pointers = ["/HeatSourceWet/hp", "/HeatSourceWet/boiler"];
        for pointer in pointers {
            assert!(heat_source_wet_input.pointer(pointer).is_some());
        }
    }
}
