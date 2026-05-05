use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::in_use_factors::{HotWaterOnlyInUseFactorEntry, InUseFactorsAccess};
use crate::products::{HeatPumpVesselType, Product, TappingProfile, Technology};
use crate::transform::{
    InvalidProductCategoryError, ResolveProductsResult, product_reference_from_json_object,
};
use serde_json::{Value as JsonValue, json};
use smartstring::alias::String;
use std::collections::HashMap;

pub async fn transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
    in_use_factors_access: &impl InUseFactorsAccess,
) -> ResolveProductsResult<()> {
    let heat_sources = match json.pointer_mut("/HotWaterSource/hw cylinder/HeatSource") {
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
        _ => return Ok(()),
    };

    for value in heat_sources.values_mut() {
        if let JsonValue::Object(heat_source) = value {
            if let Some(heat_source_type) = heat_source.get("type").and_then(|v| v.as_str()) {
                if matches!(heat_source_type, "HeatPump_HWOnly")
                    && heat_source.contains_key(PRODUCT_REFERENCE_FIELD)
                {
                    let product_reference = product_reference_from_json_object(heat_source)?;
                    let product = &products[&product_reference];

                    if let Technology::HeatPumpHotWaterOnly {
                        power_max,
                        tank_volume_declared,
                        daily_losses_declared,
                        heat_exchanger_surface_area_declared,
                        test_data,
                        hw_vessel_loss_daily,
                        vessel_type,
                        ..
                    } = &product.technology
                    {
                        heat_source.insert("power_max".into(), power_max.as_f64().into());
                        heat_source.insert(
                            "tank_volume_declared".into(),
                            tank_volume_declared.as_f64().into(),
                        );
                        heat_source.insert(
                            "daily_losses_declared".into(),
                            daily_losses_declared.as_f64().into(),
                        );
                        if let Some(heat_exchanger_surface_area_declared) =
                            heat_exchanger_surface_area_declared
                        {
                            heat_source.insert(
                                "heat_exchanger_surface_area_declared".into(),
                                heat_exchanger_surface_area_declared.as_f64().into(),
                            );
                        }
                        heat_source.insert(
                            "test_data".into(),
                            test_data
                                .iter()
                                .map(|datum| {
                                    let tapping_profile = match datum.tapping_profile {
                                        TappingProfile::L => "L",
                                        TappingProfile::M => "M",
                                    };
                                    (
                                        tapping_profile,
                                        json!({
                                            "cop_dhw": datum.cop_dhw.as_f64(),
                                            "hw_tapping_prof_daily_total": datum.hw_tapping_prof_daily_total.as_f64(),
                                            "energy_input_measured": datum.energy_input_measured.as_f64(),
                                            "power_standby": datum.power_standby.as_f64(),
                                            "hw_vessel_loss_daily": hw_vessel_loss_daily.as_f64(),
                                        })
                                    )
                                })
                                .collect(),
                        );

                        let hot_water_in_use_factors = in_use_factors_access
                            .in_use_factors::<HotWaterOnlyInUseFactorEntry>()
                            .await?;

                        let in_use_factor_mismatch = hot_water_in_use_factors
                            .iter()
                            .find(|entry| {
                                Option::<HeatPumpVesselType>::try_from(*entry)
                                    .ok()
                                    .flatten()
                                    .is_some_and(|entry_vessel_type| {
                                        entry_vessel_type == *vessel_type
                                    })
                            })
                            .ok_or_else(|| ResolvePcdbProductsError::InUseFactorEntryMissingError)?
                            .in_use_factor_mismatch;
                        heat_source.insert(
                            "in_use_factor_mismatch".into(),
                            in_use_factor_mismatch.as_f64().into(),
                        );

                        // now remove product reference
                        heat_source.remove(PRODUCT_REFERENCE_FIELD);
                    } else {
                        return Err(InvalidProductCategoryError::from((
                            product_reference,
                            "hot water only heat pump",
                        ))
                        .into());
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_use_factors::mocks::FixtureBackedInUseFactorsAccess;
    use rstest::*;
    use serde_json::{from_str, json};
    use std::collections::HashMap;

    #[fixture]
    fn in_use_factors_access() -> impl InUseFactorsAccess {
        FixtureBackedInUseFactorsAccess
    }

    fn input(product_reference: &str) -> JsonValue {
        json!({
            "HotWaterSource": {
                "hw cylinder": {
                    "HeatSource": {
                        "hw_only_hp": {
                            "type": "HeatPump_HWOnly",
                            "heater_position": 0.1,
                            "EnergySupply": "mains elec",
                            "product_reference": product_reference,
                        }
                    }
                }
            }
        })
    }

    #[tokio::test]
    #[rstest]
    async fn test_transform_heat_pump_hw_only(in_use_factors_access: impl InUseFactorsAccess) {
        let product_reference = "62";
        let mut input = input(product_reference);
        let expected: JsonValue =
            from_str(include_str!("../../test/hp_hw_only_transformed.json")).unwrap();
        let pcdb_hp_hw_only: Product =
            from_str(include_str!("../../test/hp_hw_only_pcdb.json")).unwrap();

        let result = transform(
            &mut input,
            &HashMap::from([(product_reference.into(), pcdb_hp_hw_only)]),
            &in_use_factors_access,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(
            input,
            expected,
            "actual: {}\nexpected: {}",
            serde_json::to_string_pretty(&input).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap()
        );
    }

    #[tokio::test]
    #[rstest]
    async fn test_transform_heat_pump_hw_only_errors_when_product_type_mismatch(
        in_use_factors_access: impl InUseFactorsAccess,
    ) {
        let product_reference = "hp";
        let mut input = input(product_reference);
        let pcdb_hps: HashMap<String, Product> =
            from_str(include_str!("../../test/test_heat_pump_pcdb.json")).unwrap();

        let result = transform(&mut input, &pcdb_hps, &in_use_factors_access).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("hot water only heat pump")
        );
    }
}
