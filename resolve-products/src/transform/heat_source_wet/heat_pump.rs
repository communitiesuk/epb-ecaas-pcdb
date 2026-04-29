use crate::errors::ResolvePcdbProductsError;
use crate::products::{
    find_product_for_reference, HeatPumpBackupControlType, HeatPumpTestDatum, HeatPumpTestLetter, Product,
    ProductCatalogue, Technology,
};
use crate::transform::{EnergySupplies, ResolveProductsResult};
use crate::PRODUCT_REFERENCE_FIELD;
use itertools::Itertools;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{json, Map, Value as JsonValue};

pub async fn transform(
    heat_pump: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
    catalogue: &impl ProductCatalogue,
    energy_supplies: &EnergySupplies,
) -> ResolveProductsResult<()> {
    let mut category_mismatches = vec![];

    if let Technology::HeatPump {
        source_type,
        sink_type,
        backup_control_type,
        min_temp_diff_flow_return_for_hp_to_operate,
        modulating_control,
        minimum_modulation_rate_35,
        minimum_modulation_rate_55,
        time_constant_on_off_operation,
        temp_lower_operating_limit,
        temp_return_feed_max,
        power_heating_circ_pump,
        power_heating_warm_air_fan,
        power_maximum_backup,
        power_source_circ_pump,
        power_crankcase_heater,
        power_off,
        power_standby,
        ref test_data,
        variable_temp_control,
        ref boiler_product_id,
        ..
    } = product.technology
    {
        heat_pump.insert(
            "backup_ctrl_type".into(),
            backup_control_type.to_string().into(),
        );
        heat_pump.insert(
            "min_temp_diff_flow_return_for_hp_to_operate".into(),
            min_temp_diff_flow_return_for_hp_to_operate.into(),
        );
        if modulating_control {
            // write in the rate for the different temperatures for now
            if let Some(minimum_modulation_rate_35) = minimum_modulation_rate_35 {
                heat_pump.insert(
                    "min_modulation_rate_35".into(),
                    minimum_modulation_rate_35.to_f64().into(),
                );
            }
            if let Some(minimum_modulation_rate_55) = minimum_modulation_rate_55 {
                heat_pump.insert(
                    "min_modulation_rate_55".into(),
                    minimum_modulation_rate_55.to_f64().into(),
                );
            }
        }
        heat_pump.insert("modulating_control".into(), modulating_control.into());
        heat_pump.insert(
            "power_crankcase_heater".into(),
            power_crankcase_heater.to_f64().into(),
        );
        if let Some(power_heating_circ_pump) = power_heating_circ_pump {
            heat_pump.insert(
                "power_heating_circ_pump".into(),
                power_heating_circ_pump.to_f64().into(),
            );
        }
        if let Some(power_heating_warm_air_fan) = power_heating_warm_air_fan {
            heat_pump.insert(
                "power_heating_warm_air_fan".into(),
                power_heating_warm_air_fan.to_f64().into(),
            );
        }
        if !matches!(backup_control_type, HeatPumpBackupControlType::None) {
            if power_maximum_backup.is_none() && boiler_product_id.is_none() {
                return Err(ResolvePcdbProductsError::InvalidProduct(
                    product_reference.to_string(),
                    "either power_max_backup or boilerProductID must be provided when backup_control_type is not None in a heat pump",
                ));
            }
            if let Some(power_maximum_backup) = power_maximum_backup {
                heat_pump.insert(
                    "power_max_backup".into(),
                    power_maximum_backup.to_f64().into(),
                );
            }
            if let Some(boiler_product_id) = boiler_product_id {
                let boiler_product =
                    find_product_for_reference(&boiler_product_id, catalogue).await?;
                if let Technology::Boiler {
                    rated_power,
                    efficiency_full_load,
                    efficiency_part_load,
                    boiler_location,
                    modulation_load,
                    electricity_circ_pump,
                    electricity_part_load,
                    electricity_full_load,
                    electricity_standby,
                    fuel,
                    fuel_aux,
                    ..
                } = boiler_product.technology
                {
                    let boiler = heat_pump
                        .entry("boiler")
                        .or_insert_with(Default::default)
                        .as_object_mut()
                        .ok_or_else(|| {
                            ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
                                "Boiler JSON node within a heat pump was expected to be an object",
                            )
                        })?;
                    boiler.insert("rated_power".into(), rated_power.as_f64().into());
                    boiler.insert(
                        "efficiency_full_load".into(),
                        efficiency_full_load.as_f64().into(),
                    );
                    boiler.insert(
                        "efficiency_part_load".into(),
                        efficiency_part_load.as_f64().into(),
                    );
                    boiler.insert("boiler_location".into(), json!(boiler_location));
                    boiler.insert("modulation_load".into(), modulation_load.as_f64().into());
                    boiler.insert(
                        "electricity_circ_pump".into(),
                        electricity_circ_pump.as_f64().into(),
                    );
                    boiler.insert(
                        "electricity_part_load".into(),
                        electricity_part_load.as_f64().into(),
                    );
                    boiler.insert(
                        "electricity_full_load".into(),
                        electricity_full_load.as_f64().into(),
                    );
                    boiler.insert(
                        "electricity_standby".into(),
                        electricity_standby.as_f64().into(),
                    );

                    let energy_supply = energy_supplies
                        .get(&fuel)
                        .ok_or_else(|| ResolvePcdbProductsError::from(&fuel))?;
                    let energy_supply_aux = energy_supplies
                        .get(&fuel_aux)
                        .ok_or_else(|| ResolvePcdbProductsError::from(&fuel_aux))?;
                    boiler.insert("EnergySupply".into(), json!(energy_supply.as_ref()));
                    boiler.insert("EnergySupply_aux".into(), json!(energy_supply_aux.as_ref()));
                }
            }
        }

        heat_pump.insert("power_off".into(), power_off.to_f64().into());
        heat_pump.insert(
            "power_source_circ_pump".into(),
            power_source_circ_pump.to_f64().into(),
        );
        heat_pump.insert("power_standby".into(), power_standby.to_f64().into());
        heat_pump.insert("sink_type".into(), sink_type.to_string().into());
        heat_pump.insert("source_type".into(), source_type.to_string().into());
        heat_pump.insert(
            "temp_lower_operating_limit".into(),
            temp_lower_operating_limit.to_f64().into(),
        );
        heat_pump.insert(
            "temp_return_feed_max".into(),
            temp_return_feed_max.to_f64().into(),
        );
        heat_pump.insert(
            "test_data_EN14825".into(),
            JsonValue::from(
                test_data
                    .iter()
                    .filter_map(|datum| {
                        let HeatPumpTestDatum {
                            capacity,
                            coefficient_of_performance,
                            design_flow_temperature,
                            temperature_outlet,
                            temperature_source,
                            temperature_test,
                            test_letter,
                            ..
                        } = datum;
                        // 'E' is not accepted in HEM, so filter this out
                        (*test_letter != HeatPumpTestLetter::E).then_some(json!({
                            "capacity": capacity.to_f64(),
                            "cop": coefficient_of_performance.to_f64(),
                            "design_flow_temp": design_flow_temperature.to_f64(),
                            "temp_outlet": temperature_outlet.to_f64(),
                            "temp_source": temperature_source.to_f64(),
                            "temp_test": temperature_test.to_f64(),
                            "test_letter": test_letter,
                        }))
                    })
                    .collect_vec(),
            ),
        );
        heat_pump.insert(
            "time_constant_onoff_operation".into(),
            time_constant_on_off_operation.into(),
        );
        heat_pump.insert(
            "var_flow_temp_ctrl_during_test".into(),
            variable_temp_control.into(),
        );

        // now remove product reference
        heat_pump.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        category_mismatches.push(format!(
            "Product reference '{product_reference}' does not relate to an air source heat pump."
        ));
    }

    if !category_mismatches.is_empty() {
        return Err(ResolvePcdbProductsError::ProductCategoryMismatches(
            category_mismatches,
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::catalogue::{
        mock_energy_supplies, transformed_input_matches_expected, FixtureBackedProductCatalogue,
    };
    use rstest::{fixture, rstest};
    use serde_json::{json, Value};
    use std::collections::HashMap;

    fn heat_pump_input(product_reference: &str) -> Value {
        json!({
            "type": "HeatPump",
            "EnergySupply": "mains elec",
            "product_reference": product_reference,
            "is_heat_network": false
        })
    }

    #[fixture]
    fn pcdb_heat_pumps() -> HashMap<String, Product> {
        serde_json::from_str(include_str!("../../../test/test_heat_pump_pcdb.json")).unwrap()
    }

    #[fixture]
    fn additional_fields() -> HashMap<String, Value> {
        serde_json::from_str(include_str!(
            "../../../test/test_heat_pump_additional_fields.json"
        ))
        .unwrap()
    }

    #[fixture]
    fn catalogue() -> impl ProductCatalogue {
        FixtureBackedProductCatalogue::new()
    }

    fn expected_heat_pump_input(product_reference: &str) -> Map<String, JsonValue> {
        let expected_heat_pumps: JsonValue = serde_json::from_str(include_str!(
            "../../../test/test_heat_pump_input_transformed.json"
        ))
        .unwrap();

        expected_heat_pumps
            .pointer(&format!("/HeatSourceWet/{}", product_reference))
            .unwrap()
            .as_object()
            .unwrap()
            .clone()
    }

    #[tokio::test]
    #[rstest]
    #[case("hp")]
    #[case::hp_without_modulating_control("hp_without_modulating_control")]
    #[case::hp_with_modulating_control_numeric("hp_with_modulating_control_numeric")]
    #[case::hp_with_backup_ctrl_type_substitute("hp_with_backup_ctrl_type_substitute")]
    // #[ignore = "todo: implement test case once boiler mapping has been added"]
    #[case::hp_with_boiler("hp_with_boiler")]
    async fn test_transform_heat_pump(
        pcdb_heat_pumps: HashMap<String, Product>,
        #[case] product_reference: &str,
        catalogue: impl ProductCatalogue,
    ) {
        let mut input = heat_pump_input(product_reference);
        let pcdb_data = pcdb_heat_pumps.get(product_reference).unwrap();

        if let Some(additional_fields) = additional_fields().get(product_reference) {
            input
                .as_object_mut()
                .unwrap()
                .extend(additional_fields.as_object().unwrap().to_owned());
        }

        let result = transform(
            &mut input.as_object_mut().unwrap(),
            pcdb_data,
            product_reference,
            &catalogue,
            &mock_energy_supplies(),
        )
        .await;
        assert!(result.is_ok(), "result: {result:?}");

        let expected_input = expected_heat_pump_input(product_reference);
        transformed_input_matches_expected(&mut input, expected_input);
    }
}
