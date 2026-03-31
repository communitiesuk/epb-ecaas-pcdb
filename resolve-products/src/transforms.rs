use crate::errors::ResolvePcdbProductsError;
use crate::products::{
    BoilerLocation, HeatPumpBackupControlType, HeatPumpTestDatum, HeatPumpTestLetter, Product,
    Technology, find_products_for_references,
};
use crate::{PRODUCT_REFERENCE_FIELD, extract_product_references};
use aws_sdk_dynamodb::client::Client as DynamoDbClient;
use itertools::Itertools;
use rust_decimal::prelude::ToPrimitive;
use serde_json::json;
use serde_json::map::Map;
use serde_json::value::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

pub async fn transform_json(
    json: &mut JsonValue,
    dynamo_client: &DynamoDbClient,
) -> ResolveProductsResult<()> {
    let product_references = extract_product_references(json)?;
    let products = find_products_for_references(&product_references, dynamo_client).await?;

    transform_heat_source_wets(json, &products)
}

fn transform_heat_source_wets(
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
                    transform_heat_pump(
                        heat_source_wet,
                        &products[product_reference.as_str()],
                        &product_reference,
                    )?;
                }

                if heat_source_wet
                    .get("type")
                    .is_some_and(|v| matches!(v, JsonValue::String(s) if s == "Boiler"))
                {
                    transform_boiler(
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

fn transform_heat_pump(
    heat_pump: &mut Map<std::string::String, JsonValue>,
    product: &Product,
    product_reference: &str,
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
            heat_pump.insert(
                "power_max_backup".into(),
                power_maximum_backup.map(|x| x.to_f64()).into(),
            );
            // TODO: add logic for inserting a boiler field
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

fn transform_boiler(
    boiler: &mut Map<std::string::String, JsonValue>,
    product: &Product,
    product_reference: &str,
) -> ResolveProductsResult<()> {
    let mut category_mismatches = vec![];

    if let Technology::Boiler {
        fuel,
        fuel_aux,
        rated_power,
        efficiency_full_load,
        efficiency_part_load,
        boiler_location,
        modulation_load,
        electricity_circ_pump,
        electricity_part_load,
        electricity_full_load,
        electricity_standby,
        ..
    } = &product.technology
    {
        boiler.insert("EnergySupply".into(), fuel.to_string().into());
        boiler.insert("EnergySupply_aux".into(), fuel_aux.to_string().into());
        boiler.insert("rated_power".into(), rated_power.to_f64().into());
        boiler.insert(
            "efficiency_full_load".into(),
            efficiency_full_load.to_f64().into(),
        );
        boiler.insert(
            "efficiency_part_load".into(),
            efficiency_part_load.to_f64().into(),
        );
        boiler.insert("modulation_load".into(), modulation_load.to_f64().into());
        boiler.insert(
            "electricity_circ_pump".into(),
            electricity_circ_pump.to_f64().into(),
        );
        boiler.insert(
            "electricity_part_load".into(),
            electricity_part_load.to_f64().into(),
        );
        boiler.insert(
            "electricity_full_load".into(),
            electricity_full_load.to_f64().into(),
        );
        boiler.insert(
            "electricity_standby".into(),
            electricity_standby.to_f64().into(),
        );

        match boiler_location {
            BoilerLocation::Unknown => {
                boiler.insert(
                    "boiler_location".into(),
                    boiler.get("specified_location").map(|x| x.as_str()).into(),
                );
            }
            _ => {
                boiler.insert("boiler_location".into(), boiler_location.to_string().into());
            }
        }

        boiler.remove("specified_location");
        boiler.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        category_mismatches.push(format!(
            "Product reference '{product_reference}' does not relate to a boiler."
        ));
    }

    if !category_mismatches.is_empty() {
        return Err(ResolvePcdbProductsError::ProductCategoryMismatches(
            category_mismatches,
        ));
    }

    Ok(())
}

pub type ResolveProductsResult<T> = Result<T, ResolvePcdbProductsError>;

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use serde_json::Value;

    #[fixture]
    fn pcdb_heat_pumps() -> HashMap<String, Product> {
        serde_json::from_str(include_str!("../test/test_heat_pump_pcdb.json")).unwrap()
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
            "../test/test_heat_pump_input_transformed.json"
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
        let result = transform_heat_source_wets(&mut heat_pump_input, &pcdb_heat_pumps);

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

        let result = transform_heat_source_wets(&mut heat_pump_input, &pcdb_heat_pumps);

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
        serde_json::from_str(include_str!("../test/test_boilers_pcdb.json")).unwrap()
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
        serde_json::from_str(include_str!("../test/test_boiler_input_transformed.json")).unwrap()
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
        let result = transform_heat_source_wets(&mut boiler_input, &pcdb_boilers);

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
    fn test_transform_boiler_with_specified_location(
        pcdb_boilers: HashMap<String, Product>,
        expected_boilers: JsonValue,
    ) {
        let product_reference = "boiler_unknown_location";
        let mut boiler_input = boiler_input(product_reference, Some("internal"));

        let result = transform_heat_source_wets(&mut boiler_input, &pcdb_boilers);
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
}
