use crate::errors::ResolvePcdbProductsError;
use crate::products::{
    find_products_for_references, HeatPumpTestDatum, HeatPumpTestLetter, Product, Technology,
};
use crate::{extract_product_references, PRODUCT_REFERENCE_FIELD};
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

    transform_heat_pumps(json, &products)
}

fn transform_heat_pumps(
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
        if let JsonValue::Object(heat_pump) = value {
            if heat_pump
                .get("type")
                .is_some_and(|v| matches!(v, JsonValue::String(s) if s == "HeatPump"))
                && heat_pump.contains_key(PRODUCT_REFERENCE_FIELD)
            {
                let product_reference = std::string::String::from(
                    heat_pump[PRODUCT_REFERENCE_FIELD].as_str().ok_or_else(|| {
                        ResolvePcdbProductsError::InvalidProductCategoryReference(
                            heat_pump[PRODUCT_REFERENCE_FIELD].clone(),
                        )
                    })?,
                );
                transform_heat_pump(
                    heat_pump,
                    &products[product_reference.as_str()],
                    &product_reference,
                )?;
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

    // can remove following "allow" when there is more than one technology variant modelled
    #[allow(irrefutable_let_patterns)]
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
            heat_pump.insert(
                "min_modulation_rate_35".into(),
                minimum_modulation_rate_35.to_f64().into(),
            );
            heat_pump.insert(
                "min_modulation_rate_55".into(),
                minimum_modulation_rate_55.to_f64().into(),
            );
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
        heat_pump.insert(
            "power_max_backup".into(),
            power_maximum_backup.map(|x| x.to_f64()).into(),
        );
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
        heat_pump.insert("time_delay_backup".into(), 2.into()); // canned value for now using one of the values from demo files
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

    if category_mismatches.len() > 0 {
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

    #[test]
    fn test_transform_heat_pumps() {
        let pcdb_heat_pump: Product = serde_json::from_str(include_str!("../test/test_heat_pump_pcdb.json")).unwrap();
        let pcdb_heat_pumps: &HashMap<String, Product> = &HashMap::from([("123".into(), pcdb_heat_pump)]);
        let mut input: JsonValue = serde_json::from_str(include_str!("../test/test_heat_pump_input.json")).unwrap();

        let result = transform_heat_pumps(&mut input, pcdb_heat_pumps);

        assert!(result.is_ok());
    }
}
