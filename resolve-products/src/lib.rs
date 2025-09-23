pub mod errors;
mod products;

use crate::errors::{JsonPathError, JsonValidationError, ResolvePcdbProductsError};
use crate::products::{
    find_products_for_references, HeatPumpTestDatum, HeatPumpTestLetter, Product, Technology,
};
use itertools::Itertools;
use jsonpath_rust::JsonPath;
use jsonschema::Validator;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{json, Map, Value as JsonValue};
use smartstring::alias::String;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{BufReader, Cursor, Read};
use std::sync::LazyLock;

pub fn resolve_products(json: impl Read) -> ResolveProductsResult<impl Read + Debug> {
    let reader = BufReader::new(json);

    let mut input: JsonValue =
        serde_json::from_reader(reader).map_err(|_| ResolvePcdbProductsError::InvalidJson)?;

    // validate first
    if let Err(e) = SCHEMA_VALIDATOR.validate(&input) {
        return Err(JsonValidationError::from(e).into());
    }

    transform_json(&mut input)?;

    Ok(Cursor::new(input.to_string()))
}

static SCHEMA_VALIDATOR: LazyLock<Validator> = LazyLock::new(|| {
    let schema = serde_json::from_str(include_str!("./combined_schema.json"))
        .expect("Schema file was not parseable.");
    jsonschema::validator_for(&schema).expect(
        "Failed to create validator for schema. \
             This is a bug in resolve-products. Please report it.",
    )
});

fn extract_product_references(json: &JsonValue) -> ResolveProductsResult<Vec<String>> {
    let instances = match json.query_with_path(&format!("$..{PRODUCT_REFERENCE_FIELD}")) {
        Ok(instances) => instances,
        Err(json_path_error) => {
            return Err(JsonPathError::from(json_path_error).into());
        }
    };

    instances
        .into_iter()
        .map(|v| -> ResolveProductsResult<String> {
            match v.val() {
                JsonValue::String(value) => Ok(String::from(value)),
                value => Err(ResolvePcdbProductsError::InvalidProductCategoryReference(
                    value.to_owned(),
                )),
            }
        })
        .collect::<ResolveProductsResult<Vec<String>>>()
}

const PRODUCT_REFERENCE_FIELD: &'static str = "product_reference";

fn transform_json(json: &mut JsonValue) -> ResolveProductsResult<()> {
    let product_references = extract_product_references(json)?;
    let products = find_products_for_references(&product_references)?;

    transform_heat_pumps(json, &products)
}

fn transform_heat_pumps(
    json: &mut JsonValue,
    products: &HashMap<&str, &Product>,
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
                    products[product_reference.as_str()],
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
    if let Technology::AirSourceHeatPump {
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
        heat_pump.insert(
            "power_heating_circ_pump".into(),
            power_heating_circ_pump.to_f64().into(),
        );
        heat_pump.insert(
            "power_heating_warm_air_fan".into(),
            power_heating_warm_air_fan.to_f64().into(),
        );
        heat_pump.insert(
            "power_max_backup".into(),
            power_maximum_backup.to_f64().into(),
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
                            degradation_coefficient,
                            design_flow_temperature,
                            temperature_outlet,
                            temperature_source,
                            temperature_test,
                            test_letter,
                        } = datum;
                        // 'E' is not accepted in HEM, so filter this out
                        (*test_letter != HeatPumpTestLetter::E).then_some(json!({
                            "capacity": capacity.to_f64(),
                            "cop": coefficient_of_performance.to_f64(),
                            "degradation_coeff": degradation_coefficient.to_f64(),
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

type ResolveProductsResult<T> = Result<T, ResolvePcdbProductsError>;

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn test_failing_input_fails_schema_check() {
        let input = r#"{"foo": "bar"}"#;
        let result = resolve_products(Cursor::new(input));
        assert!(result.is_err());
    }

    #[fixture]
    fn heat_pump_product_ref_document() -> JsonValue {
        serde_json::from_str(include_str!("../test/demo_heat_pump_product_ref.json")).unwrap()
    }

    #[rstest]
    fn test_extract_product_references_from_document(heat_pump_product_ref_document: JsonValue) {
        assert_eq!(
            extract_product_references(&heat_pump_product_ref_document).unwrap(),
            [String::from("HEATPUMP-MEDIUM")]
        );
    }

    #[rstest]
    fn test_resolve_products_produces_passing_output() {
        let json = Cursor::new(include_str!("../test/demo_heat_pump_product_ref.json"));
        let result = resolve_products(json);
        assert!(result.is_ok());
        let result_json: JsonValue =
            serde_json::from_reader(BufReader::new(result.unwrap())).unwrap();
        let schema = serde_json::from_str(include_str!("../test/target_schema.json"))
            .expect("Schema file was not parseable.");
        let validator =
            jsonschema::validator_for(&schema).expect("Failed to create validator for schema.");
        for error in validator.iter_errors(&result_json) {
            eprintln!("Error: {error}");
            eprintln!("Location: {}", error.instance_path);
        }
        assert!(validator.is_valid(&result_json));
    }
}
