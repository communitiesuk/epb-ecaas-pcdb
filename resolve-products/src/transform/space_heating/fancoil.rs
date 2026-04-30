use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::{FanCoilTestDatum, Product, Technology};
use crate::transform::ResolveProductsResult;
use itertools::Itertools;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{Map, Value as JsonValue, json};
use std::collections::BTreeSet;

pub fn transform(
    fancoil: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
) -> ResolveProductsResult<()> {
    let mut category_mismatches = vec![];

    if let Technology::FanCoil {
        frac_convective,
        test_data,
        ..
    } = &product.technology
    {
        fancoil.insert("frac_convective".into(), frac_convective.as_f64().into());
        fancoil.insert(
            "fancoil_test_data".into(),
            json!(test_data_for_target(test_data)),
        );

        // now remove product reference
        fancoil.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        category_mismatches.push(format!(
            "Product reference '{product_reference}' does not relate to a fancoil product."
        ));
    }

    if !category_mismatches.is_empty() {
        return Err(ResolvePcdbProductsError::ProductCategoryMismatches(
            category_mismatches,
        ));
    }

    Ok(())
}

fn test_data_for_target(test_data: &[FanCoilTestDatum]) -> JsonValue {
    let mut fan_speed_data: Vec<FanSpeedDatum> = Default::default();
    let mut fan_power_w: BTreeSet<Decimal> = Default::default();

    // ensure we normalise the order of the test data as it can come in unsorted
    for test_datum in test_data.iter().sorted_by(|a, b| {
        a.fan_speed
            .cmp(&b.fan_speed)
            .then_with(|| a.temperature_diff.cmp(&b.temperature_diff))
    }) {
        let fan_speed_datum = fan_speed_data
            .iter_mut()
            .find(|datum| datum.temperature_diff == test_datum.temperature_diff.as_f64());

        if let Some(fan_speed_datum) = fan_speed_datum {
            fan_speed_datum.add_power_output(test_datum.power_output.as_f64());
        } else {
            let mut new_datum = FanSpeedDatum::new(test_datum.temperature_diff.as_f64());
            new_datum.add_power_output(test_datum.power_output.as_f64());
            fan_speed_data.push(new_datum);
        }

        fan_power_w.insert(test_datum.fan_power_w);
    }

    json!({
        "fan_speed_data": fan_speed_data,
        "fan_power_W": fan_power_w.iter().map(Decimal::as_f64).collect_vec(),
    })
}

#[derive(Debug, Default, Clone, Serialize)]
struct FanSpeedDatum {
    temperature_diff: f64,
    power_output: Vec<f64>,
}

impl FanSpeedDatum {
    fn new(temperature_diff: f64) -> Self {
        Self {
            temperature_diff,
            ..Default::default()
        }
    }

    fn add_power_output(&mut self, power_output: f64) {
        self.power_output.push(power_output);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::catalogue::transformed_input_matches_expected;
    use crate::transform::space_heating::tests::SPACE_HEATING_PCDB_PRODUCTS;
    use serde_json::{from_str, json};

    fn input(product_reference: &str) -> JsonValue {
        json!({
            "wet_emitter_type": "fancoil",
            "product_reference": product_reference,
            "n_units": 2,
        })
    }

    #[test]
    fn test_transform_underfloor_heating() {
        let product_reference = "999";
        let mut input = input(product_reference);
        let expected: Map<String, JsonValue> =
            from_str(include_str!("../../../test/fancoil_transformed.json")).unwrap();

        let result = transform(
            input.as_object_mut().unwrap(),
            SPACE_HEATING_PCDB_PRODUCTS.get(product_reference).unwrap(),
            product_reference,
        );

        assert!(result.is_ok());
        transformed_input_matches_expected(&input, expected);
    }
}
