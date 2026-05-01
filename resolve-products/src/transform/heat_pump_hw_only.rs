use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, Technology};
use crate::transform::{ResolveProductsResult, product_reference_from_json_object};
use serde_json::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

pub fn _transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
) -> ResolveProductsResult<()> {
    let heat_pumps = match json.pointer_mut("/HeatPump_HWOnly") {
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
        _ => return Ok(()),
    };

    for value in heat_pumps.values_mut() {
        if let JsonValue::Object(heat_pump) = value {
            if heat_pump.contains_key(PRODUCT_REFERENCE_FIELD) {
                let product_ref = product_reference_from_json_object(heat_pump)?;
                let product = &products[&product_ref];
                let mut category_mismatches = vec![];

                if let Technology::HeatPumpHotWaterOnly { .. } = &product.technology {
                    // now remove product reference
                    heat_pump.remove(PRODUCT_REFERENCE_FIELD);
                } else {
                    category_mismatches.push(format!(
                        "Product reference '{product_ref}' does not relate to a heat pump hot water only."
                    ));
                }

                if !category_mismatches.is_empty() {
                    return Err(ResolvePcdbProductsError::ProductCategoryMismatches(
                        category_mismatches,
                    ));
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{from_str, json};
    use std::collections::HashMap;

    fn input(product_reference: &str) -> JsonValue {
        json!({"HeatPump_HWOnly": {
                product_reference: {
                    "EnergySupply": "mains elec",
                    "product_reference": product_reference,
                }}
        })
    }

    #[test]
    #[ignore = "todo complete transformed test file and mapping"]
    fn test_transform_heat_pump_hw_only() {
        let product_reference = "62";
        let mut input = input(product_reference);
        let expected: JsonValue =
            from_str(include_str!("../../test/hp_hw_only_transformed.json")).unwrap();
        let pcdb_hp_hw_only: Product = serde_json::from_value(
            from_str::<JsonValue>(include_str!("../../test/hp_hw_only_pcdb.json"))
                .unwrap()
                .get(product_reference)
                .unwrap()
                .clone(),
        )
        .unwrap();

        let result = _transform(
            &mut input,
            &HashMap::from([(product_reference.into(), pcdb_hp_hw_only)]),
        );

        assert!(result.is_ok());
        assert_eq!(
            input,
            expected,
            "actual: {}\nexpected: {}",
            serde_json::to_string_pretty(&input).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap()
        );
    }

    #[test]
    fn test_transform_heat_pump_hw_only_errors_when_product_type_mismatch() {
        let product_reference = "hp";
        let mut input = input(product_reference);
        let pcdb_hps: HashMap<String, Product> =
            from_str(include_str!("../../test/test_heat_pump_pcdb.json")).unwrap();

        let result = _transform(&mut input, &pcdb_hps);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Product reference 'hp' does not relate to a heat pump hot water only.")
        );
    }
}
