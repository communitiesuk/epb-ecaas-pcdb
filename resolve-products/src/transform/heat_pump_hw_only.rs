use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::{Product, Technology};
use crate::transform::{
    InvalidProductCategoryError, ResolveProductsResult, product_reference_from_json_object,
};
use serde_json::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

pub fn _transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
) -> ResolveProductsResult<()> {
    let heat_sources = match json.pointer_mut("/HeatSource") {
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

                    if let Technology::HeatPumpHotWaterOnly { .. } = &product.technology {
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
    use serde_json::{from_str, json};
    use std::collections::HashMap;

    fn input(product_reference: &str) -> JsonValue {
        json!({
            "HeatSource": {
                "hw_only_hp": {
                    "type": "HeatPump_HWOnly",
                    "heater_position": 0.1,
                    "EnergySupply": "mains elec",
                    "product_reference": product_reference,
                }
            }
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
                .contains("hot water only heat pump")
        );
    }
}
