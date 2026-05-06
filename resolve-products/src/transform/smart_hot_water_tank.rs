use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::{Product, Technology};
use crate::transform::{
    InvalidProductCategoryError, ResolveProductsResult, product_reference_from_json_object,
};
use serde_json::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

pub fn transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
) -> ResolveProductsResult<()> {
    let hot_water_source = match json.pointer_mut("/HotWaterSource/hw cylinder") {
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
        _ => return Ok(()),
    };

    if let Some(source_type) = hot_water_source.get("type").and_then(|v| v.as_str()) {
        if matches!(source_type, "SmartHotWaterTank")
            && hot_water_source.contains_key(PRODUCT_REFERENCE_FIELD)
        {
            let product_reference = product_reference_from_json_object(hot_water_source)?;
            let product = &products[&product_reference];

            if let Technology::SmartHotWaterTank {
                max_flow_rate_pump_l_per_min,
                power_pump_kw,
                temp_usable,
                daily_losses,
                volume,
                ..
            } = &product.technology
            {
                hot_water_source.insert(
                    "max_flow_rate_pump_l_per_min".into(),
                    max_flow_rate_pump_l_per_min.as_f64().into(),
                );
                hot_water_source.insert("power_pump_kW".into(), power_pump_kw.as_f64().into());
                hot_water_source.insert("temp_usable".into(), temp_usable.as_f64().into());
                hot_water_source.insert("daily_losses".into(), daily_losses.as_f64().into());
                hot_water_source.insert("volume".into(), volume.as_f64().into());

                // now remove product reference
                hot_water_source.remove(PRODUCT_REFERENCE_FIELD);
            } else {
                return Err(InvalidProductCategoryError::from((
                    product_reference,
                    "smart hot water tank",
                ))
                .into());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::products::Product;
    use serde_json::{from_str, json};
    use std::collections::HashMap;

    fn input(product_reference: &str) -> JsonValue {
        json!({
            "HotWaterSource": {
                "hw cylinder": {
                    "type": "SmartHotWaterTank",
                    "EnergySupply_pump": "mains elec",
                    "product_reference": product_reference,
                    "ColdWaterSource": "mains water",
                    "HeatSource": {
                        "regularBoiler": {
                            "heater_position": 0.1,
                            "name": "regularBoiler",
                            "type": "HeatSourceWet"
                        }
                    }
                }
            },
        })
    }

    #[test]
    fn test_transform_smart_hot_water_tank() {
        let product_reference = "smart_tank";
        let mut input = input(product_reference);
        let expected: JsonValue =
            from_str(include_str!("../../test/smart_hw_tank_transformed.json")).unwrap();
        let pcdb_smart_tank: Product =
            from_str(include_str!("../../test/smart_hw_tank_pcdb.json")).unwrap();
        let result = transform(
            &mut input,
            &HashMap::from([(product_reference.into(), pcdb_smart_tank)]),
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
    fn test_transform_smart_hot_water_tank_errors_when_product_type_mismatch() {
        let product_reference = "hp";
        let mut input = input(product_reference);
        let pcdb_hps: HashMap<String, Product> =
            from_str(include_str!("../../test/test_heat_pump_pcdb.json")).unwrap();

        let result = transform(&mut input, &pcdb_hps);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("smart hot water tank")
        );
    }
}
