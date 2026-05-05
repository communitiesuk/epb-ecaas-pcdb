use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::{Product, Technology};
use crate::transform::{InvalidProductCategoryError, TransformResult};
use serde_json::{Map, Value as JsonValue};

pub fn _transform(
    smart_hot_water_tank: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
) -> TransformResult {
    if let Technology::SmartHotWaterTank { .. } = &product.technology {
        // now remove product reference
        smart_hot_water_tank.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        return Err(InvalidProductCategoryError::from((
            product_reference,
            "smart hot water tank",
        )));
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
            "EnergySupply_pump": "mains elec",
            "product_reference": product_reference,
            "HeatSource": {
                "regularBoiler": {
                    "heater_position": 0.1,
                    "name": "regularBoiler",
                    "type": "HeatSourceWet"
                }
            },
        })
    }

    #[test]
    fn test_transform_smart_hot_water_tank_errors_when_product_type_mismatch() {
        let product_reference = "hp";
        let mut input = input(product_reference);
        let pcdb_hps: HashMap<String, Product> =
            from_str(include_str!("../../../test/test_heat_pump_pcdb.json")).unwrap();

        let result = _transform(
            input.as_object_mut().unwrap(),
            pcdb_hps.get(product_reference).unwrap(),
            product_reference,
        );

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("smart hot water tank")
        );
    }
}
