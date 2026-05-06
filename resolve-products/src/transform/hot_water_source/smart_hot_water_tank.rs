use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::{Product, Technology};
use crate::transform::{InvalidProductCategoryError, TransformResult};
use serde_json::{Map, Value as JsonValue};

pub fn _transform(
    smart_hot_water_tank: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
) -> TransformResult {
    if let Technology::SmartHotWaterTank {
        max_flow_rate_pump_l_per_min,
        power_pump_kw,
        temp_usable,
        daily_losses,
        volume,
        ..
    } = &product.technology
    {
        smart_hot_water_tank.insert(
            "max_flow_rate_pump_l_per_min".into(),
            max_flow_rate_pump_l_per_min.as_f64().into(),
        );
        smart_hot_water_tank.insert("power_pump_kW".into(), power_pump_kw.as_f64().into());
        smart_hot_water_tank.insert("temp_usable".into(), temp_usable.as_f64().into());
        smart_hot_water_tank.insert("daily_losses".into(), daily_losses.as_f64().into());
        smart_hot_water_tank.insert("volume".into(), volume.as_f64().into());

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
    use crate::transform::catalogue::transformed_input_matches_expected;
    use serde_json::{Value, from_str, json};
    use std::collections::HashMap;
    use std::sync::LazyLock;

    fn input(product_reference: &str) -> JsonValue {
        json!({
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
            },
        })
    }

    pub(crate) static HOT_WATER_SOURCE_PCDB_PRODUCTS: LazyLock<
        HashMap<smartstring::alias::String, Product>,
    > = LazyLock::new(|| {
        from_str(include_str!("../../../test/hot_water_source_pcdb.json")).unwrap()
    });

    #[test]
    fn test_transform_smart_hot_water_tank() {
        let product_reference = "smart_tank";
        let mut input = input(product_reference);
        let expected: Map<String, Value> =
            from_str(include_str!("../../../test/smart_hw_tank_transformed.json")).unwrap();

        let result = _transform(
            input.as_object_mut().unwrap(),
            HOT_WATER_SOURCE_PCDB_PRODUCTS
                .get(product_reference)
                .unwrap(),
            product_reference,
        );

        assert!(result.is_ok());
        transformed_input_matches_expected(&input, expected);
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
