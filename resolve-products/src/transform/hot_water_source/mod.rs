mod smart_hot_water_tank;

use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::Product;
use crate::transform::{ResolveProductsResult, product_reference_from_json_object};
use serde_json::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

pub(crate) fn transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
) -> ResolveProductsResult<()> {
    let hot_water_source = match json.pointer_mut("/HotWaterSource/hw cylinder") {
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
        _ => return Ok(()),
    };

    if let Some(source_type) = hot_water_source.get("type").and_then(|v| v.as_str()) {
        match source_type {
            "SmartHotWaterTank" if hot_water_source.contains_key(PRODUCT_REFERENCE_FIELD) => {
                let product_reference = product_reference_from_json_object(hot_water_source)?;

                smart_hot_water_tank::transform(
                    hot_water_source,
                    &products[&product_reference],
                    &product_reference,
                )?
            }
            "HIU" => {}
            _ => {} // TODO could add warning about unexpected type being reached
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
    use std::sync::LazyLock;

    pub(crate) static HOT_WATER_SOURCE_PCDB_PRODUCTS: LazyLock<HashMap<String, Product>> =
        LazyLock::new(|| {
            from_str(include_str!("../../../test/hot_water_source_pcdb.json")).unwrap()
        });

    #[test]
    fn test_transform_hot_water_source() {
        let mut input =
            from_str(include_str!("../../../test/hot_water_source_input.json")).unwrap();
        let expected_smart_hw_tank: JsonValue =
            from_str(include_str!("../../../test/smart_hw_tank_transformed.json")).unwrap();
        let expected_input = json!({
            "HotWaterSource": {
                "hw cylinder": expected_smart_hw_tank
            }
        });

        let result = transform(&mut input, &HOT_WATER_SOURCE_PCDB_PRODUCTS);

        assert!(result.is_ok());
        assert_eq!(
            input,
            expected_input,
            "actual: {}\nexpected: {}",
            serde_json::to_string_pretty(&input).unwrap(),
            serde_json::to_string_pretty(&expected_input).unwrap()
        );
    }
}
