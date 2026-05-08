use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::{Product, Technology};
use crate::transform::{InvalidProductCategoryError, TransformResult};
use serde_json::{Map, Value};

pub(crate) fn transform(
    hiu: &mut Map<String, Value>,
    product: &Product,
    product_reference: &str,
) -> TransformResult {
    if let Technology::Hiu {
        hiu_daily_loss,
        max_power_water_55,
        ..
    } = &product.technology
    {
        hiu.insert("HIU_daily_loss".into(), hiu_daily_loss.as_f64().into());
        hiu.insert("power_max".into(), max_power_water_55.as_f64().into());

        // now remove product reference
        hiu.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        return Err(InvalidProductCategoryError::from((
            product_reference,
            "HIU",
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::catalogue::transformed_input_matches_expected;
    use serde_json::{from_str, json};
    use std::collections::HashMap;

    fn input(product_reference: &str) -> Value {
        json!({
            "type": "HIU",
            "EnergySupply": "mains elec",
            "product_reference": product_reference,
            "building_level_distribution_losses": 1,
            "is_heat_network": false,
        })
    }

    #[test]
    fn test_transform_hiu() {
        let product_reference = "hiu";
        let mut input = input(product_reference);
        let expected: Map<String, Value> =
            from_str(include_str!("../fixtures/hiu_transformed.json")).unwrap();
        let hiu_pcdb: HashMap<String, Product> =
            from_str(include_str!("../fixtures/hiu_pcdb.json")).unwrap();

        let result = transform(
            input.as_object_mut().unwrap(),
            hiu_pcdb.get(product_reference).unwrap(),
            product_reference,
        );

        assert!(result.is_ok());
        transformed_input_matches_expected(&input, expected);
    }

    #[test]
    fn test_transform_hiu_errors_when_product_type_mismatch() {
        let product_reference = "hp";
        let mut input = input(product_reference);
        let pcdb_hps: HashMap<String, Product> =
            from_str(include_str!("../fixtures/heat_pump_pcdb.json")).unwrap();

        let result = transform(
            input.as_object_mut().unwrap(),
            pcdb_hps.get(product_reference).unwrap(),
            product_reference,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("HIU"));
    }
}
