use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, Technology};
use crate::transform::ResolveProductsResult;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value as JsonValue};
use std::vec;

pub fn _transform(
    radiator: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
) -> ResolveProductsResult<()> {
    let mut category_mismatches = vec![];

    if let Technology::Radiator {
        n,
        frac_convective,
        thermal_mass_per_m,
        c,
        ..
    } = &product.technology
    {
        radiator.insert("n".into(), n.to_f64().into());
        radiator.insert("frac_convective".into(), frac_convective.to_f64().into());
        radiator.insert("c_per_m".into(), c.to_f64().into());
        radiator.insert(
            "thermal_mass_per_m".into(),
            thermal_mass_per_m.to_f64().into(),
        );

        // now remove product reference and radiator type
        radiator.remove(PRODUCT_REFERENCE_FIELD);
        radiator.remove("radiator_type");
    } else {
        category_mismatches.push(format!(
            "Product reference '{product_reference}' does not relate to a radiator."
        ));
    }

    if !category_mismatches.is_empty() {
        return Err(ResolvePcdbProductsError::ProductCategoryMismatches(
            category_mismatches,
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_transform_radiator() {
        let product_reference = "60";
        let mut input = json!({
            "wet_emitter_type": "radiator",
            "radiator_type": "standard",
            "product_reference": product_reference,
            "length": 7,
        });
        let pcdb_radiator =
            serde_json::from_str(include_str!("../../../test/test_radiator_pcdb.json")).unwrap();
        let expected: JsonValue = serde_json::from_str(include_str!(
            "../../../test/test_radiator_input_transformed.json"
        ))
        .unwrap();

        let result = _transform(
            input.as_object_mut().unwrap(),
            &pcdb_radiator,
            product_reference,
        );

        assert!(result.is_ok());

        let mut actual_keys = input.as_object().unwrap().keys().collect_vec();
        actual_keys.sort();
        let mut expected_keys = expected.as_object().unwrap().keys().collect_vec();
        expected_keys.sort();

        assert_eq!(actual_keys, expected_keys);

        for key in expected_keys {
            assert_eq!(input[key], expected[key], "{:?}", key);
        }
    }

    #[test]
    fn test_transform_radiator_errors_when_product_type_mismatch() {
        let product_reference = "hp";
        let mut input = json!({
            "wet_emitter_type": "radiator",
            "radiator_type": "standard",
            "product_reference": product_reference,
            "length": 7,
        });
        let pcdb_hps: HashMap<String, Product> =
            serde_json::from_str(include_str!("../../../test/test_heat_pump_pcdb.json")).unwrap();

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
                .contains("Product reference 'hp' does not relate to a radiator.")
        );
    }
}
