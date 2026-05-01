use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::{Product, Technology};
use crate::transform::{InvalidProductCategoryError, TransformResult};
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value as JsonValue};

pub fn transform(
    radiator: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
) -> TransformResult {
    if let Technology::Radiator {
        n,
        frac_convective,
        thermal_mass_per_m,
        c_per_m,
        ..
    } = &product.technology
    {
        radiator.insert("n".into(), n.to_f64().into());
        radiator.insert("frac_convective".into(), frac_convective.to_f64().into());
        radiator.insert("c_per_m".into(), c_per_m.to_f64().into());
        radiator.insert(
            "thermal_mass_per_m".into(),
            thermal_mass_per_m.to_f64().into(),
        );

        // now remove product reference and radiator type
        radiator.remove(PRODUCT_REFERENCE_FIELD);
        radiator.remove("radiator_type");
    } else {
        return Err(InvalidProductCategoryError::from((
            product_reference,
            "radiator",
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::catalogue::transformed_input_matches_expected;
    use crate::transform::space_heating::tests::SPACE_HEATING_PCDB_PRODUCTS;
    use serde_json::{Value, from_str, json};
    use std::collections::HashMap;

    fn input(product_reference: &str) -> JsonValue {
        json!({
            "wet_emitter_type": "radiator",
            "radiator_type": "standard",
            "product_reference": product_reference,
            "length": 7,
        })
    }

    #[test]
    fn test_transform_radiator() {
        let product_reference = "60";
        let mut input = input(product_reference);
        let expected: Map<String, Value> =
            from_str(include_str!("../../../test/radiator_transformed.json")).unwrap();

        let result = transform(
            input.as_object_mut().unwrap(),
            SPACE_HEATING_PCDB_PRODUCTS.get(product_reference).unwrap(),
            product_reference,
        );

        assert!(result.is_ok());
        transformed_input_matches_expected(&input, expected);
    }

    #[test]
    fn test_transform_radiator_errors_when_product_type_mismatch() {
        let product_reference = "hp";
        let mut input = input(product_reference);
        let pcdb_hps: HashMap<String, Product> =
            from_str(include_str!("../../../test/test_heat_pump_pcdb.json")).unwrap();

        let result = transform(
            input.as_object_mut().unwrap(),
            pcdb_hps.get(product_reference).unwrap(),
            product_reference,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("radiator"));
    }
}
