use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::{Product, Technology};
use crate::transform::{InvalidProductCategoryError, TransformResult};
use serde_json::{Map, Value as JsonValue};

pub fn transform(
    underfloor_heating: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
) -> TransformResult {
    if let Technology::UnderfloorHeating {
        system_performance_factor,
        frac_convective,
        equivalent_specific_thermal_mass,
        ..
    } = &product.technology
    {
        underfloor_heating.insert(
            "system_performance_factor".into(),
            system_performance_factor.as_f64().into(),
        );
        underfloor_heating.insert("frac_convective".into(), frac_convective.as_f64().into());
        underfloor_heating.insert(
            "equivalent_specific_thermal_mass".into(),
            equivalent_specific_thermal_mass.as_f64().into(),
        );

        // now remove product reference
        underfloor_heating.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        return Err(InvalidProductCategoryError::from((
            product_reference,
            "underfloor heating",
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::catalogue::transformed_input_matches_expected;
    use crate::transform::space_heat_system::tests::SPACE_HEATING_PCDB_PRODUCTS;
    use serde_json::{from_str, json};

    fn input(product_reference: &str) -> JsonValue {
        json!({
            "wet_emitter_type": "ufh",
            "product_reference": product_reference,
            "emitter_floor_area": 42,
        })
    }

    #[test]
    fn test_transform_underfloor_heating() {
        let product_reference = "720";
        let mut input = input(product_reference);
        let expected: Map<String, JsonValue> =
            from_str(include_str!("../../../test/ufh_transformed.json")).unwrap();

        let result = transform(
            input.as_object_mut().unwrap(),
            SPACE_HEATING_PCDB_PRODUCTS.get(product_reference).unwrap(),
            product_reference,
        );

        assert!(result.is_ok());
        transformed_input_matches_expected(&input, expected);
    }
}
