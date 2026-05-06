use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::Product;
use crate::transform::ResolveProductsResult;
use serde_json::{Map, Value as JsonValue};

pub(crate) fn transform(
    mech_vent: &mut Map<String, JsonValue>,
    _product: &Product,
    _product_reference: &str,
    _number_of_wetrooms: usize,
) -> ResolveProductsResult<()> {
    mech_vent.remove(PRODUCT_REFERENCE_FIELD);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::products::Product;
    use crate::transform::mechanical_ventilation::mechanical_ventilation_pcdb_products;
    use rstest::{fixture, rstest};
    use serde_json::{Value, json};
    use std::collections::HashMap;

    #[fixture]
    fn pcdb_products() -> HashMap<String, Product> {
        mechanical_ventilation_pcdb_products()
    }

    fn centralised_mvhr_input(product_reference: &str) -> Value {
        json!({
            "vent_type": "MVHR",
            "EnergySupply": "mains elec",
            "product_reference": product_reference,
            "design_outdoor_air_flow_rate": 80,
            "installed_under_approved_scheme": true,
            "mvhr_location": "inside",
            "ductwork": [],
            "position_intake": {
                "mid_height_air_flow_path": 1.5,
                "orientation360": 90,
                "pitch": 60
            },
            "position_exhaust": {
                "mid_height_air_flow_path": 1.6,
                "orientation360": 90,
                "pitch": 60
            }
        })
    }

    #[rstest]
    fn test_transform_centralised_mev(pcdb_products: HashMap<String, Product>) {
        let product_reference = "centralisedMvhr";
        let number_of_wet_rooms = 1;
        let mut mev_input = centralised_mvhr_input(product_reference);
        let pcdb_mev = pcdb_products.get(product_reference).unwrap();

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
            number_of_wet_rooms,
        );
        assert!(result.is_ok());

        // let expected_input = expected_transformed_mech_vent_input(product_reference);
        // transformed_input_matches_expected(&mev_input, expected_input);
    }
}
