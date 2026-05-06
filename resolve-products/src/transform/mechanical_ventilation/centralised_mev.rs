use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, Technology};
use crate::transform::ResolveProductsResult;
use serde_json::{Map, Value as JsonValue, json};

pub(crate) fn transform(
    mech_vent: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
    number_of_wetrooms: usize,
) -> ResolveProductsResult<()> {
    if let Technology::CentralisedMev { test_data, .. } = &product.technology {
        let test_data_matching_number_of_wet_rooms: Vec<_> = test_data
            .iter()
            .filter(|a| a.configuration == number_of_wetrooms)
            .collect();

        let test_datum = match test_data_matching_number_of_wet_rooms.as_slice() {
            [one] => one,
            [] => {
                return Err(ResolvePcdbProductsError::InvalidCombination(format!(
                    "Centralised MeV product {} from PCDB has no configuration for specified number of wet rooms ({:?})",
                    product_reference, number_of_wetrooms
                )));
            }
            _ => {
                return Err(ResolvePcdbProductsError::InvalidProduct(
                    product_reference.to_string(),
                    "Centralised MeV product from PCDB has ambiguous test data",
                ));
            }
        };

        mech_vent.insert("SFP".into(), json!(test_datum.sfp.as_f64()));
        mech_vent.remove("installed_under_approved_scheme"); // TODO: review
        mech_vent.remove(PRODUCT_REFERENCE_FIELD);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::catalogue::transformed_input_matches_expected;
    use crate::transform::mechanical_ventilation::{
        expected_transformed_mech_vent_input, mechanical_ventilation_pcdb_products,
    };
    use rstest::{fixture, rstest};
    use serde_json::{Value, json};
    use std::collections::HashMap;

    #[fixture]
    fn pcdb_products() -> HashMap<String, Product> {
        mechanical_ventilation_pcdb_products()
    }

    fn centralised_mev_input(product_reference: &str) -> Value {
        json!({
            "vent_type": "Centralised continuous MEV",
            "EnergySupply": "mains elec",
            "product_reference": product_reference,
            "design_outdoor_air_flow_rate": 80,
            "installed_under_approved_scheme": true,
            "measured_fan_power": 12.26,
            "measured_air_flow_rate": 37,
            "mid_height_air_flow_path": 1.5,
            "orientation360": 90,
            "pitch": 60
        })
    }

    #[rstest]
    #[case::one_wet_room("centralisedMev1WetRoom", 1)]
    #[case::two_wet_rooms("centralisedMev2WetRooms", 2)]
    #[case::eleven_wet_rooms("centralisedMev6WetRooms", 6)]
    fn test_transform_centralised_mev(
        pcdb_products: HashMap<String, Product>,
        #[case] product_reference: &str,
        #[case] number_of_wet_rooms: usize,
    ) {
        let mut mev_input = centralised_mev_input(product_reference);
        let pcdb_mev = pcdb_products.get("centralisedMev").unwrap();

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
            number_of_wet_rooms,
        );
        assert!(result.is_ok());

        let expected_input = expected_transformed_mech_vent_input(product_reference);
        transformed_input_matches_expected(&mev_input, expected_input);
    }

    #[rstest]
    fn test_transform_centralised_mev_errors_given_unsupported_number_of_wet_rooms(
        pcdb_products: HashMap<String, Product>,
    ) {
        let product_reference = "centralisedMev";
        let mut mev_input = centralised_mev_input(product_reference);
        let pcdb_mev = pcdb_products.get(product_reference).unwrap();

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
            7,
        );
        assert!(result.is_err());
    }

    #[rstest]
    fn test_transform_decentralised_mev_errors_given_ambiguous_configuration_from_pcdb(
        pcdb_products: HashMap<String, Product>,
    ) {
        let product_reference = "centralisedMevWithTwoEntriesForTheSameConfiguration";
        let mut mev_input = centralised_mev_input(product_reference);
        let pcdb_mev = pcdb_products.get(product_reference).unwrap();

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
            1,
        );
        assert!(result.is_err());
    }
}
