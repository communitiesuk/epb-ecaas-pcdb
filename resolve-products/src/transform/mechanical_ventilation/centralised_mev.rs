use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::in_use_factors::{InUseFactorsAccess, MechanicalVentilationSystemType};
use crate::products::{Product, Technology};
use crate::transform::ResolveProductsResult;
use crate::transform::mechanical_ventilation::resolve_sfp_in_use_factor;
use serde_json::{Map, Value as JsonValue, json};

pub(crate) async fn transform(
    mech_vent: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
    number_of_wetrooms: usize,
    in_use_factors_access: &impl InUseFactorsAccess,
) -> ResolveProductsResult<()> {
    if let Technology::CentralisedMev { test_data, .. } = &product.technology {
        let test_data_matching_number_of_wet_rooms: Vec<_> = test_data
            .iter()
            .filter(|a| a.configuration == number_of_wetrooms - 1) // configuration excludes kitchen, number_of_wetrooms includes it
            .collect();

        let test_datum = match test_data_matching_number_of_wet_rooms.as_slice() {
            [one] => one,
            [] => {
                return Err(ResolvePcdbProductsError::InvalidCombination(format!(
                    "Centralised MEV product {} from PCDB has no configuration for specified number of wet rooms ({:?})",
                    product_reference, number_of_wetrooms
                )));
            }
            _ => {
                return Err(ResolvePcdbProductsError::InvalidProduct(
                    product_reference.to_string(),
                    "Centralised MEV product from PCDB has ambiguous test data",
                ));
            }
        };

        if !mech_vent.contains_key("measured_fan_power")
            || !mech_vent.contains_key("measured_air_flow_rate")
        {
            mech_vent.insert("SFP".into(), json!(test_datum.sfp.as_f64()));
        }

        let duct_type = &test_datum.duct_type;
        let installed_under_approved_scheme = mech_vent.get("installed_under_approved_scheme").and_then(JsonValue::as_bool).ok_or_else(|| { ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck("Centralised MeV input was expected to have an 'installed_under_approved_scheme' field that is a boolean")})?;
        let sfp_in_use_factor = resolve_sfp_in_use_factor(
            in_use_factors_access,
            &MechanicalVentilationSystemType::CentralisedMev,
            duct_type,
            installed_under_approved_scheme,
        )
        .await?;
        mech_vent.insert(
            "SFP_in_use_factor".into(),
            json!(sfp_in_use_factor.as_f64()),
        );

        mech_vent.remove("installed_under_approved_scheme");
        mech_vent.remove(PRODUCT_REFERENCE_FIELD);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_use_factors::mocks::FixtureBackedInUseFactorsAccess;
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

    #[fixture]
    fn in_use_factor_access() -> impl InUseFactorsAccess {
        FixtureBackedInUseFactorsAccess
    }

    fn centralised_mev_input(product_reference: &str) -> Value {
        json!({
            "vent_type": "Centralised continuous MEV",
            "EnergySupply": "mains elec",
            "product_reference": product_reference,
            "design_outdoor_air_flow_rate": 80,
            "installed_under_approved_scheme": true,
            "mid_height_air_flow_path": 1.5,
            "orientation360": 90,
            "pitch": 60
        })
    }

    #[tokio::test]
    #[rstest]
    #[case::two_wet_rooms("centralisedMev2WetRooms", 2)]
    #[case::three_wet_rooms("centralisedMev3WetRooms", 3)]
    #[case::six_wet_rooms("centralisedMev6WetRooms", 6)]
    async fn test_transform_centralised_mev(
        pcdb_products: HashMap<String, Product>,
        in_use_factor_access: impl InUseFactorsAccess,
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
            &in_use_factor_access,
        )
        .await;
        assert!(result.is_ok());

        let expected_input = expected_transformed_mech_vent_input(product_reference);
        transformed_input_matches_expected(&mev_input, expected_input);
    }

    #[tokio::test]
    #[rstest]
    async fn test_transform_centralised_mev_with_measured_fields_provided(
        pcdb_products: HashMap<String, Product>,
        in_use_factor_access: impl InUseFactorsAccess,
    ) {
        let product_reference = "centralisedMevWithMeasuredFieldsProvided";
        let mut mev_input = centralised_mev_input(product_reference);
        let pcdb_mev = pcdb_products.get("centralisedMev").unwrap();

        {
            let mvhr_object = mev_input.as_object_mut().unwrap();
            mvhr_object.insert("measured_fan_power".to_string(), json!(12.26));
            mvhr_object.insert("measured_air_flow_rate".to_string(), json!(37));
        }

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
            6,
            &in_use_factor_access,
        )
        .await;
        assert!(result.is_ok());

        let expected_input = expected_transformed_mech_vent_input(product_reference);
        transformed_input_matches_expected(&mev_input, expected_input);
    }

    #[tokio::test]
    #[rstest]
    async fn test_transform_centralised_mev_errors_given_unsupported_number_of_wet_rooms(
        pcdb_products: HashMap<String, Product>,
        in_use_factor_access: impl InUseFactorsAccess,
    ) {
        let product_reference = "centralisedMev";
        let mut mev_input = centralised_mev_input(product_reference);
        let pcdb_mev = pcdb_products.get(product_reference).unwrap();

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
            8,
            &in_use_factor_access,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[rstest]
    async fn test_transform_decentralised_mev_errors_given_ambiguous_configuration_from_pcdb(
        pcdb_products: HashMap<String, Product>,
        in_use_factor_access: impl InUseFactorsAccess,
    ) {
        let product_reference = "centralisedMevWithTwoEntriesForTheSameConfiguration";
        let mut mev_input = centralised_mev_input(product_reference);
        let pcdb_mev = pcdb_products.get(product_reference).unwrap();

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
            1,
            &in_use_factor_access,
        )
        .await;
        assert!(result.is_err());
    }
}
