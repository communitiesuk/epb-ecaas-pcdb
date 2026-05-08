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
    if let Technology::CentralisedMvhr { test_data, .. } = &product.technology {
        let test_data_matching_number_of_wet_rooms: Vec<_> = test_data
            .iter()
            .filter(|a| a.configuration == number_of_wetrooms - 1) // configuration excludes kitchen, number_of_wetrooms includes it
            .collect();

        let test_datum = match test_data_matching_number_of_wet_rooms.as_slice() {
            [one] => one,
            [] => {
                return Err(ResolvePcdbProductsError::InvalidCombination(format!(
                    "Centralised MVHR product {} from PCDB has no configuration for specified number of wet rooms ({:?})",
                    product_reference, number_of_wetrooms
                )));
            }
            _ => {
                return Err(ResolvePcdbProductsError::InvalidProduct(
                    product_reference.to_string(),
                    "Centralised MVHR product from PCDB has ambiguous test data",
                ));
            }
        };

        // if measured_fan_power and measured_air_flow_rate are not present, we need to fetch and add the SFP
        if !mech_vent.contains_key("measured_fan_power")
            || !mech_vent.contains_key("measured_air_flow_rate")
        {
            mech_vent.insert("SFP".into(), test_datum.sfp.as_f64().into());
        }

        mech_vent.insert("mvhr_eff".into(), json!(test_datum.mvhr_eff.as_f64()));

        let duct_type = &test_datum.duct_type;
        let installed_under_approved_scheme = mech_vent.get("installed_under_approved_scheme").and_then(JsonValue::as_bool).ok_or_else(|| { ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck("Centralised MVHR input was expected to have an 'installed_under_approved_scheme' field that is a boolean")})?;
        let sfp_in_use_factor = resolve_sfp_in_use_factor(
            in_use_factors_access,
            &MechanicalVentilationSystemType::CentralisedMvAndMvhr,
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
    use crate::products::Product;
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

    #[tokio::test]
    #[rstest]
    #[case::two_wet_rooms("centralisedMvhr", 2)]
    #[case::three_wet_rooms("centralisedMvhr3WetRooms", 3)]
    #[case::seven_wet_rooms("centralisedMvhr7WetRooms", 7)]
    async fn test_transform_centralised_mvhr(
        pcdb_products: HashMap<String, Product>,
        in_use_factor_access: impl InUseFactorsAccess,
        #[case] product_reference: &str,
        #[case] number_of_wet_rooms: usize,
    ) {
        let mut mvhr_input = centralised_mvhr_input(product_reference);
        let pcdb_mvhr = pcdb_products.get("centralisedMvhr").unwrap();

        let result = transform(
            mvhr_input.as_object_mut().unwrap(),
            pcdb_mvhr,
            product_reference,
            number_of_wet_rooms,
            &in_use_factor_access,
        )
        .await;
        assert!(result.is_ok());

        let expected_input = expected_transformed_mech_vent_input(product_reference);
        transformed_input_matches_expected(&mvhr_input, expected_input);
    }

    #[tokio::test]
    #[rstest]
    async fn test_transform_centralised_mvhr_with_measured_fields_supplied(
        pcdb_products: HashMap<String, Product>,
        in_use_factor_access: impl InUseFactorsAccess,
    ) {
        let product_reference = "centralisedMvhrWithMeasuredFanPowerAndAirFlowRate";
        let mut mvhr_input_value = centralised_mvhr_input(product_reference);
        let mvhr_input = mvhr_input_value.as_object_mut().unwrap();

        // add measured_* fields
        mvhr_input.insert("measured_fan_power".into(), json!(2.0));
        mvhr_input.insert("measured_air_flow_rate".into(), json!(3.0));

        let pcdb_mvhr = pcdb_products.get("centralisedMvhr").unwrap();

        let result = transform(
            mvhr_input,
            pcdb_mvhr,
            product_reference,
            4,
            &in_use_factor_access,
        )
        .await;
        assert!(result.is_ok());

        let expected_input = expected_transformed_mech_vent_input(product_reference);
        transformed_input_matches_expected(&mvhr_input_value, expected_input);
    }

    #[tokio::test]
    #[rstest]
    async fn test_transform_centralised_mvhr_errors_given_unsupported_number_of_wet_rooms(
        pcdb_products: HashMap<String, Product>,
        in_use_factor_access: impl InUseFactorsAccess,
    ) {
        let product_reference = "centralisedMvhr";
        let mut mvhr_input = centralised_mvhr_input(product_reference);
        let pcdb_mvhr = pcdb_products.get(product_reference).unwrap();

        let result = transform(
            mvhr_input.as_object_mut().unwrap(),
            pcdb_mvhr,
            product_reference,
            9,
            &in_use_factor_access,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[rstest]
    async fn test_transform_decentralised_mvhr_errors_given_ambiguous_configuration_from_pcdb(
        pcdb_products: HashMap<String, Product>,
        in_use_factor_access: impl InUseFactorsAccess,
    ) {
        let product_reference = "centralisedMvhrWithTwoEntriesForTheSameConfiguration";
        let mut mvhr_input = centralised_mvhr_input(product_reference);
        let pcdb_mvhr = pcdb_products.get(product_reference).unwrap();

        let result = transform(
            mvhr_input.as_object_mut().unwrap(),
            pcdb_mvhr,
            product_reference,
            1,
            &in_use_factor_access,
        )
        .await;
        assert!(result.is_err());
    }
}
