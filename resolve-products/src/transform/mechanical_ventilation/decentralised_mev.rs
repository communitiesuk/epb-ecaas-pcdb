use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::{
    DecentralisedMevInstallationConfiguration, DecentralisedMevTestDatum, Product, Technology,
};
use crate::transform::{InvalidProductCategoryError, ResolveProductsResult};
use serde_json::{Map, Value as JsonValue, json};

pub(crate) fn transform(
    mech_vent: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
) -> ResolveProductsResult<()> {
    if let Technology::DecentralisedMev { test_data, .. } = &product.technology {
        let installation_type = mech_vent
            .get("installation_type")
            .ok_or_else(|| {
                ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
                    "Decentralised Mev was expected to have an installation_type",
                )
            })?
            .as_str();

        let installation_location = mech_vent
            .get("installation_location")
            .ok_or_else(|| {
                ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
                    "Decentralised Mev was expected to have an installation_location",
                )
            })?
            .as_str();

        let expected_configuration = match (installation_type, installation_location) {
            (Some("in_ceiling"), Some("kitchen")) => {
                Ok(DecentralisedMevInstallationConfiguration::InRoomFanKitchen)
            }
            (Some("in_ceiling"), Some("other_wet_room")) => {
                Ok(DecentralisedMevInstallationConfiguration::InRoomFanOtherWetRoom)
            }
            (Some("in_duct"), Some("kitchen")) => {
                Ok(DecentralisedMevInstallationConfiguration::InDuctFanKitchen)
            }
            (Some("in_duct"), Some("other_wet_room")) => {
                Ok(DecentralisedMevInstallationConfiguration::InDuctFanOtherWetRoom)
            }
            (Some("through_wall"), Some("kitchen")) => {
                Ok(DecentralisedMevInstallationConfiguration::ThroughWallFanKitchen)
            }
            (Some("through_wall"), Some("other_wet_room")) => {
                Ok(DecentralisedMevInstallationConfiguration::ThroughWallFanOtherWetRoom)
            }
            (_, _) => Err(
                ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
                    "Decentralised Mev fields 'installation_type' and 'installation_location' were expected to be strings",
                ),
            ),
        }?;

        let test_datum: &DecentralisedMevTestDatum = test_data
            .iter()
            .find(|a|
                a.configuration == expected_configuration
            )
            .ok_or_else(|| ResolvePcdbProductsError::InvalidCombination(format!("Decentralised Mev product {} from PCDB is not compatible with specified installation configuration ({:?}, {:?})", product_reference, installation_type, installation_location)))?;

        // TODO: review, account for other PCDB fields: SFP2 (required) and SFP3 (optional)
        // TODO: review, account for other PCDB fields: flowRate2 (required) and flowRate3 (optional)
        let DecentralisedMevTestDatum { sfp, flow_rate, .. } = test_datum;
        mech_vent.insert("SFP".into(), json!(sfp.as_f64()));
        mech_vent.insert(
            "design_outdoor_air_flow_rate".into(),
            json!(flow_rate.as_f64()),
        );

        mech_vent.remove("installation_type");
        mech_vent.remove("installation_location");
        mech_vent.remove("installed_under_approved_scheme"); // TODO: review
        mech_vent.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        return Err(
            InvalidProductCategoryError::from((product_reference, "decentralised mev")).into(),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::catalogue::transformed_input_matches_expected;
    use rstest::{fixture, rstest};
    use serde_json::{Value, json};
    use std::collections::HashMap;

    #[fixture]
    fn mechanical_ventilation_pcdb_products() -> HashMap<String, Product> {
        serde_json::from_str(include_str!(
            "../../../test/test_mechanical_ventilation_pcdb.json"
        ))
        .unwrap()
    }

    fn decentralised_mev_input(
        product_reference: &str,
        installation_type: &str,
        installation_location: &str,
    ) -> Value {
        json!({
            "vent_type": "Decentralised continuous MEV",
            "EnergySupply": "mains elec",
            "product_reference": product_reference,
            "installed_under_approved_scheme": true,
            "installation_type": installation_type,
            "installation_location": installation_location,
            "mid_height_air_flow_path": 2,
            "orientation360": 0,
            "pitch": 90
        })
    }

    fn expected_transformed_input(product_reference: &str) -> Map<String, JsonValue> {
        let expected_mechanical_ventilation: JsonValue = serde_json::from_str(include_str!(
            "../../../test/test_mechanical_ventilation_input_transformed.json"
        ))
        .unwrap();

        expected_mechanical_ventilation
            .pointer(&format!("/MechanicalVentilation/{}", product_reference))
            .unwrap()
            .as_object()
            .unwrap()
            .clone()
    }

    #[rstest]
    #[case::in_ceiling_kitchen("decentralisedMev", "in_ceiling", "kitchen")]
    #[case::in_ceiling_other("decentralisedMevInCeilingOther", "in_ceiling", "other_wet_room")]
    #[case::in_duct_kitchen("decentralisedMevInDuctKitchen", "in_duct", "kitchen")]
    #[case::in_duct_other("decentralisedMevInDuctOther", "in_duct", "other_wet_room")]
    #[case::through_wall_kitchen("decentralisedMevThroughWallKitchen", "through_wall", "kitchen")]
    #[case::through_wall_other(
        "decentralisedMevThroughWallOther",
        "through_wall",
        "other_wet_room"
    )]
    fn test_transform_decentralised_mev(
        mechanical_ventilation_pcdb_products: HashMap<String, Product>,
        #[case] product_reference: &str,
        #[case] installation_type: &str,
        #[case] installation_location: &str,
    ) {
        let mut mev_input =
            decentralised_mev_input(product_reference, installation_type, installation_location);
        let pcdb_mev = mechanical_ventilation_pcdb_products
            .get("decentralisedMevWithAllConfigurations")
            .unwrap();

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
        );
        assert!(result.is_ok());

        let expected_input = expected_transformed_input(product_reference);
        transformed_input_matches_expected(&mev_input, expected_input);
    }

    #[rstest]
    fn test_transform_decentralised_mev_missing_configuration_error(
        mechanical_ventilation_pcdb_products: HashMap<String, Product>,
    ) {
        let product_reference = "decentralisedMev";
        let mut mev_input =
            decentralised_mev_input(product_reference, "in_ceiling", "other_wet_room");
        let pcdb_mev = mechanical_ventilation_pcdb_products
            .get(product_reference)
            .unwrap();

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
        );
        assert!(result.is_err());
    }
}
