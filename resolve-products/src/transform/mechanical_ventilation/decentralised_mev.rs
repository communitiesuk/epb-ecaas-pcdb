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

        // PCDB configuration
        // It defines the configuration which the other test data relates to.
        // 1 In-room fan, kitchen
        // 2 In-room fan, other wet room
        // 3 In-duct fan, kitchen
        // 4 In-duct fan, other wet room
        // 5 Through-wall fan, kitchen
        // 6 Through-wall fan, other wet room

        match (installation_type, installation_location) {
            (Some("in_ceiling"), Some("kitchen")) => {
                let conf: &DecentralisedMevTestDatum = test_data
                    .iter()
                    .find(|a|
                        { matches!(a.configuration, DecentralisedMevInstallationConfiguration::InRoomFanKitchen) }
                    )
                    .ok_or_else(|| ResolvePcdbProductsError::InvalidCombination(format!("Decentralised Mev installation configuration for InRoomFanKitchen is missing from PCDB product {}", product_reference)))?;

                let DecentralisedMevTestDatum { sfp, flow_rate, .. } = conf;
                mech_vent.insert("SFP".into(), json!(sfp.as_f64())); // TODO: review, account for other PCDB fields: SFP2 (required) and SFP3 (optional)
                mech_vent.insert(
                    "design_outdoor_air_flow_rate".into(),
                    json!(flow_rate.as_f64()),
                ); // TODO: review, account for other PCDB fields: flowRate2 (required) and flowRate3 (optional)
            }
            (Some("in_ceiling"), Some("other_wet_room")) => {
                todo!()
            }
            (Some("in_duct"), Some("kitchen")) => {
                todo!()
            }
            (Some("in_duct"), Some("other_wet_room")) => {
                todo!()
            }
            (Some("through_wall"), Some("kitchen")) => {
                todo!()
            }
            (Some("through_wall"), Some("other_wet_room")) => {
                todo!()
            }
            (_, _) => {}
        };
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
    // #[case::InCeilingKitchen("decentralisedMev", "in_ceiling", "other_wet_room")]
    // #[case::InCeilingKitchen("decentralisedMev", "in_duct", "kitchen")]
    // #[case::InCeilingKitchen("decentralisedMev", "in_duct", "other_wet_room")]
    // #[case::InCeilingKitchen("decentralisedMev", "through_wall", "kitchen")]
    // #[case::InCeilingKitchen("decentralisedMev", "through_wall", "other_wet_room")]
    fn test_transform_decentralised_mev(
        mechanical_ventilation_pcdb_products: HashMap<String, Product>,
        #[case] product_reference: &str,
        #[case] installation_type: &str,
        #[case] installation_location: &str,
    ) {
        let mut mev_input =
            decentralised_mev_input(product_reference, installation_type, installation_location);
        let pcdb_mev = mechanical_ventilation_pcdb_products
            .get(product_reference)
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
}
