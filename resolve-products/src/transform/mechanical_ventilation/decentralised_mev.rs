use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::in_use_factors::InUseFactorsAccess;
use crate::products::{
    DecentralisedMevInstallationConfiguration, DecentralisedMevTestDatum,
    MechanicalVentilationDuctType, Product, Technology,
};
use crate::transform::mechanical_ventilation::{
    MechanicalVentilationSystemType, resolve_sfp_in_use_factor,
};
use crate::transform::{InvalidProductCategoryError, ResolveProductsResult};
use serde::Deserialize;
use serde_json::{Map, Value as JsonValue, json};

pub(crate) async fn transform(
    mech_vent: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
    in_use_factors_access: &impl InUseFactorsAccess,
) -> ResolveProductsResult<()> {
    if let Technology::DecentralisedMev { test_data, .. } = &product.technology {
        let installation_type = mech_vent
            .get("installation_type")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
                    "Decentralised Mev was expected to have an installation_type",
                )
            })?;

        let installation_location = mech_vent
            .get("installation_location")
            .cloned()
            .map(serde_json::from_value::<InstallationLocation>)
            .transpose()
            .ok()
            .flatten()
            .ok_or_else(|| {
                ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
                    "Decentralised Mev was expected to have an installation_location",
                )
            })?;

        let expected_configuration = match installation_type {
            "in_ceiling" => Ok(DecentralisedMevInstallationConfiguration::InCeiling),
            "in_duct" => Ok(DecentralisedMevInstallationConfiguration::InDuct),
            "through_wall" => Ok(DecentralisedMevInstallationConfiguration::ThroughWall),
            _ => Err(
                ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
                    "Decentralised Mev field 'installation_type' was expected to be a known value",
                ),
            ),
        }?;

        let test_datum: &DecentralisedMevTestDatum = test_data
            .iter()
            .find(|a|
                a.configuration == expected_configuration
            )
            .ok_or_else(|| ResolvePcdbProductsError::InvalidCombination(format!("Decentralised Mev product {} from PCDB is not compatible with specified installation configuration ({:?}, {:?})", product_reference, installation_type, installation_location)))?;

        {
            // use SFP for kitchen, or SFP2 for other
            let sfp = match installation_location {
                InstallationLocation::Kitchen => test_datum.sfp,
                InstallationLocation::OtherWetRoom => test_datum.sfp2,
            };

            mech_vent.insert("SFP".into(), json!(sfp.as_f64()));
        }

        {
            // assuming a rigid duct type for all Decentralised MeVs
            let duct_type = MechanicalVentilationDuctType::RigidDucting;
            let installed_under_approved_scheme = mech_vent.get("installed_under_approved_scheme").and_then(JsonValue::as_bool).ok_or_else(|| { ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck("Decentralised MeV input was expected to have a installed_under_approved_scheme field that is a boolean")})?;
            let sfp_in_use_factor = resolve_sfp_in_use_factor(
                in_use_factors_access,
                &MechanicalVentilationSystemType::DecentralisedMev,
                &duct_type,
                installed_under_approved_scheme,
            )
            .await?;
            mech_vent.insert(
                "SFP_in_use_factor".into(),
                json!(sfp_in_use_factor.as_f64()),
            );
        }

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

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum InstallationLocation {
    Kitchen,
    OtherWetRoom,
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

    fn decentralised_mev_input(
        product_reference: &str,
        installation_type: &str,
        installation_location: &str,
    ) -> Value {
        json!({
            "vent_type": "Decentralised continuous MEV",
            "EnergySupply": "mains elec",
            "product_reference": product_reference,
            "design_outdoor_air_flow_rate": 20.0,
            "installed_under_approved_scheme": true,
            "installation_type": installation_type,
            "installation_location": installation_location,
            "mid_height_air_flow_path": 2,
            "orientation360": 0,
            "pitch": 90
        })
    }

    #[tokio::test]
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
    async fn test_transform_decentralised_mev(
        pcdb_products: HashMap<String, Product>,
        in_use_factor_access: impl InUseFactorsAccess,
        #[case] product_reference: &str,
        #[case] installation_type: &str,
        #[case] installation_location: &str,
    ) {
        let mut mev_input =
            decentralised_mev_input(product_reference, installation_type, installation_location);
        let pcdb_mev = pcdb_products
            .get("decentralisedMevWithAllConfigurations")
            .unwrap();

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
            &in_use_factor_access,
        )
        .await;
        assert!(result.is_ok());

        let expected_input = expected_transformed_mech_vent_input(product_reference);
        transformed_input_matches_expected(&mev_input, expected_input);
    }

    #[tokio::test]
    #[rstest]
    async fn test_transform_decentralised_mev_missing_configuration_error(
        pcdb_products: HashMap<String, Product>,
        in_use_factor_access: impl InUseFactorsAccess,
    ) {
        let product_reference = "decentralisedMev";
        let mut mev_input = decentralised_mev_input(product_reference, "in_duct", "other_wet_room");
        let pcdb_mev = pcdb_products.get(product_reference).unwrap();

        let result = transform(
            mev_input.as_object_mut().unwrap(),
            pcdb_mev,
            product_reference,
            &in_use_factor_access,
        )
        .await;
        assert!(result.is_err());
    }
}
