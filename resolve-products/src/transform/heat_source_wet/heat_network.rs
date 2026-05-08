use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, SubHeatNetwork, Technology};
use crate::transform::{InvalidProductCategoryError, ResolveProductsResult};
use serde_json::{Map, Value, json};

pub fn transform(
    heat_source_wet: &mut Map<String, serde_json::Value>,
    product: &Product,
    product_reference: &str,
    is_heat_pump_present: bool,
) -> ResolveProductsResult<()> {
    if let Technology::HeatNetwork {
        sub_heat_networks,
        community_heat_network_name,
        has_booster_heat_pump,
        ..
    } = &product.technology
    {
        // if heat network needs a booster heat pump, check there is at least one heat pump
        if *has_booster_heat_pump && !is_heat_pump_present {
            return Err(ResolvePcdbProductsError::BoosterHeatPumpNotPresentError);
        }

        let sub_heat_network_name = heat_source_wet
            .get("sub_heat_network_name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
                    "sub_heat_network_name was expected as a string",
                )
            })?;

        let SubHeatNetwork {
            emissions_factor,
            emissions_factor_including_out_of_scope,
            primary_energy_factor,
            ..
        } = sub_heat_networks
            .iter()
            .find(|sub_network| sub_network.name == sub_heat_network_name)
            .ok_or_else(|| {
                ResolvePcdbProductsError::SubHeatNetworkNotFoundError(
                    sub_heat_network_name.into(),
                    product_reference.into(),
                )
            })?;

        heat_source_wet.insert(
            "EnergySupply".into(),
            json!({
                "name": format!("{} - {}", community_heat_network_name, sub_heat_network_name),
                "is_export_capable": json!(false),
                "factor": {
                    "Emissions Factor kgCO2e/kWh": emissions_factor.as_f64(),
                    "Emissions Factor kgCO2e/kWh including out-of-scope emissions": emissions_factor_including_out_of_scope.as_f64(),
                    "Primary Energy Factor kWh/kWh delivered": primary_energy_factor.as_f64(),
                }
            }),
        );
    } else {
        return Err(InvalidProductCategoryError::from((product_reference, "heat network")).into());
    }

    heat_source_wet.remove("heat_network_reference");
    heat_source_wet.remove("sub_heat_network_name");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::catalogue::transformed_input_matches_expected;
    use rstest::*;
    use serde_json::{Value, json};
    use std::collections::HashMap;

    fn heat_network_reference_input(
        heat_network_reference: &str,
        sub_heat_network_name: &str,
    ) -> Value {
        json!({
            "heat_network_reference": heat_network_reference,
            "sub_heat_network_name": sub_heat_network_name
        })
    }

    #[fixture]
    fn pcdb_heat_networks() -> HashMap<String, Product> {
        serde_json::from_str(include_str!("../../../test/heat_network_pcdb.json")).unwrap()
    }

    fn expected_heat_network_transformed(product_reference: &str) -> Map<String, Value> {
        let expected_heat_networks: Value =
            serde_json::from_str(include_str!("../../../test/heat_network_transformed.json"))
                .unwrap();
        expected_heat_networks
            .get(product_reference)
            .unwrap()
            .as_object()
            .unwrap()
            .clone()
    }

    #[rstest]
    #[case("Thomas's Shed", "heatNetworkThomas")]
    #[case("Chez the Fat Controller", "heatNetworkFatController")]
    fn test_transform_heat_network(
        #[case] sub_heat_network_name: &str,
        #[case] key_for_expected: &str,
        pcdb_heat_networks: HashMap<String, Product>,
    ) {
        let heat_network_reference = "heatNetwork";
        let mut input = heat_network_reference_input(heat_network_reference, sub_heat_network_name);
        let pcdb_product = pcdb_heat_networks.get(heat_network_reference).unwrap();

        let result = transform(
            input.as_object_mut().unwrap(),
            pcdb_product,
            heat_network_reference,
            false,
        );
        match result {
            Ok(_) => {
                let expected_transformed = expected_heat_network_transformed(key_for_expected);
                transformed_input_matches_expected(&input, expected_transformed);
            }
            Err(e) => panic!("Transformation failed with error: {}", e),
        }
    }

    #[rstest]
    fn test_transform_heat_network_errors_for_5th_gen_with_no_heat_pump(
        pcdb_heat_networks: HashMap<String, Product>,
    ) {
        let heat_network_reference = "heatNetworkBoosterHeatPump";
        let mut input = heat_network_reference_input(heat_network_reference, "Thomas's Shed");
        let pcdb_product = pcdb_heat_networks.get(heat_network_reference).unwrap();

        let result = transform(
            input.as_object_mut().unwrap(),
            pcdb_product,
            heat_network_reference,
            false,
        );
        assert!(
            result.is_err(),
            "Expected error for 5th gen heat network with no heat pump"
        );
    }
}
