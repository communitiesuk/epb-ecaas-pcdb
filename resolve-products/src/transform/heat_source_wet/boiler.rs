use crate::errors::ResolvePcdbProductsError;
use crate::products::{BoilerLocation, Product, Technology};
use crate::transform::{EnergySupplies, ResolveProductsResult};
use crate::PRODUCT_REFERENCE_FIELD;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{json, Map, Value as JsonValue};

pub fn transform(
    boiler: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
    energy_supplies: &EnergySupplies,
) -> ResolveProductsResult<()> {
    let mut category_mismatches = vec![];

    if let Technology::Boiler {
        fuel,
        fuel_aux,
        rated_power,
        efficiency_full_load,
        efficiency_part_load,
        boiler_location,
        modulation_load,
        electricity_circ_pump,
        electricity_part_load,
        electricity_full_load,
        electricity_standby,
        ..
    } = &product.technology
    {
        let energy_supply = energy_supplies
            .get(fuel)
            .ok_or_else(|| ResolvePcdbProductsError::from(fuel))?;
        let energy_supply_aux = energy_supplies
            .get(fuel_aux)
            .ok_or_else(|| ResolvePcdbProductsError::from(fuel_aux))?;
        boiler.insert("EnergySupply".into(), json!(energy_supply.as_ref()));
        boiler.insert("EnergySupply_aux".into(), json!(energy_supply_aux.as_ref()));

        boiler.insert("rated_power".into(), rated_power.to_f64().into());
        boiler.insert(
            "efficiency_full_load".into(),
            efficiency_full_load.to_f64().into(),
        );
        boiler.insert(
            "efficiency_part_load".into(),
            efficiency_part_load.to_f64().into(),
        );
        boiler.insert("modulation_load".into(), modulation_load.to_f64().into());
        boiler.insert(
            "electricity_circ_pump".into(),
            electricity_circ_pump.to_f64().into(),
        );
        boiler.insert(
            "electricity_part_load".into(),
            electricity_part_load.to_f64().into(),
        );
        boiler.insert(
            "electricity_full_load".into(),
            electricity_full_load.to_f64().into(),
        );
        boiler.insert(
            "electricity_standby".into(),
            electricity_standby.to_f64().into(),
        );

        match boiler_location {
            BoilerLocation::Unknown => {
                let specified_location = boiler
                    .get("specified_location")
                    .ok_or(Err("Expected location for boiler to be specified as boiler location from PCDB is unknown"))
                    .map_err(|_: Result<(), &_>| ResolvePcdbProductsError::InvalidCombination)?;

                boiler.insert("boiler_location".into(), specified_location.as_str().into());
            }
            _ => {
                boiler.insert("boiler_location".into(), boiler_location.to_string().into());
            }
        }

        boiler.remove("specified_location");
        boiler.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        category_mismatches.push(format!(
            "Product reference '{product_reference}' does not relate to a boiler."
        ));
    }

    if !category_mismatches.is_empty() {
        return Err(ResolvePcdbProductsError::ProductCategoryMismatches(
            category_mismatches,
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::catalogue::mock_energy_supplies;
    use itertools::Itertools;
    use rstest::{fixture, rstest};
    use serde_json::{json, Value};
    use std::collections::HashMap;

    fn boiler_input(product_reference: &str, specified_location: Option<&str>) -> Value {
        let mut boiler_input = json!({
                "type": "Boiler",
                "EnergySupply": "mains gas",
                "product_reference": product_reference,
                "is_heat_network": false // TODO: add one with heat_network
        });
        if let Some(location) = specified_location {
            boiler_input["specified_location"] = json!(location);
        }
        boiler_input
    }

    #[fixture]
    fn pcdb_boilers() -> HashMap<String, Product> {
        serde_json::from_str(include_str!("../../../test/test_boilers_pcdb.json")).unwrap()
    }

    #[fixture]
    fn energy_supplies() -> EnergySupplies {
        mock_energy_supplies()
    }

    fn expected_boiler_input(product_reference: &str) -> Map<String, JsonValue> {
        let expected_boilers: JsonValue = serde_json::from_str(include_str!(
            "../../../test/test_boiler_input_transformed.json"
        ))
        .unwrap();

        expected_boilers
            .pointer(&format!("/HeatSourceWet/{}", product_reference))
            .unwrap()
            .as_object()
            .unwrap()
            .clone()
    }

    fn transformed_input_matches_expected(
        transformed_input: &mut Value,
        expected_input: Map<String, Value>,
    ) {
        let mut actual_keys = transformed_input.as_object().unwrap().keys().collect_vec();
        actual_keys.sort();

        let mut expected_keys = expected_input.keys().collect_vec();
        expected_keys.sort();

        assert_eq!(actual_keys, expected_keys);

        for key in expected_keys {
            assert_eq!(transformed_input[key], expected_input[key], "{:?}", key);
        }
    }

    #[rstest]
    #[case::boiler_with_pcdb_location("boiler", None)]
    #[case::boiler_with_specified_location("boiler_unknown_location", Some("internal"))]
    fn test_transform_boiler(
        pcdb_boilers: HashMap<String, Product>,
        #[case] product_reference: &str,
        #[case] specified_location: Option<&str>,
        energy_supplies: EnergySupplies,
    ) {
        let mut boiler_input = boiler_input(product_reference, specified_location);
        let pcdb_boiler = pcdb_boilers.get(product_reference).unwrap();

        let result = transform(
            &mut boiler_input.as_object_mut().unwrap(),
            pcdb_boiler,
            product_reference,
            &energy_supplies,
        );
        assert!(result.is_ok());

        let expected_input = expected_boiler_input(product_reference);
        transformed_input_matches_expected(&mut boiler_input, expected_input);
    }

    #[rstest]
    fn test_transform_boiler_errors_with_neither_pcdb_nor_specified_location(
        pcdb_boilers: HashMap<String, Product>,
        energy_supplies: EnergySupplies,
    ) {
        let product_reference = "boiler_unknown_location";
        let specified_location = None;

        let mut input = boiler_input(product_reference, specified_location);
        let pcdb_boiler = pcdb_boilers.get(product_reference).unwrap();

        let result = transform(
            &mut input.as_object_mut().unwrap(),
            pcdb_boiler,
            product_reference,
            &energy_supplies,
        );
        assert!(result.is_err());
    }
}
