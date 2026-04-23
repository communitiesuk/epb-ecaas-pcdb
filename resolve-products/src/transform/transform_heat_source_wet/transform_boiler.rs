use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::{BoilerLocation, Product, Technology};
use crate::transform::ResolveProductsResult;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value as JsonValue};

pub fn transform_boiler(
    boiler: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
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
        boiler.insert("EnergySupply".into(), fuel.to_string().into());
        boiler.insert("EnergySupply_aux".into(), fuel_aux.to_string().into());
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
    use std::collections::HashMap;
    use super::*;
    use smartstring::alias::String as SmartString;
    use rstest::{fixture, rstest};
    use serde_json::{Value, json};

    #[fixture]
    fn pcdb_boilers() -> HashMap<SmartString, Product> {
        serde_json::from_str(include_str!("../../../test/test_boilers_pcdb.json")).unwrap()
    }

    fn input_with_boiler_reference(product_reference: &str, specified_location: Option<&str>) -> Map<String,Value> {
        let mut boiler_input = json!({
                "type": "Boiler",
                "EnergySupply": "mains gas",
                "product_reference": product_reference,
                "is_heat_network": false // TODO: add one with heat_network
        });
        if let Some(location) = specified_location {
            boiler_input["specified_location"] = json!(location);
        }
        boiler_input.as_object().unwrap().clone()
    }

    #[fixture]
    fn expected_boilers() -> JsonValue {
        serde_json::from_str(include_str!(
            "../../../test/test_boiler_input_transformed.json"
        ))
            .unwrap()
    }

    #[rstest]
    fn test_transform_boiler_with_specified_location(pcdb_boilers: HashMap<SmartString, Product>) {
        let product_reference = "boiler_unknown_location";
        let mut input_with_boiler_ref = input_with_boiler_reference(product_reference, Some("internal"));
        let pcdb_boiler = pcdb_boilers.get(&SmartString::from(product_reference)).unwrap();
        let result = transform_boiler(&mut input_with_boiler_ref, &pcdb_boiler, product_reference);
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_transform_boiler_with_neither_pcdb_nor_specified_location(pcdb_boilers: HashMap<SmartString, Product>) {
        let product_reference = "boiler_unknown_location";
        let mut input_with_boiler_ref = input_with_boiler_reference(product_reference, None);
        let pcdb_boiler = pcdb_boilers.get(&SmartString::from(product_reference)).unwrap();

        let result = transform_boiler(&mut input_with_boiler_ref, &pcdb_boiler, product_reference);
        assert!(result.is_err());
    }
}
