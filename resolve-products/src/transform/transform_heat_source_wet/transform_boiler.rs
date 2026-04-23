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
