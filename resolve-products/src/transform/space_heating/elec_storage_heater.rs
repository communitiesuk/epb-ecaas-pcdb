use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, Technology};
use crate::transform::{EnergySupplies, ResolveProductsResult};
use crate::PRODUCT_REFERENCE_FIELD;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{json, Map, Value as JsonValue};
use std::vec;

pub fn transform(
    elec_storage_heater: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
    energy_supplies: &EnergySupplies,
) -> ResolveProductsResult<()> {
    let mut category_mismatches = vec![];

    if let Technology::ElectricStorageHeater {
        pwr_in,
        rated_power_instant,
        storage_capacity,
        air_flow_type,
        frac_convective,
        fuel,
        fan_pwr,
        test_data,
        ..
    } = &product.technology
    {
        elec_storage_heater.insert("pwr_in".into(), pwr_in.to_f64().into());
        elec_storage_heater.insert(
            "rated_power_instant".into(),
            rated_power_instant.to_f64().into(),
        );
        elec_storage_heater.insert("storage_capacity".into(), storage_capacity.to_f64().into());
        elec_storage_heater.insert("air_flow_type".into(), air_flow_type.to_string().into());
        elec_storage_heater.insert("frac_convective".into(), frac_convective.to_f64().into());

        let energy_supply = energy_supplies
            .get(fuel)
            .ok_or_else(|| ResolvePcdbProductsError::from(fuel))?;
        elec_storage_heater.insert("EnergySupply".into(), json!(energy_supply.as_ref()));

        elec_storage_heater.insert("fan_pwr".into(), fan_pwr.to_f64().into());

        let (dry_core_min_output, dry_core_max_output): (Vec<[f64; 2]>, Vec<[f64; 2]>) = test_data
            .iter()
            .map(|datum| {
                let test_point = datum.test_point.as_f64();

                (
                    [test_point, datum.dry_core_min_output.as_f64()],
                    [test_point, datum.dry_core_max_output.as_f64()],
                )
            })
            .unzip();

        elec_storage_heater.insert("dry_core_min_output".into(), dry_core_min_output.into());
        elec_storage_heater.insert("dry_core_max_output".into(), dry_core_max_output.into());

        // now remove product reference
        elec_storage_heater.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        category_mismatches.push(format!(
            "Product reference '{product_reference}' does not relate to an electric storage heater."
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
    use crate::transform::catalogue::{mock_energy_supplies, transformed_input_matches_expected};
    use crate::transform::space_heating::tests::SPACE_HEATING_PCDB_PRODUCTS;
    use rstest::*;
    use serde_json::{from_str, json, Value};
    use smartstring::alias::String;
    use std::collections::HashMap;

    #[fixture]
    fn energy_supplies() -> EnergySupplies {
        mock_energy_supplies()
    }

    fn input(product_reference: &str) -> JsonValue {
        json!({
            "type": "ElecStorageHeater",
            "n_units": 1,
            "Zone": "ThermalZone",
            "product_reference": product_reference,
        })
    }

    #[rstest]
    fn test_transform_esh(energy_supplies: EnergySupplies) {
        let product_reference = "444";
        let mut input = input(product_reference);
        let expected: Map<std::string::String, Value> =
            from_str(include_str!("../../../test/esh_transformed.json")).unwrap();

        let result = transform(
            input.as_object_mut().unwrap(),
            SPACE_HEATING_PCDB_PRODUCTS.get(product_reference).unwrap(),
            product_reference,
            &energy_supplies,
        );

        assert!(result.is_ok());
        transformed_input_matches_expected(&input, expected);
    }

    #[rstest]
    fn test_transform_esh_errors_when_product_type_mismatch(energy_supplies: EnergySupplies) {
        let product_reference = "hp";
        let mut input = input(product_reference);
        let pcdb_hps: HashMap<String, Product> =
            from_str(include_str!("../../../test/test_heat_pump_pcdb.json")).unwrap();

        let result = transform(
            input.as_object_mut().unwrap(),
            pcdb_hps.get(product_reference).unwrap(),
            product_reference,
            &energy_supplies,
        );

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Product reference 'hp' does not relate to an electric storage heater.")
        );
    }
}
