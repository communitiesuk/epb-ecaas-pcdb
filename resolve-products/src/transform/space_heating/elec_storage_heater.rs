use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, Technology};
use crate::transform::ResolveProductsResult;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Map, Value as JsonValue};
use std::vec;

pub fn transform(
    elec_storage_heater: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
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
        elec_storage_heater.insert("EnergySupply".into(), fuel.to_string().into());
        elec_storage_heater.insert("fan_pwr".into(), fan_pwr.to_f64().into());

        let mut dry_core_min_output: Vec<[f64; 2]> = Vec::new();
        let mut dry_core_max_output: Vec<[f64; 2]> = Vec::new();

        for datum in test_data {
            dry_core_min_output.push([
                datum.test_point.as_f64(),
                datum.dry_core_min_output.as_f64(),
            ]);
            dry_core_max_output.push([
                datum.test_point.as_f64(),
                datum.dry_core_max_output.as_f64(),
            ]);
        }
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
    use itertools::Itertools;
    use serde_json::json;

    #[test]
    fn test_transform_esh() {
        let product_reference = "444";
        let mut input = json!({
            "type": "ElecStorageHeater",
            "n_units": 1,
            "Zone": "ThermalZone",
            "product_reference": product_reference,
        });
        let pcdb_esh =
            serde_json::from_str(include_str!("../../../test/test_esh_pcdb.json")).unwrap();
        let expected: JsonValue = serde_json::from_str(include_str!(
            "../../../test/test_esh_input_transformed.json"
        ))
        .unwrap();

        let result = transform(input.as_object_mut().unwrap(), &pcdb_esh, product_reference);

        assert!(result.is_ok());

        let mut actual_keys = input.as_object().unwrap().keys().collect_vec();
        actual_keys.sort();
        let mut expected_keys = expected.as_object().unwrap().keys().collect_vec();
        expected_keys.sort();

        assert_eq!(actual_keys, expected_keys);

        for key in expected_keys {
            assert_eq!(input[key], expected[key], "{:?}", key);
        }
    }
}
