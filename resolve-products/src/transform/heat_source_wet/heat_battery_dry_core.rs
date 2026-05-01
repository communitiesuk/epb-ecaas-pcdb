use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, Technology};
use crate::transform::{EnergySupplies, InvalidProductCategoryError, ResolveProductsResult};
use itertools::Itertools;
use serde_json::{Map, Value as JsonValue, json};

pub(crate) fn transform(
    dry_core_battery: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
    energy_supplies: &EnergySupplies,
) -> ResolveProductsResult<()> {
    if let Technology::HeatBatteryDryCore {
        fuel,
        electricity_circ_pump,
        electricity_standby,
        pwr_in,
        rated_power_instant,
        heat_storage_capacity,
        fan_pwr,
        test_data,
        ..
    } = &product.technology
    {
        let energy_supply = energy_supplies
            .get(fuel)
            .ok_or_else(|| ResolvePcdbProductsError::from(fuel))?;

        dry_core_battery.insert("EnergySupply".into(), json!(energy_supply.as_ref()));
        dry_core_battery.insert(
            "electricity_circ_pump".into(),
            electricity_circ_pump.as_f64().into(),
        );
        dry_core_battery.insert(
            "electricity_standby".into(),
            electricity_standby.as_f64().into(),
        );
        dry_core_battery.insert("pwr_in".into(), pwr_in.as_f64().into());
        dry_core_battery.insert(
            "rated_power_instant".into(),
            rated_power_instant.as_f64().into(),
        );
        dry_core_battery.insert(
            "heat_storage_capacity".into(),
            heat_storage_capacity.as_f64().into(),
        );
        dry_core_battery.insert("fan_pwr".into(), fan_pwr.as_f64().into());

        let (dry_core_min_output, dry_core_max_output): (Vec<[f64; 2]>, Vec<[f64; 2]>) = test_data
            .iter()
            .sorted_by(|a, b| Ord::cmp(&a.charge_level, &b.charge_level))
            .map(|datum| {
                let charge_level = datum.charge_level.as_f64();

                (
                    [charge_level, datum.dry_core_min_output.as_f64()],
                    [charge_level, datum.dry_core_max_output.as_f64()],
                )
            })
            .unzip();

        let state_of_charge_init = test_data
            .iter()
            .map(|datum| datum.charge_level)
            .min()
            .ok_or_else(|| {
                ResolvePcdbProductsError::InvalidProduct(
                    product_reference.into(),
                    "fancoil test data was unexpectedly empty",
                )
            })?;
        dry_core_battery.insert("dry_core_min_output".into(), dry_core_min_output.into());
        dry_core_battery.insert("dry_core_max_output".into(), dry_core_max_output.into());
        dry_core_battery.insert(
            "state_of_charge_init".into(),
            state_of_charge_init.as_f64().into(),
        );

        dry_core_battery.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        return Err(InvalidProductCategoryError::from((
            product_reference,
            "dry core heat battery",
        ))
        .into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::products::Product;
    use crate::transform::EnergySupplies;
    use crate::transform::catalogue::{mock_energy_supplies, transformed_input_matches_expected};
    use crate::transform::heat_source_wet::heat_battery_dry_core::transform;
    use rstest::{fixture, rstest};
    use serde_json::{Map, Value as JsonValue, json};
    use std::collections::HashMap;

    fn dry_core_heat_battery_input(product_reference: &str) -> JsonValue {
        json!({
                "type": "HeatBattery",
                "battery_type": "dry_core",
                "product_reference": product_reference,
                "number_of_units": 2,
                "is_heat_network": false
        })
    }

    #[fixture]
    fn pcdb_heat_batteries() -> HashMap<String, Product> {
        serde_json::from_str(include_str!("../../../test/test_heat_batteries_pcdb.json")).unwrap()
    }

    #[fixture]
    fn energy_supplies() -> EnergySupplies {
        mock_energy_supplies()
    }

    fn expected_transformed_input(product_reference: &str) -> Map<String, JsonValue> {
        let expected_input: JsonValue = serde_json::from_str(include_str!(
            "../../../test/test_heat_battery_input_transformed.json"
        ))
        .unwrap();

        expected_input
            .pointer(&format!("/HeatSourceWet/{}", product_reference))
            .unwrap()
            .as_object()
            .unwrap()
            .clone()
    }

    #[rstest]
    #[case("dry_core")]
    #[case("dry_core_unordered_test_data")]
    fn test_transform_heat_battery_dry_core(
        pcdb_heat_batteries: HashMap<String, Product>,
        energy_supplies: EnergySupplies,
        #[case] product_reference: &str,
    ) {
        let mut dry_core_input = dry_core_heat_battery_input(product_reference);
        let pcdb_dry_core_heat_battery = pcdb_heat_batteries.get(product_reference).unwrap();

        let result = transform(
            dry_core_input.as_object_mut().unwrap(),
            pcdb_dry_core_heat_battery,
            product_reference,
            &energy_supplies,
        );
        assert!(result.is_ok());
        let expected_input = expected_transformed_input(product_reference);
        transformed_input_matches_expected(&dry_core_input, expected_input);
    }
}
