use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, Technology};
use crate::transform::{EnergySupplies, ResolveProductsResult};
use crate::PRODUCT_REFERENCE_FIELD;
use serde_json::{json, Map, Value as JsonValue};

pub(crate) fn transform(
    pcm_battery: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
    energy_supplies: &EnergySupplies,
) -> ResolveProductsResult<()> {
    let mut category_mismatches = vec![];

    if let Technology::HeatBatteryPcm {
        a,
        b,
        fuel,
        electricity_circ_pump,
        electricity_standby,
        flow_rate_l_per_min,
        heat_storage_kj_per_k_above_phase_transition,
        heat_storage_kj_per_k_below_phase_transition,
        heat_storage_kj_per_k_during_phase_transition,
        inlet_diameter_mm,
        max_rated_losses,
        max_temperature,
        phase_transition_temperature_lower,
        phase_transition_temperature_upper,
        rated_charge_power,
        simultaneous_charging_and_discharging,
        velocity_in_hex_tube_at_1_l_per_min_m_per_s,
        ..
    } = &product.technology
    {
        let energy_supply = if let Some(fuel) = fuel {
            energy_supplies
                .get(fuel)
                .ok_or_else(|| ResolvePcdbProductsError::from(fuel))?
        } else {
            // if fuel is not set on battery from PCDB, then documentation says that this is to be
            // the same as the main heat generator, which we have assumed to always be "mains elec"                                                                                                                           |
            "mains elec"
        };
        pcm_battery.insert("EnergySupply".into(), energy_supply.into());
        pcm_battery.insert("A".into(), a.as_f64().into());
        pcm_battery.insert("B".into(), b.as_f64().into());
        pcm_battery.insert(
            "electricity_circ_pump".into(),
            electricity_circ_pump.as_f64().into(),
        );
        pcm_battery.insert(
            "electricity_standby".into(),
            electricity_standby.as_f64().into(),
        );
        pcm_battery.insert(
            "flow_rate_l_per_min".into(),
            flow_rate_l_per_min.as_f64().into(),
        );
        pcm_battery.insert(
            "heat_storage_kJ_per_K_above_Phase_transition".into(),
            heat_storage_kj_per_k_above_phase_transition.as_f64().into(),
        );
        pcm_battery.insert(
            "heat_storage_kJ_per_K_below_Phase_transition".into(),
            heat_storage_kj_per_k_below_phase_transition.as_f64().into(),
        );
        pcm_battery.insert(
            "heat_storage_kJ_per_K_during_Phase_transition".into(),
            heat_storage_kj_per_k_during_phase_transition
                .as_f64()
                .into(),
        );
        pcm_battery.insert(
            "inlet_diameter_mm".into(),
            inlet_diameter_mm.as_f64().into(),
        );
        pcm_battery.insert("max_rated_losses".into(), max_rated_losses.as_f64().into());
        pcm_battery.insert("max_temperature".into(), max_temperature.as_f64().into());
        pcm_battery.insert(
            "phase_transition_temperature_lower".into(),
            phase_transition_temperature_lower.as_f64().into(),
        );
        pcm_battery.insert(
            "phase_transition_temperature_upper".into(),
            phase_transition_temperature_upper.as_f64().into(),
        );
        pcm_battery.insert(
            "rated_charge_power".into(),
            rated_charge_power.as_f64().into(),
        );
        pcm_battery.insert(
            "simultaneous_charging_and_discharging".into(),
            json!(simultaneous_charging_and_discharging),
        );
        pcm_battery.insert(
            "velocity_in_HEX_tube_at_1_l_per_min_m_per_s".into(),
            velocity_in_hex_tube_at_1_l_per_min_m_per_s.as_f64().into(),
        );
        pcm_battery.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        category_mismatches.push(format!(
            "Product reference '{product_reference}' does not relate to a heat battery."
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
    use crate::products::Product;
    use crate::transform::catalogue::{mock_energy_supplies, transformed_input_matches_expected};
    use crate::transform::heat_source_wet::heat_battery_pcm::transform;
    use crate::transform::EnergySupplies;
    use rstest::{fixture, rstest};
    use serde_json::{json, Map, Value as JsonValue};
    use std::collections::HashMap;

    fn pcm_heat_battery_input(product_reference: &str) -> JsonValue {
        json!({
                "type": "HeatBattery",
                "battery_type": "pcm",
                "product_reference": product_reference,
                "number_of_units": 2,
                "is_heat_network": false
        })
    }

    #[fixture]
    fn pcdb_pcm_heat_batteries() -> HashMap<String, Product> {
        serde_json::from_str(include_str!(
            "../../../test/test_pcm_heat_batteries_pcdb.json"
        ))
        .unwrap()
    }

    #[fixture]
    fn energy_supplies() -> EnergySupplies {
        mock_energy_supplies()
    }

    fn expected_transformed_input(product_reference: &str) -> Map<String, JsonValue> {
        let expected_input: JsonValue = serde_json::from_str(include_str!(
            "../../../test/test_pcm_heat_battery_input_transformed.json"
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
    #[case::pcm_with_pcdb_fuel("pcm")]
    #[case::pcm_without_pcdb_fuel("pcm_without_pcdb_fuel")]
    fn test_transform_heat_battery_pcm(
        pcdb_pcm_heat_batteries: HashMap<String, Product>,
        energy_supplies: EnergySupplies,
        #[case] product_reference: &str,
    ) {
        let mut pcm_input = pcm_heat_battery_input(product_reference);
        let pcdb_pcm_heat_battery = pcdb_pcm_heat_batteries.get(product_reference).unwrap();

        let result = transform(
            pcm_input.as_object_mut().unwrap(),
            pcdb_pcm_heat_battery,
            product_reference,
            &energy_supplies,
        );
        assert!(result.is_ok());
        let expected_input = expected_transformed_input(product_reference);
        transformed_input_matches_expected(&pcm_input, expected_input);
    }
}
