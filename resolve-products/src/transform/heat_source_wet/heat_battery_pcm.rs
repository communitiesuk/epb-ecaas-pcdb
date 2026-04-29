use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, Technology};
use crate::transform::{EnergySupplies, ResolveProductsResult};
use serde_json::{Map, Value as JsonValue};

pub(crate) fn transform(
    pcm_battery: &mut Map<String, JsonValue>,
    product: &Product,
    product_reference: &str,
    _energy_supplies: &EnergySupplies,
) -> ResolveProductsResult<()> {
    let mut category_mismatches = vec![];

    if let Technology::HeatBatteryPcm { a, .. } = &product.technology {
        pcm_battery.insert("A".into(), a.as_f64().into());
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
    use crate::products::Product;
    use crate::transform::EnergySupplies;
    use crate::transform::catalogue::mock_energy_supplies;
    use crate::transform::heat_source_wet::heat_battery_pcm::transform;
    use rstest::{fixture, rstest};
    use serde_json::{Map, Value as JsonValue, json};
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
    fn test_transform_boiler(
        pcdb_pcm_heat_batteries: HashMap<String, Product>,
        energy_supplies: EnergySupplies,
    ) {
        let product_reference = "pcm";
        let mut pcm_input = pcm_heat_battery_input(product_reference);
        let pcdb_pcm_heat_battery = pcdb_pcm_heat_batteries.get(product_reference).unwrap();

        let result = transform(
            &mut pcm_input.as_object_mut().unwrap(),
            pcdb_pcm_heat_battery,
            product_reference,
            &energy_supplies,
        );
        assert!(result.is_ok());
    }
}
