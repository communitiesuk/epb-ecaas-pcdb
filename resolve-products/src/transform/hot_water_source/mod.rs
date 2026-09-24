use crate::PRODUCT_REFERENCE_FIELD;
use crate::in_use_factors::InUseFactorsAccess;
use crate::products::Product;
use crate::transform::{EnergySupplies, ResolveProductsResult};
use serde_json::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

mod combi_boiler;
pub mod heat_pump_hw_only;
pub mod smart_hot_water_tank;

pub async fn transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
    in_use_factors_access: &impl InUseFactorsAccess,
    energy_supplies: &EnergySupplies,
) -> ResolveProductsResult<()> {
    let hot_water_source = match json.pointer_mut("/HotWaterSource/hw cylinder") {
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
        _ => return Ok(()),
    };

    // first, transform any heat sources
    let heat_sources = hot_water_source
        .get_mut("HeatSource")
        .and_then(JsonValue::as_object_mut)
        .map(|sources| sources.values_mut().filter_map(JsonValue::as_object_mut))
        .into_iter()
        .flatten();

    for heat_source in heat_sources {
        if let Some(heat_source_type) = heat_source.get("type").and_then(|v| v.as_str()) {
            match heat_source_type {
                "HeatPump_HWOnly" => {
                    heat_pump_hw_only::transform(
                        heat_source,
                        products,
                        in_use_factors_access,
                        energy_supplies,
                    )
                    .await?;
                }
                _ => {}
            }
        }
    }

    if !hot_water_source.contains_key(PRODUCT_REFERENCE_FIELD) {
        return Ok(());
    }

    let source_type =
        if let Some(source_type) = hot_water_source.get("type").and_then(JsonValue::as_str) {
            source_type.to_string()
        } else {
            return Ok(());
        };

    match source_type.as_str() {
        "SmartHotWaterTank" => {
            smart_hot_water_tank::transform(hot_water_source, products)?;
        }
        "CombiBoiler" => {
            combi_boiler::transform(hot_water_source, products)?;
        }
        _ => {}
    }

    Ok(())
}
