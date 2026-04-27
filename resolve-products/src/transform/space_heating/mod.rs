mod elec_storage_heater;

use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::Product;
use crate::transform::ResolveProductsResult;
use serde_json::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

pub fn transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
) -> ResolveProductsResult<()> {
    let space_heat_systems = match json.pointer_mut("/SpaceHeatSystem") {
        Some(node)
            if node.is_object() => {
                node.as_object_mut().unwrap()
            }
        _ => return Ok(()),
    };

    for value in space_heat_systems.values_mut() {
        if let JsonValue::Object(system) = value {
            let product_reference = if system.contains_key(PRODUCT_REFERENCE_FIELD) {
                String::from(system[PRODUCT_REFERENCE_FIELD].as_str().ok_or_else(|| {
                    ResolvePcdbProductsError::InvalidProductCategoryReference(
                        system[PRODUCT_REFERENCE_FIELD].clone(),
                    )
                })?)
                .into()
            } else {
                None
            };

            if let Some(product_reference) = product_reference {
                if system
                    .get("type")
                    .is_some_and(|v| matches!(v, JsonValue::String(s) if s == "ElecStorageHeater"))
                {
                    elec_storage_heater::transform(
                        system,
                        &products[product_reference.as_str()],
                        &product_reference,
                    )?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::space_heating::transform;
    use serde_json::json;

    #[test]
    fn test_transform_space_heating_with_elec_storage_heaters() {
        let product_ref = "444";
        let mut input = json!({
            "SpaceHeatSystem": {
                "SpaceHeatSystem1": {
                    "type": "ElecStorageHeater",
                    "n_units": 1,
                    "Zone": "ThermalZone",
                    "product_reference": product_ref,
                },
                "SpaceHeatSystem2": {
                    "type": "ElecStorageHeater",
                    "n_units": 1,
                    "Zone": "ThermalZone",
                    "product_reference": product_ref,
                }
            }
        });
        let pcdb_esh =
            serde_json::from_str(include_str!("../../../test/test_esh_pcdb.json")).unwrap();
        let products = HashMap::from([(product_ref.into(), pcdb_esh)]);
        let expected_esh: JsonValue = serde_json::from_str(include_str!(
            "../../../test/test_esh_input_transformed.json"
        ))
        .unwrap();
        let expected_input = json!({
            "SpaceHeatSystem": {
                "SpaceHeatSystem1": expected_esh,
                "SpaceHeatSystem2": expected_esh
            }
        });

        let result = transform(&mut input, &products);

        assert!(result.is_ok());

        assert_eq!(input, expected_input);
    }
}
