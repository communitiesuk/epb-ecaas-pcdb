use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::{CombiBoilerHotWaterSourceFields, Product, Technology};
use crate::transform::{
    InvalidProductCategoryError, ResolveProductsResult, product_reference_from_json_object,
};
use serde_json::{Map, Value as JsonValue, json};
use smartstring::alias::String;
use std::collections::HashMap;

pub fn transform(
    combi_boiler: &mut Map<std::string::String, JsonValue>,
    products: &HashMap<String, Product>,
) -> ResolveProductsResult<()> {
    if !combi_boiler.contains_key(PRODUCT_REFERENCE_FIELD) {
        return Ok(());
    }

    let product_reference = product_reference_from_json_object(combi_boiler)?;
    let product = &products[product_reference.as_str()];

    if let Technology::CombiBoiler {
        hot_water_source:
            CombiBoilerHotWaterSourceFields {
                separate_dhw_tests,
                storage_loss_factor_1,
                rejected_energy_1,
                storage_loss_factor_2,
                rejected_factor_3,
                ..
            },
        ..
    } = &product.technology
    {
        combi_boiler.insert("separate_DHW_tests".into(), json!(separate_dhw_tests));
        combi_boiler.insert("storage_loss_factor_1".into(), json!(storage_loss_factor_1));
        combi_boiler.insert("rejected_energy_1".into(), json!(rejected_energy_1));
        if let Some(storage_loss_factor_2) = storage_loss_factor_2 {
            combi_boiler.insert("storage_loss_factor_2".into(), json!(storage_loss_factor_2));
        }
        if let Some(rejected_factor_3) = rejected_factor_3 {
            combi_boiler.insert("rejected_factor_3".into(), json!(rejected_factor_3));
        }

        // now remove product reference
        combi_boiler.remove(PRODUCT_REFERENCE_FIELD);
    } else {
        return Err(InvalidProductCategoryError::from((product_reference, "combi boiler")).into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_ok::assert_ok;
    use itertools::Itertools;
    use serde_json::{from_str, json};
    use std::collections::{BTreeMap, HashMap};

    fn input(product_reference: &str) -> JsonValue {
        json!({
            "type": "CombiBoiler",
            "ColdWaterSource": "mains water",
            "HeatSourceWet": "combi boiler",
            "product_reference": product_reference,
        })
    }

    #[test]
    fn test_transform_combi_boiler() {
        let expected: JsonValue = from_str(include_str!(
            "../fixtures/combi_boiler_hot_water_source_transformed.json"
        ))
        .unwrap();
        let pcdb_combi_boilers: BTreeMap<String, Product> =
            from_str(include_str!("../fixtures/boilers_pcdb.json")).unwrap();
        let boiler_keys: Vec<String> = pcdb_combi_boilers.keys().cloned().collect();
        let mut inputs = boiler_keys.iter().map(|key| input(key)).collect_vec();

        let result: ResolveProductsResult<()> = pcdb_combi_boilers
            .into_iter()
            .zip(inputs.iter_mut())
            .map(|((name, combi_boiler), input)| {
                transform(
                    input.as_object_mut().unwrap(),
                    &HashMap::from([(name, combi_boiler)]),
                )
            })
            .collect::<Result<_, _>>();

        assert_ok!(result);

        for (i, name) in boiler_keys.iter().enumerate() {
            assert_eq!(
                inputs[i],
                expected[name.as_str()],
                "actual: {}\nexpected: {}",
                serde_json::to_string_pretty(&inputs[i]).unwrap(),
                serde_json::to_string_pretty(&expected[name.as_str()]).unwrap()
            );
        }
    }
}
