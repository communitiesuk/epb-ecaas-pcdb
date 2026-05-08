use crate::PRODUCT_REFERENCE_FIELD;
use crate::products::{Product, Technology};
use crate::transform::{
    InvalidProductCategoryError, ResolveProductsResult, product_reference_from_json_object,
};
use serde_json::{Value as JsonValue, json};
use smartstring::alias::String;
use std::collections::HashMap;

pub(crate) fn transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
) -> ResolveProductsResult<()> {
    let showers = match json.pointer_mut("/HotWaterDemand/Shower") {
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
        _ => return Ok(()),
    };

    for value in showers.values_mut() {
        if let JsonValue::Object(shower) = value {
            if let Some(shower_type) = shower.get("type").and_then(|v| v.as_str()) {
                if matches!(shower_type, "MixerShower")
                    && shower.contains_key(PRODUCT_REFERENCE_FIELD)
                {
                    let product_reference = product_reference_from_json_object(shower)?;
                    let product = &products[&product_reference];

                    if let Technology::AirPoweredShower {
                        flow_rate,
                        allow_low_flowrate,
                        ..
                    } = &product.technology
                    {
                        shower.insert("flowrate".into(), flow_rate.as_f64().into());
                        shower.insert("allow_low_flowrate".into(), json!(allow_low_flowrate));

                        // now remove product reference
                        shower.remove(PRODUCT_REFERENCE_FIELD);
                    } else {
                        return Err(InvalidProductCategoryError::from((
                            product_reference,
                            "air powered shower",
                        ))
                        .into());
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{from_str, json};

    fn input(product_reference: &str) -> JsonValue {
        json!({
            "HotWaterDemand": {
                "Shower": {
                    "Shower1": {
                        "type": "MixerShower",
                        "ColdWaterSource": "mains water",
                        "product_reference": product_reference,
                    }
                }
            }
        })
    }

    #[test]
    fn test_transform_air_powered_shower() {
        let product_reference = "432";
        let mut input = input(product_reference);
        let expected: JsonValue =
            from_str(include_str!("fixtures/air_powered_shower_transformed.json")).unwrap();
        let pcdb_shower: Product =
            from_str(include_str!("fixtures/air_powered_shower_pcdb.json")).unwrap();

        let result = transform(
            &mut input,
            &HashMap::from([(product_reference.into(), pcdb_shower)]),
        );

        assert!(result.is_ok());
        assert_eq!(
            input,
            expected,
            "actual: {}\nexpected: {}",
            serde_json::to_string_pretty(&input).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap()
        );
    }

    #[test]
    fn test_transform_air_powered_shower_errors_when_product_type_mismatch() {
        let product_reference = "hp";
        let mut input = input(product_reference);
        let pcdb_hps: HashMap<String, Product> =
            from_str(include_str!("fixtures/heat_pump_pcdb.json")).unwrap();

        let result = transform(&mut input, &pcdb_hps);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("air powered shower")
        );
    }
}
