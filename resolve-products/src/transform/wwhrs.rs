use crate::PRODUCT_REFERENCE_FIELD;
use crate::errors::ResolvePcdbProductsError;
use crate::products::{Product, Technology, WwhrsSystemType};
use crate::transform::{
    InvalidProductCategoryError, ResolveProductsResult, product_reference_from_json_object,
};
use itertools::Itertools;
use serde_json::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

pub fn transform(
    json: &mut JsonValue,
    products: &HashMap<String, Product>,
) -> ResolveProductsResult<()> {
    let wwhrs_systems = match json.pointer_mut("/WWHRS") {
        Some(node) if node.is_object() => node.as_object_mut().unwrap(),
        _ => return Ok(()),
    };

    for value in wwhrs_systems.values_mut() {
        if let JsonValue::Object(wwhrs) = value {
            if wwhrs.contains_key(PRODUCT_REFERENCE_FIELD) {
                let product_reference = product_reference_from_json_object(wwhrs)?;
                let product = &products[&product_reference];

                if let Technology::Wwhrs {
                    test_data,
                    utilisation_factor,
                    ..
                } = &product.technology
                {
                    wwhrs.insert("type".into(), "WWHRS_Instantaneous".into());

                    let system_type: WwhrsSystemType = test_data
                        .first()
                        .ok_or_else(|| {
                            ResolvePcdbProductsError::InvalidProduct(
                                product_reference.to_string(),
                                "WWHRS test data was not expected to be empty",
                            )
                        })?
                        .system_type;

                    let (flow_rates, system_efficiencies): (Vec<f64>, Vec<f64>) = test_data
                        .iter()
                        .sorted_by(|a, b| a.flow_rate.cmp(&b.flow_rate))
                        .map(|test_datum| {
                            (
                                test_datum.flow_rate.as_f64(),
                                test_datum.efficiency.as_f64(),
                            )
                        })
                        .unzip();

                    let (efficiencies_field, utilisation_factor_field) = match system_type {
                        WwhrsSystemType::A => {
                            ("system_a_efficiencies", "system_a_utilisation_factor")
                        }
                        WwhrsSystemType::B => {
                            ("system_b_efficiencies", "system_b_utilisation_factor")
                        }
                        WwhrsSystemType::C => {
                            ("system_c_efficiencies", "system_c_utilisation_factor")
                        }
                    };

                    wwhrs.insert("flow_rates".into(), flow_rates.into());
                    wwhrs.insert(efficiencies_field.into(), system_efficiencies.into());
                    wwhrs.insert(
                        utilisation_factor_field.into(),
                        utilisation_factor.as_f64().into(),
                    );

                    // now remove product reference
                    wwhrs.remove(PRODUCT_REFERENCE_FIELD);
                } else {
                    return Err(
                        InvalidProductCategoryError::from((product_reference, "WWHRS")).into(),
                    );
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::{from_str, json};
    use std::collections::HashMap;

    fn input(product_reference: &str) -> JsonValue {
        json!({"WWHRS": {
                product_reference: {
                    "ColdWaterSource": "mains water",
                    "product_reference": product_reference,
                }}
        })
    }

    #[rstest]
    #[case("wwhrsA")]
    #[case("wwhrsC")]
    fn test_transform_wwhrs_system_c(#[case] product_reference: &str) {
        let mut input = input(product_reference);
        let expected = from_str::<JsonValue>(include_str!("../../test/wwhrs_transformed.json"))
            .unwrap()
            .get(product_reference)
            .unwrap()
            .clone();
        let pcdb_wwhrs: Product = serde_json::from_value(
            from_str::<JsonValue>(include_str!("../../test/wwhrs_pcdb.json"))
                .unwrap()
                .get(product_reference)
                .unwrap()
                .clone(),
        )
        .unwrap();

        let result = transform(
            &mut input,
            &HashMap::from([(product_reference.into(), pcdb_wwhrs)]),
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
    fn test_transform_wwhrs_errors_when_product_type_mismatch() {
        let product_reference = "hp";
        let mut input = input(product_reference);
        let pcdb_hps: HashMap<String, Product> =
            from_str(include_str!("../../test/test_heat_pump_pcdb.json")).unwrap();

        let result = transform(&mut input, &pcdb_hps);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("WWHRS"));
    }
}
