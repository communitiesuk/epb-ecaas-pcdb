pub mod heat_source_wet;
mod space_heating;
mod wwhrs;

use crate::errors::ResolvePcdbProductsError;
use crate::products::{
    DynamoDbBackedProductCatalogue, FuelType, Product, Technology, find_products_for_references,
};
use crate::{PRODUCT_REFERENCE_FIELD, extract_product_references};
use aws_sdk_dynamodb::client::Client as DynamoDbClient;
use serde_json::Map;
use serde_json::value::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

pub async fn transform_json(
    json: &mut JsonValue,
    dynamo_client: &DynamoDbClient,
) -> ResolveProductsResult<()> {
    let product_references = extract_product_references(json)?;
    let product_catalogue = DynamoDbBackedProductCatalogue::new(dynamo_client);
    let products: HashMap<String, Product> =
        find_products_for_references(&product_references, &product_catalogue).await?;
    if products.values().any(|p| {
        !matches!(
            p.technology,
            Technology::HeatPump { .. }
                | Technology::Boiler { .. }
                | Technology::ElectricStorageHeater { .. }
                | Technology::HeatBatteryDryCore { .. }
                | Technology::HeatBatteryPcm { .. }
                | Technology::Radiator { .. }
                | Technology::UnderfloorHeating { .. }
                | Technology::FanCoil { .. }
                | Technology::Wwhrs { .. }
        )
    }) {
        return Err(ResolvePcdbProductsError::UnsupportedProductAtMapping);
    }

    let energy_supplies = extract_energy_supplies(json).map_err(|_| {
        ResolvePcdbProductsError::InvalidRequestEncounteredAfterSchemaCheck(
            "Energy Supply node was not in expected form in request payload",
        )
    })?;

    heat_source_wet::transform(json, &products, &product_catalogue, &energy_supplies).await?;
    space_heating::transform(json, &products, &energy_supplies)?;
    wwhrs::transform(json, &products)?;

    Ok(())
}

fn product_reference_from_json_object(
    product_json: &Map<std::string::String, JsonValue>,
) -> Result<String, ResolvePcdbProductsError> {
    Ok(String::from(
        product_json[PRODUCT_REFERENCE_FIELD]
            .as_str()
            .ok_or_else(|| {
                ResolvePcdbProductsError::InvalidProductCategoryReference(
                    product_json[PRODUCT_REFERENCE_FIELD].clone(),
                )
            })?,
    ))
}

pub type ResolveProductsResult<T> = Result<T, ResolvePcdbProductsError>;

pub(crate) type EnergySupplies = HashMap<FuelType, Arc<str>>;

fn extract_energy_supplies(json: &JsonValue) -> Result<EnergySupplies, ()> {
    let energy_supplies_node = json
        .get("EnergySupply")
        .and_then(JsonValue::as_object)
        .ok_or(())?;
    let mut energy_supplies = HashMap::from([(FuelType::Electricity, Arc::from("mains elec"))]);

    for (energy_supply_name, energy_supply) in energy_supplies_node {
        let fuel_type = energy_supply.get("fuel").ok_or(())?;
        let fuel_type: FuelType = serde_json::from_value(fuel_type.clone()).map_err(|_| ())?;
        // if it's electricity, skip as this always maps to "mains elec" as per the FHS schema
        if fuel_type == FuelType::Electricity {
            continue;
        }

        energy_supplies.insert(fuel_type, Arc::from(energy_supply_name.as_str()));
    }

    Ok(energy_supplies)
}

#[derive(Debug, Error)]
#[error(
    "Product reference '{product_reference}' does not have the expected category '{category_for_display}' product."
)]
struct InvalidProductCategoryError {
    product_reference: String,
    category_for_display: &'static str,
}

impl<T: Into<String>> From<(T, &'static str)> for InvalidProductCategoryError {
    fn from((product_reference, category_for_display): (T, &'static str)) -> Self {
        Self {
            product_reference: product_reference.into(),
            category_for_display,
        }
    }
}

type TransformResult = Result<(), InvalidProductCategoryError>;

impl From<InvalidProductCategoryError> for ResolvePcdbProductsError {
    fn from(err: InvalidProductCategoryError) -> Self {
        ResolvePcdbProductsError::ProductCategoryMismatches(vec![err.to_string()])
    }
}

#[cfg(test)]
mod catalogue {
    use crate::errors::ResolvePcdbProductsError;
    use crate::products::{Product, ProductCatalogue};
    use crate::transform::{EnergySupplies, ResolveProductsResult, extract_energy_supplies};
    use itertools::Itertools;
    use serde_json::{Map, Value};
    use std::collections::HashMap;

    pub(crate) struct FixtureBackedProductCatalogue {
        products: serde_json::Map<String, serde_json::Value>,
    }

    impl FixtureBackedProductCatalogue {
        pub(crate) fn new() -> Self {
            Self {
                products: serde_json::from_str::<serde_json::Value>(include_str!(
                    "../../test/referenced_products.json"
                ))
                .unwrap()
                .as_object()
                .unwrap()
                .to_owned(),
            }
        }
    }

    impl ProductCatalogue for FixtureBackedProductCatalogue {
        async fn find_products_for_references(
            &self,
            product_references: &[smartstring::alias::String],
        ) -> ResolveProductsResult<HashMap<smartstring::alias::String, Product>> {
            product_references
                .iter()
                .map(|reference| {
                    let product: Result<Product, _> = self
                        .products
                        .get(reference.as_str())
                        .ok_or(ResolvePcdbProductsError::UnknownProductReference(
                            reference.to_string(),
                        ))
                        .and_then(|product_json| {
                            serde_json::from_value(product_json.clone())
                                .map_err(ResolvePcdbProductsError::BadTestProductError)
                        });
                    Ok((reference.clone(), product?))
                })
                .collect()
        }
    }

    pub(crate) fn mock_energy_supplies() -> EnergySupplies {
        let mock_energy_supplies_json =
            serde_json::from_str(include_str!("../../test/request_with_energy_supplies.json"))
                .unwrap();
        extract_energy_supplies(&mock_energy_supplies_json).unwrap()
    }

    pub(crate) fn transformed_input_matches_expected(
        transformed_input: &Value,
        expected_input: Map<String, Value>,
    ) {
        let mut actual_keys = transformed_input.as_object().unwrap().keys().collect_vec();
        actual_keys.sort();

        let mut expected_keys = expected_input.keys().collect_vec();
        expected_keys.sort();

        assert_eq!(actual_keys, expected_keys);

        for key in expected_keys {
            assert_eq!(transformed_input[key], expected_input[key], "{:?}", key);
        }
    }
}
