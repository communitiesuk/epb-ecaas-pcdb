pub mod heat_source_wet;
mod space_heating;

use crate::errors::ResolvePcdbProductsError;
use crate::extract_product_references;
use crate::products::{
    find_products_for_references, DynamoDbBackedProductCatalogue, FuelType, Product, Technology,
};
use aws_sdk_dynamodb::client::Client as DynamoDbClient;
use serde_json::value::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;
use std::sync::Arc;

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

    Ok(())
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

#[cfg(test)]
mod catalogue {
    use crate::errors::ResolvePcdbProductsError;
    use crate::products::{Product, ProductCatalogue};
    use crate::transform::{extract_energy_supplies, EnergySupplies, ResolveProductsResult};
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
}
