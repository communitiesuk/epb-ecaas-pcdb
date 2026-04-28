pub mod heat_source_wet;
mod space_heating;

use crate::errors::ResolvePcdbProductsError;
use crate::extract_product_references;
use crate::products::{
    find_products_for_references, DynamoDbBackedProductCatalogue, Product, Technology,
};
use aws_sdk_dynamodb::client::Client as DynamoDbClient;
use serde_json::value::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

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
    heat_source_wet::transform(json, &products, &product_catalogue)?;
    space_heating::transform(json, &products)?;

    Ok(())
}

pub type ResolveProductsResult<T> = Result<T, ResolvePcdbProductsError>;

#[cfg(test)]
mod catalogue {
    use crate::errors::ResolvePcdbProductsError;
    use crate::products::{Product, ProductCatalogue};
    use crate::transform::ResolveProductsResult;
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
                        .and_then(|product_json| serde_json::from_value(product_json.clone()).ok())
                        .ok_or(ResolvePcdbProductsError::UnknownProductReference(
                            reference.to_string(),
                        ));
                    Ok((reference.clone(), product?))
                })
                .collect()
        }
    }
}
