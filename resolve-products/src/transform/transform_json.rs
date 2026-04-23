use crate::errors::ResolvePcdbProductsError;
use crate::products::{
    find_products_for_references,
    Product, Technology,
};
use crate::extract_product_references;
use aws_sdk_dynamodb::client::Client as DynamoDbClient;
use serde_json::value::Value as JsonValue;
use crate::transform::transform_heat_source_wet;

pub async fn transform_json(
    json: &mut JsonValue,
    dynamo_client: &DynamoDbClient,
) -> ResolveProductsResult<()> {
    let product_references = extract_product_references(json)?;
    let products = find_products_for_references(&product_references, dynamo_client).await?;
    for product in products.values() {
        match product {
            Product {
                technology: Technology::HeatPump { .. } | Technology::Boiler { .. },
                ..
            } => continue,
            _ => return Err(ResolvePcdbProductsError::UnsupportedProductAtMapping),
        }
    }
    transform_heat_source_wet::transform_heat_source_wet(json, &products)
}

pub type ResolveProductsResult<T> = Result<T, ResolvePcdbProductsError>;

