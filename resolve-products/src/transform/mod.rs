pub mod transform_heat_source_wet;
mod transform_space_heating;

use crate::errors::ResolvePcdbProductsError;
use crate::extract_product_references;
use crate::products::{Product, Technology, find_products_for_references};
use aws_sdk_dynamodb::client::Client as DynamoDbClient;
use serde_json::value::Value as JsonValue;
use smartstring::alias::String;
use std::collections::HashMap;

pub async fn transform_json(
    json: &mut JsonValue,
    dynamo_client: &DynamoDbClient,
) -> ResolveProductsResult<()> {
    let product_references = extract_product_references(json)?;
    let products: HashMap<String, Product> =
        find_products_for_references(&product_references, dynamo_client).await?;
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
