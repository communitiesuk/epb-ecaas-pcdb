pub mod heat_source_wet;
mod space_heating;

use crate::errors::ResolvePcdbProductsError;
use crate::extract_product_references;
use crate::products::{find_products_for_references, Product, Technology};
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
    heat_source_wet::transform(json, &products)?;
    space_heating::transform(json, &products)?;

    Ok(())
}

pub type ResolveProductsResult<T> = Result<T, ResolvePcdbProductsError>;
