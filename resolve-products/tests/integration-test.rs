use aws_sdk_dynamodb::{Client, types::AttributeValue};
use rstest::{fixture, rstest};
use serde_dynamo::to_item;
use serde_json::{Value, from_str};
use std::collections::HashMap;

mod common;

#[fixture]
fn input_string() -> String {
    include_str!("./example_input_hp_only.json").into()
}

async fn add_item(client: &Client, item: Value) {
    println!("Item: {:?}", item);
    let product_data: HashMap<String, AttributeValue> = to_item(item).unwrap();

    let request = client
        .put_item()
        .table_name("products")
        .set_item(product_data.into());

    request.send().await.unwrap();
}

#[tokio::test]
#[rstest]
async fn test_setup(input_string: String) {
    let input_bytes = input_string.as_bytes();
    let client = common::setup().await;

    let hp: Value = from_str::<Value>(include_str!("./pcdb_products.json"))
        .unwrap()
        .pointer("/hp")
        .unwrap()
        .clone();

    let _ = add_item(client, hp).await;

    let result = resolve_products::resolve_products(input_bytes, client).await;
    assert!(result.is_ok());
}
