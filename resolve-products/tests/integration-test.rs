use jsonschema::ValidationError;
use rstest::{fixture, rstest};
use serde_json::{Value, from_str};

mod common;

#[fixture]
fn input() -> &'static [u8] {
    include_bytes!("./example_input_hp_only.json")
}

async fn add_item(client: &Client, item: Value) {
    println!("Item: {:?}", item);
    let product_data: HashMap<String, AttributeValue> = to_item(item).unwrap();

    let request = client
        .put_item()
        .table_name("products")
        .set_item(product_data.into());

#[tokio::test]
#[rstest]
async fn test_setup(input: &[u8]) {
    let client = common::setup().await;

    let result = resolve_products::resolve_products(input, client).await;
    assert!(result.is_ok());
}
