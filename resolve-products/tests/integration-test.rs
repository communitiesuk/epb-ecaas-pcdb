use std::io::{BufReader, Cursor};

mod common;

#[tokio::test]
async fn test_setup() {
    let client = common::setup().await;
    let result =
        resolve_products::resolve_products(BufReader::new(Cursor::new("{}")), client).await;
    assert!(result.is_err());
}
