use rstest::{fixture, rstest};

mod common;

#[fixture]
fn input_string() -> String {
    include_str!("./example_input.json").into()
}


#[tokio::test]
#[rstest]
async fn test_setup(input_string: String) {
    let input_bytes = input_string.as_bytes();
    let client = common::setup().await;

    let result = resolve_products::resolve_products(input_bytes, client).await;
    let error = result.err().unwrap();
    assert!(error.to_string().contains("Cannot do operations on a non-existent table"));
    // assert!(error.to_string(), "");
}
