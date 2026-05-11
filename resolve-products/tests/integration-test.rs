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
    let _ = common::create_products_table(client).await;

    let result = resolve_products::resolve_products(input_bytes, client).await;
    let error = result.err().unwrap();
    assert!(error.to_string().contains("At least one product reference from the list (hp, boiler, mvhr, fancoil) could not be found within the PCDB store."));
    // assert_eq!(error.to_string(), "");
}
