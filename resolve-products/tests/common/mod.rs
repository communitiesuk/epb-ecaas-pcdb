// set up shared utilities
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::Credentials;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::create_table::{CreateTableError, CreateTableOutput};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use aws_sdk_dynamodb::{Client as DynamoDbClient, Client};
use serde_dynamo::to_item;
use serde_json::{Value, from_str};
use std::collections::HashMap;
use std::process::Command;
use testcontainers_modules::dynamodb_local::DynamoDb;
use testcontainers_modules::testcontainers::core::IntoContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::sync::OnceCell;

static ONCE_DYNAMODB_CLIENT: OnceCell<DynamoDbClient> = OnceCell::const_new();

static DYNAMO_NODE: OnceCell<ContainerAsync<DynamoDb>> = OnceCell::const_new();

pub async fn setup() -> &'static DynamoDbClient {
    let client = ONCE_DYNAMODB_CLIENT
        .get_or_init(|| async {
            let dynamo_node = DYNAMO_NODE
                .get_or_init(|| async {
                    let _ = Command::new("sh")
                        .arg("-c")
                        .arg("docker ps -aq --filter 'label=tests=pcdb' | xargs -r docker rm -f")
                        .output();
                    let node = DynamoDb::default()
                        .with_label("tests", "pcdb")
                        .start()
                        .await
                        .expect("Failed to start DynamoDB Local container");

                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                    node
                })
                .await;

            let host = dynamo_node
                .get_host()
                .await
                .expect("Could not resolve host for DynamoDB node");
            let host_port = dynamo_node
                .get_host_port_ipv4(8000.tcp())
                .await
                .expect("Could not resolve port for DynamoDB node");

            let endpoint_url = format!("http://{host}:{host_port}");

            let config = aws_config::defaults(BehaviorVersion::latest())
                .region("eu-west-2")
                .endpoint_url(endpoint_url)
                .credentials_provider(Credentials::new(
                    "dummyaccesskey",
                    "dummysecretkey",
                    None,
                    None,
                    "dummy",
                ))
                .load()
                .await;

            DynamoDbClient::new(&config)
        })
        .await;

    let _ = create_products_table(client).await;

    let products: Value =
        from_str::<Value>(include_str!("../fixtures/pcdb_products.json")).unwrap();
    if let Some(products) = products.as_object() {
        for product in products.values() {
            add_item(client, product.clone()).await;
        }
    }

    client
}

pub async fn create_products_table(
    client: &DynamoDbClient,
) -> Result<CreateTableOutput, SdkError<CreateTableError, HttpResponse>> {
    let id_attribute = AttributeDefinition::builder()
        .attribute_name("id")
        .attribute_type(ScalarAttributeType::S)
        .build()?;

    // TODO: will we need attribute definitions for technologyType, technologyGroup and brandName

    let keys = KeySchemaElement::builder()
        .attribute_name("id")
        .key_type(KeyType::Hash)
        .build()?;

    client
        .create_table()
        .table_name("products")
        .key_schema(keys)
        .attribute_definitions(id_attribute)
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
}

async fn add_item(client: &Client, item: Value) {
    let product_data: HashMap<String, AttributeValue> = to_item(item).unwrap();

    let request = client
        .put_item()
        .table_name("products")
        .set_item(product_data.into());

    request.send().await.unwrap();
}
