// set up shared utilities
use aws_config::BehaviorVersion;
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

static DYNAMO_NODE: OnceCell<ContainerAsync<DynamoDb>> = OnceCell::const_new();
static DYNAMO_URL: OnceCell<String> = OnceCell::const_new();

pub async fn setup() -> DynamoDbClient {
    let url = DYNAMO_URL
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

            let seed_config = aws_config::defaults(BehaviorVersion::latest())
                .behavior_version(BehaviorVersion::latest())
                .endpoint_url(&endpoint_url)
                .region("eu-west-2")
                .load()
                .await;
            let seed_client = DynamoDbClient::new(&seed_config);

            create_products_table(&seed_client)
                .await
                .expect("Failed to create table");
            populate_products_table(seed_client).await;

            endpoint_url
        })
        .await;

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region("eu-west-2")
        .endpoint_url(url)
        .load()
        .await;

    DynamoDbClient::new(&config)
}

async fn create_products_table(
    client: &DynamoDbClient,
) -> Result<CreateTableOutput, SdkError<CreateTableError, HttpResponse>> {
    let id_attribute = AttributeDefinition::builder()
        .attribute_name("id")
        .attribute_type(ScalarAttributeType::S)
        .build()?;

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

async fn populate_products_table(client: DynamoDbClient) {
    let products: Value = from_str::<Value>(include_str!("../fixtures/pcdb_products.json"))
        .expect("Could not parse products file");

    if let Some(products) = products.as_object() {
        for product in products.values() {
            add_item(&client, product.clone()).await;
        }
    }
}

async fn add_item(client: &Client, item: Value) {
    let product_data: HashMap<String, AttributeValue> = to_item(item).unwrap();

    let request = client
        .put_item()
        .table_name("products")
        .set_item(product_data.into());

    request.send().await.unwrap();
}
