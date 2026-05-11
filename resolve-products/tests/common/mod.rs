// set up shared utilities

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_dynamodb::config::Credentials;
use testcontainers_modules::dynamodb_local::DynamoDb;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::core::IntoContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::OnceCell;

static ONCE_DYNAMODB_CLIENT: OnceCell<DynamoDbClient> = OnceCell::const_new();

static DYNAMO_NODE: OnceCell<ContainerAsync<DynamoDb>> = OnceCell::const_new();

pub async fn setup() -> &'static DynamoDbClient {
    ONCE_DYNAMODB_CLIENT
        .get_or_init(|| async {
            let dynamo_node = DYNAMO_NODE
                .get_or_init(|| async {
                    DynamoDb::default()
                        .start()
                        .await
                        .expect("Failed to start DynamoDB Local container")
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
        .await
}
