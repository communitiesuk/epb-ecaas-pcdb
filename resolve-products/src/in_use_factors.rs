//! module is concerned with in use factors data that is stored alongside product data under individual IDs
//! with one `data` field containing a JSON list of items

use crate::products::{
    HeatPumpVesselType, MechanicalVentilationDuctType, MechanicalVentilationInstallationType,
};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_dynamo::from_item;
use serde_repr::Deserialize_repr;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HotWaterOnlyInUseFactorEntry {
    pub in_use_factor_mismatch: Decimal,
    pub vessel_type: Arc<str>,
}

const UNSPECIFIED_VESSEL_TYPE: &str = "Unspecified";

impl TryFrom<&HotWaterOnlyInUseFactorEntry> for Option<HeatPumpVesselType> {
    type Error = UnmatchableVesselTypeError;

    fn try_from(entry: &HotWaterOnlyInUseFactorEntry) -> Result<Self, Self::Error> {
        if entry.vessel_type.as_ref() == UNSPECIFIED_VESSEL_TYPE {
            return Ok(None);
        }

        Ok(entry
            .vessel_type
            .as_ref()
            .parse::<HeatPumpVesselType>()
            .map_err(|_| UnmatchableVesselTypeError)?
            .into())
    }
}

impl InUseFactorsEntry for HotWaterOnlyInUseFactorEntry {
    fn entry_id() -> &'static str {
        "HotWaterOnlyInUseFactors"
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MVInUseFactorEntry {
    pub(crate) sfp_in_use_factor: Decimal,
    pub(crate) system_type: MechanicalVentilationSystemType,
    pub(crate) duct_type: MechanicalVentilationDuctType,
    pub(crate) installation: MechanicalVentilationInstallationType,
}

impl InUseFactorsEntry for MVInUseFactorEntry {
    fn entry_id() -> &'static str {
        "MVInUseFactors"
    }
}

#[derive(Clone, Copy, Debug, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum MechanicalVentilationSystemType {
    CentralisedMev = 1,
    DecentralisedMev = 2,
    CentralisedMvAndMvhr = 3,
    PositiveInputVentilation = 5,
    DefaultData = 10,
}

pub trait InUseFactorsEntry: DeserializeOwned {
    fn entry_id() -> &'static str;
}

pub trait InUseFactorsAccess {
    async fn in_use_factors<T: InUseFactorsEntry>(
        &self,
    ) -> Result<Vec<T>, InUseFactorsInaccessibleError>;
}

pub struct DynamoDbBackedInUseFactorsAccess<'a> {
    dynamo_db_client: &'a DynamoDbClient,
}

impl<'a> DynamoDbBackedInUseFactorsAccess<'a> {
    pub fn new(dynamo_client: &'a DynamoDbClient) -> Self {
        Self {
            dynamo_db_client: dynamo_client,
        }
    }
}

impl InUseFactorsAccess for DynamoDbBackedInUseFactorsAccess<'_> {
    async fn in_use_factors<T: InUseFactorsEntry>(
        &self,
    ) -> Result<Vec<T>, InUseFactorsInaccessibleError> {
        let data = self
            .dynamo_db_client
            .get_item()
            .table_name("products")
            .key("id", AttributeValue::S(T::entry_id().to_string()))
            .send()
            .await?
            .item
            .and_then(|record| record.get("data").cloned())
            .ok_or(InUseFactorsInaccessibleError::DataKeyMissingFromInUseFactorsRecord)
            .and_then(|attr_value| {
                AttributeValue::as_l(&attr_value)
                    .map_err(|e| {
                        InUseFactorsInaccessibleError::InUseFactorsRecordDataFieldNotList(e.clone())
                    })
                    .cloned()
            })?;

        let mut erroring_attribute_values: Vec<AttributeValue> = vec![];

        Ok(data
            .into_iter()
            .map(|item| {
                from_item::<HashMap<String, AttributeValue>, T>({
                    let map = AttributeValue::as_m(&item).cloned();
                    if map.is_err() {
                        erroring_attribute_values.push(item.clone());
                    }
                    map.map_err(|_| {
                        serde::de::Error::custom(
                            "Could not deserialize item into object as expected",
                        )
                    })?
                })
            })
            .collect::<Result<Vec<T>, _>>()
            .map_err(|_| {
                InUseFactorsInaccessibleError::DeserializeError(erroring_attribute_values)
            })?)
    }
}

#[derive(Debug, Error)]
#[error("The expected in use factors data was not available on the PCDB data store.")]
pub enum InUseFactorsInaccessibleError {
    #[error("Could not make query for in use factors against DynamoDB: {0}")]
    DynamoDbError(#[from] SdkError<GetItemError, HttpResponse>),
    DataKeyMissingFromInUseFactorsRecord,
    InUseFactorsRecordDataFieldNotList(AttributeValue),
    DeserializeError(Vec<AttributeValue>),
    #[cfg(test)]
    #[error("Could not deserialize item into object as expected")]
    IncorrectFixture(String),
}

#[derive(Debug, Error)]
#[error("Unmatchable vessel type")]
pub struct UnmatchableVesselTypeError;

#[cfg(test)]
pub mod mocks {
    use crate::in_use_factors::{
        InUseFactorsAccess, InUseFactorsEntry, InUseFactorsInaccessibleError, MVInUseFactorEntry,
        MechanicalVentilationSystemType,
    };
    use crate::products::{MechanicalVentilationDuctType, MechanicalVentilationInstallationType};
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock};

    pub static IN_USE_FACTORS: LazyLock<HashMap<Arc<str>, serde_json::Value>> =
        LazyLock::new(|| {
            serde_json::from_str(include_str!("transform/fixtures/in_use_factors.json")).unwrap()
        });

    pub struct FixtureBackedInUseFactorsAccess;

    impl InUseFactorsAccess for FixtureBackedInUseFactorsAccess {
        async fn in_use_factors<T: InUseFactorsEntry>(
            &self,
        ) -> Result<Vec<T>, InUseFactorsInaccessibleError> {
            let in_use_factors_json: serde_json::Value = IN_USE_FACTORS[T::entry_id()].to_owned();

            Ok(
                serde_json::from_value(in_use_factors_json.clone()).map_err(|_| {
                    InUseFactorsInaccessibleError::IncorrectFixture(in_use_factors_json.to_string())
                })?,
            )
        }
    }

    #[tokio::test]
    async fn test_can_access_mock() {
        let fixture_access = FixtureBackedInUseFactorsAccess;
        let result = fixture_access.in_use_factors::<MVInUseFactorEntry>().await;
        assert!(result.is_ok());
        assert_eq!(
            result
                .unwrap()
                .into_iter()
                .find(|entry| entry.system_type
                    == MechanicalVentilationSystemType::PositiveInputVentilation
                    && entry.duct_type == MechanicalVentilationDuctType::Flexible
                    && entry.installation
                        == MechanicalVentilationInstallationType::InstalledUnderApprovedScheme)
                .unwrap()
                .sfp_in_use_factor
                .as_f64(),
            1.6
        );
    }
}
