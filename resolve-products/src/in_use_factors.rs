//! module is concerned with in use factors data that is stored alongside product data under individual IDs
//! with one `data` field containing a JSON list of items

#![allow(dead_code)]

use crate::errors::ResolvePcdbProductsError;
use crate::products::{
    HeatPumpVesselType, MechanicalVentilationDuctType, MechanicalVentilationInstallationType,
};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_dynamodb::types::AttributeValue;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_dynamo::from_item;
use serde_repr::Deserialize_repr;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Deserialize)]
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
pub struct MVInUseFactorEntry {
    #[serde(rename = "SFP_in_use_factor")]
    sfp_in_use_factor: f64,
    system_type: MechanicalVentilationSystemType,
    duct_type: MechanicalVentilationDuctType,
    installation: MechanicalVentilationInstallationType,
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

impl InUseFactorsAccess for DynamoDbBackedInUseFactorsAccess<'_> {
    async fn in_use_factors<T: InUseFactorsEntry>(
        &self,
    ) -> Result<Vec<T>, InUseFactorsInaccessibleError> {
        let data = self
            .dynamo_db_client
            .get_item()
            .key("id", AttributeValue::S(T::entry_id().to_string()))
            .send()
            .await
            .map_err(|_| ())?
            .item
            .and_then(|record| record.get("data").cloned())
            .ok_or(())
            .and_then(|attr_value| AttributeValue::as_m(&attr_value).map_err(|_| ()).cloned())?;

        Ok(from_item::<_, Vec<T>>(data).map_err(|_| ())?)
    }
}

#[derive(Debug, Error)]
#[error("The expected in use factors data was not available on the PCDB data store.")]
pub struct InUseFactorsInaccessibleError;

impl From<()> for InUseFactorsInaccessibleError {
    fn from(_: ()) -> Self {
        Self
    }
}

impl From<InUseFactorsInaccessibleError> for ResolvePcdbProductsError {
    fn from(_: InUseFactorsInaccessibleError) -> Self {
        Self::InUseFactorsInaccessibleError
    }
}

#[derive(Debug, Error)]
#[error("Unmatchable vessel type")]
pub struct UnmatchableVesselTypeError;

#[cfg(test)]
mod mocks {
    use crate::in_use_factors::{
        InUseFactorsAccess, InUseFactorsEntry, InUseFactorsInaccessibleError, MVInUseFactorEntry,
        MechanicalVentilationSystemType,
    };
    use crate::products::{MechanicalVentilationDuctType, MechanicalVentilationInstallationType};
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock};

    pub static IN_USE_FACTORS: LazyLock<HashMap<Arc<str>, serde_json::Value>> =
        LazyLock::new(|| {
            serde_json::from_str(include_str!("../test/in_use_factors.json")).unwrap()
        });

    pub struct FixtureBackedInUseFactorsAccess;

    impl InUseFactorsAccess for FixtureBackedInUseFactorsAccess {
        async fn in_use_factors<T: InUseFactorsEntry>(
            &self,
        ) -> Result<Vec<T>, InUseFactorsInaccessibleError> {
            let in_use_factors_json = IN_USE_FACTORS.get(T::entry_id()).ok_or(())?;

            Ok(serde_json::from_value(in_use_factors_json.clone()).map_err(|_| ())?)
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
                    && entry.installation == MechanicalVentilationInstallationType::InDuct)
                .unwrap()
                .sfp_in_use_factor,
            1.6
        );
    }
}
