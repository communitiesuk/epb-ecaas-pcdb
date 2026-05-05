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

impl InUseFactorsEntry<'_> for HotWaterOnlyInUseFactorEntry {
    fn entry_id() -> &'static str {
        "HotWaterOnly"
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

impl InUseFactorsEntry<'_> for MVInUseFactorEntry {
    fn entry_id() -> &'static str {
        "MV"
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

pub trait InUseFactorsEntry<'de>: Deserialize<'de> {
    fn entry_id() -> &'static str;
}

pub trait InUseFactorsAccess {
    async fn in_use_factors<'de, T: InUseFactorsEntry<'de>>(
        &self,
    ) -> Result<Vec<T>, InUseFactorsInaccessibleError>;
}

pub struct DynamoDbBackedInUseFactorsAccess<'a> {
    dynamo_db_client: &'a DynamoDbClient,
}

impl InUseFactorsAccess for DynamoDbBackedInUseFactorsAccess<'_> {
    async fn in_use_factors<'a, T: InUseFactorsEntry<'a>>(
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
