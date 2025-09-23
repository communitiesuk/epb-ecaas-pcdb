#![allow(dead_code)]

use crate::errors::ResolvePcdbProductsError;
use crate::ResolveProductsResult;
use indexmap::IndexMap;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_enum_str::{Deserialize_enum_str, Serialize_enum_str};
use serde_valid::Validate;
use smartstring::alias::String;
use std::collections::HashMap;
use std::sync::LazyLock;

pub(crate) fn find_products_for_references<'a>(
    product_references: &[String],
) -> ResolveProductsResult<HashMap<&'a str, &'a Product<'a>>> {
    PCDB_PRODUCTS
        .iter()
        .filter(|(k, _)| product_references.contains(k))
        .map(|(k, v)| {
            if product_references.contains(k) {
                Ok((k.as_str(), v))
            } else {
                Err(ResolvePcdbProductsError::UnknownProductReference(
                    k.to_string(),
                ))
            }
        })
        .collect()
}

fn find_product_by_reference(reference: &str) -> Option<&Product<'_>> {
    PCDB_PRODUCTS.get(reference)
}

static PCDB_PRODUCTS: LazyLock<IndexMap<String, Product>> =
    LazyLock::new(|| serde_json::from_str(include_str!("products.json")).unwrap());

#[derive(Debug, Deserialize, Validate)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Manufacturer<'a> {
    #[serde(rename = "ID")]
    #[validate(pattern = r"^\d+$")]
    id: &'a str,
    #[serde(rename = "manufacturerReferenceNo")]
    #[validate(pattern = r"^\d+$")]
    manufacturer_reference_number: &'a str,
    current_name: &'a str,
    secondary_addressable: Option<&'a str>,
    primary_addressable: Option<&'a str>,
    street_name: &'a str,
    locality_name: Option<&'a str>,
    town_name: &'a str,
    administrative_area_name: Option<&'a str>,
    postcode: &'a str,
    country: Option<&'a str>,
    phone_number: &'a str,
    url: &'a str,
    #[validate(
        pattern = r"(19|20)\d{2}-(0[1-9]|1[1,2])-(0[1-9]|[12][0-9]|3[01])\s([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])"
    )]
    updated: &'a str,
}

#[derive(Debug, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Product<'a> {
    #[serde(rename = "ID")]
    id: u32,
    manufacturer: Manufacturer<'a>,
    original_manufacturer_name: Option<&'a str>,
    brand_name: &'a str,
    model_name: &'a str,
    model_qualifier: &'a str,
    first_year_of_manufacture: u16,
    final_year_of_manufacture: Option<YearOfManufacture>,
    #[serde(flatten)]
    pub(crate) technology: Technology<'a>,
}

#[derive(Debug, Clone)]
enum YearOfManufacture {
    Current,
    Year(u16),
}

impl YearOfManufacture {
    pub fn is_current(&self) -> bool {
        matches!(self, YearOfManufacture::Current)
    }

    pub fn as_year(&self) -> Option<u16> {
        match self {
            YearOfManufacture::Year(year) => Some(*year),
            YearOfManufacture::Current => None,
        }
    }
}

// Custom deserialization to handle the string "current"
impl<'de> Deserialize<'de> for YearOfManufacture {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum YearHelper {
            String(String),
            Number(u16),
        }

        match YearHelper::deserialize(deserializer)? {
            YearHelper::String(s) if s == "current" => Ok(YearOfManufacture::Current),
            YearHelper::Number(year) => Ok(YearOfManufacture::Year(year)),
            YearHelper::String(other) => Err(serde::de::Error::custom(format!(
                "expected 'current' or an integer, found '{}'",
                other
            ))),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "technologyType", rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum Technology<'a> {
    #[serde(rename = "Air Source Heat Pump", rename_all = "camelCase")]
    AirSourceHeatPump {
        energy_supply: &'a str,
        source_type: HeatPumpSourceType,
        sink_type: HeatPumpSinkType,
        backup_control_type: HeatPumpBackupControlType,
        modulating_control: bool,
        #[serde(rename = "standardRatingCapacity20C")]
        standard_rating_capacity_20c: Decimal,
        #[serde(rename = "standardRatingCapacity35C")]
        standard_rating_capacity_35c: Decimal,
        #[serde(rename = "standardRatingCapacity55C")]
        standard_rating_capacity_55c: Decimal,
        #[serde(rename = "minimumModulationRate35")]
        minimum_modulation_rate_35: Decimal,
        #[serde(rename = "minimumModulationRate55")]
        minimum_modulation_rate_55: Decimal,
        time_constant_on_off_operation: i32,
        temp_return_feed_max: Decimal,
        temp_lower_operating_limit: Decimal,
        min_temp_diff_flow_return_for_hp_to_operate: i32,
        #[serde(rename = "varFlowTempCtrlDuringTest")]
        variable_temp_control: bool,
        power_heating_circ_pump: Decimal,
        power_heating_warm_air_fan: Decimal,
        power_source_circ_pump: Decimal,
        power_standby: Decimal,
        power_crankcase_heater: Decimal,
        power_off: Decimal,
        power_maximum_backup: Decimal,
        test_data: Vec<HeatPumpTestDatum>,
    },
}

#[derive(Copy, Clone, Debug, Deserialize_enum_str, PartialEq, Serialize_enum_str)]
pub(crate) enum HeatPumpSourceType {
    Ground,
    OutsideAir,
    ExhaustAirMEV,
    ExhaustAirMVHR,
    ExhaustAirMixed,
    WaterGround,
    WaterSurface,
    HeatNetwork,
}

// following heat pump related enums are copied in from epb-home-energy-model for now

#[derive(Copy, Clone, Debug, Deserialize_enum_str, PartialEq, Serialize_enum_str)]
pub(crate) enum HeatPumpSinkType {
    Water,
    Air,
}

#[derive(Copy, Clone, Debug, Deserialize_enum_str, PartialEq, Serialize_enum_str)]
pub(crate) enum HeatPumpBackupControlType {
    None,
    TopUp,
    Substitute,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct HeatPumpTestDatum {
    pub(crate) design_flow_temperature: i32,
    pub(crate) test_letter: HeatPumpTestLetter,
    pub(crate) temperature_test: i32,
    pub(crate) temperature_source: Decimal,
    pub(crate) temperature_outlet: Decimal,
    pub(crate) capacity: Decimal,
    pub(crate) coefficient_of_performance: Decimal,
    pub(crate) degradation_coefficient: Decimal,
}

#[derive(Copy, Clone, Debug, Deserialize_enum_str, PartialEq, Serialize_enum_str)]
pub(crate) enum HeatPumpTestLetter {
    A,
    B,
    C,
    D,
    E,
    F,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn test_can_read_fake_file() {
        assert_eq!(PCDB_PRODUCTS.len(), 3);
    }

    #[rstest]
    fn test_find_product_by_reference() {
        assert_eq!(
            find_product_by_reference("HEATPUMP-SMALL")
                .unwrap()
                .model_name,
            "Small Heat Pump"
        );
        assert!(find_product_by_reference("HEATPUMP-UNKNOWN").is_none());
    }
}
