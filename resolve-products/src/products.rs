#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use serde_enum_str::{Deserialize_enum_str, Serialize_enum_str};
use serde_valid::Validate;
use std::collections::HashMap;
use std::sync::LazyLock;

static PCDB_PRODUCTS: LazyLock<HashMap<String, Product>> =
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
struct Product<'a> {
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
    technology: Technology<'a>,
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
enum Technology<'a> {
    #[serde(rename = "Air Source Heat Pump", rename_all = "camelCase")]
    AirSourceHeatPump {
        fuel: &'a str,
        source_type: HeatPumpSourceType,
        sink_type: HeatPumpSinkType,
        backup_control_type: HeatPumpBackupControlType,
        modulating_control: bool,
        #[serde(rename = "standardRatingCapacity20C")]
        standard_rating_capacity_20c: Option<&'a str>,
        #[serde(rename = "standardRatingCapacity35C")]
        standard_rating_capacity_35c: Option<&'a str>,
        #[serde(rename = "standardRatingCapacity55C")]
        standard_rating_capacity_55c: Option<&'a str>,
        minimum_modulation_rate: &'a str,
        variable_temp_control: bool,
        power_standby: &'a str,
        power_crankcase_heater: Option<&'a str>,
        power_off: Option<&'a str>,
        power_maximum_backup: Option<&'a str>,
        test_data: Vec<HeatPumpTestDatum>,
    },
}

#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
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

#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) enum HeatPumpSinkType {
    Water,
    Air,
    Glycol25,
}

#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) enum HeatPumpBackupControlType {
    None,
    TopUp,
    Substitute,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HeatPumpTestDatum {
    design_flow_temperature: i32,
    test_condition: HeatPumpTestLetter,
    test_condition_temperature: i32,
    inlet_temperature: f64,
    outlet_temperature: f64,
    heating_capacity: f64,
    coefficient_of_performance: f64,
    degradation_coefficient: f64,
}

#[derive(Debug, Deserialize_enum_str, Serialize_enum_str)]
enum HeatPumpTestLetter {
    A,
    B,
    C,
    D,
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
}
