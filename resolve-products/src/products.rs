#![allow(dead_code)]

use crate::errors::ResolvePcdbProductsError;
use crate::ResolveProductsResult;
use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};
use serde_dynamo::from_item;
use serde_enum_str::{Deserialize_enum_str, Serialize_enum_str};
use serde_valid::Validate;
use smartstring::alias::String;
use std::collections::HashMap;

pub(crate) async fn find_products_for_references<'a>(
    product_references: &[String],
    dynamo_db_client: &DynamoDbClient,
) -> ResolveProductsResult<HashMap<String, Product>> {
    if product_references.is_empty() {
        return Ok(HashMap::new());
    }

    let results = dynamo_db_client
        .batch_get_item()
        .request_items(
            "products",
            KeysAndAttributes::builder()
                .keys(
                    product_references
                        .iter()
                        .map(|product_ref| {
                            (
                                std::string::String::from("id"),
                                AttributeValue::N(product_ref.to_string()),
                            )
                        })
                        .collect(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await;

    let results = match results {
        Ok(results) => results,
        Err(e) => {
            return Err(ResolvePcdbProductsError::AccessError(e.into()));
        }
    };

    let products = results.responses().unwrap().get("products").unwrap();

    if products.len() != product_references.len() {
        return Err(ResolvePcdbProductsError::UnknownProductReference(format!(
            "At least one product reference from the list ({}) could not be found within the PCDB store.",
            product_references.join(", "),
        )));
    }

    let products = products
        .iter()
        .cloned()
        .map(|item| {
            let product = from_item::<_, Product>(item);
            let product = match product {
                Ok(product) => product,
                Err(e) => return Err(e),
            };

            Ok((String::from(product.id.to_string()), product))
        })
        .collect::<Result<HashMap<_, _>, _>>();

    products.map_err(|e| ResolvePcdbProductsError::DeserializeError(e.into()))
}

#[derive(Debug, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Product {
    id: u32,
    brand_name: String,
    model_name: String,
    model_qualifier: Option<String>,
    first_year_of_manufacture: u16,
    final_year_of_manufacture: Option<YearOfManufacture>,
    #[serde(flatten)]
    pub(crate) technology: Technology,
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
#[serde(tag = "technologyType", rename_all = "camelCase")]
pub(crate) enum Technology {
    #[serde(
        alias = "air source heat pump",
        alias = "water source heat pump",
        alias = "ground source heat pump",
        alias = "exhaust air mixed heat pump",
        alias = "exhaust air mev heat pump",
        alias = "exhaust air mvhr heat pump"
    )]
    HeatPump {
        #[serde(rename = "EnergySupply")]
        energy_supply: String,
        source_type: HeatPumpSourceType,
        sink_type: HeatPumpSinkType,
        #[serde(rename = "backup_ctrl_type")]
        backup_control_type: HeatPumpBackupControlType,
        #[serde(deserialize_with = "deserialize_numeric_bool")]
        modulating_control: bool,
        minimum_modulation_rate_35: Decimal,
        minimum_modulation_rate_55: Decimal,
        #[serde(rename = "time_constant_onoff_operation")]
        time_constant_on_off_operation: i32,
        temp_return_feed_max: Decimal,
        temp_lower_operating_limit: Decimal,
        min_temp_diff_flow_return_for_hp_to_operate: i32,
        #[serde(
            rename = "var_flow_temp_ctrl_during_test",
            deserialize_with = "deserialize_numeric_bool"
        )]
        variable_temp_control: bool,
        power_heating_circ_pump: Option<Decimal>,
        power_heating_warm_air_fan: Option<Decimal>,
        power_source_circ_pump: Decimal,
        power_standby: Decimal,
        power_crankcase_heater: Decimal,
        power_off: Decimal,
        #[serde(rename = "power_max_backup")]
        power_maximum_backup: Option<Decimal>,
        #[serde(rename = "testData")]
        test_data: Vec<HeatPumpTestDatum>,
    },
}

// special deserialization logic so that booleans that are indicated by 0 or 1 are deserialized as true or false
pub(crate) fn deserialize_numeric_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let bool_int: u8 = Deserialize::deserialize(deserializer)?;
    Ok(bool_int == 1)
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
pub(crate) struct HeatPumpTestDatum {
    #[serde(rename = "design_flow_temp")]
    pub(crate) design_flow_temperature: i32,
    pub(crate) test_letter: HeatPumpTestLetter,
    #[serde(rename = "temp_test")]
    pub(crate) temperature_test: i32,
    #[serde(rename = "temp_source")]
    pub(crate) temperature_source: Decimal,
    #[serde(rename = "temp_outlet")]
    pub(crate) temperature_outlet: Decimal,
    pub(crate) capacity: Decimal,
    #[serde(rename = "cop")]
    pub(crate) coefficient_of_performance: Decimal,
    #[serde(rename = "degradation_coeff")]
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use rstest::*;
//
//     #[rstest]
//     fn test_find_product_by_reference() {
//         assert_eq!(
//             find_product_by_reference("HEATPUMP-SMALL")
//                 .unwrap()
//                 .model_name,
//             "Small Heat Pump"
//         );
//         assert!(find_product_by_reference("HEATPUMP-UNKNOWN").is_none());
//     }
// }
