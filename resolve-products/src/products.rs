#![allow(dead_code)]

use crate::ResolveProductsResult;
use crate::errors::ResolvePcdbProductsError;
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};
use serde_dynamo::from_item;
use serde_enum_str::{Deserialize_enum_str, Serialize_enum_str};
use serde_json::{Number, Value};
use serde_valid::Validate;
use smartstring::alias::String;
use std::collections::HashMap;

pub(crate) async fn find_products_for_references(
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

    products.map_err(ResolvePcdbProductsError::DeserializeError)
}

#[derive(Debug, Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Product {
    id: u32,
    brand_name: String,
    model_name: String,
    model_qualifier: Option<String>,
    #[serde(flatten)]
    pub(crate) technology: Technology,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "technologyType", rename_all = "camelCase")]
pub(crate) enum Technology {
    #[serde(
        alias = "AirSourceHeatPump",
        alias = "WaterSourceHeatPump",
        alias = "BoosterHeatPump",
        alias = "GroundSourceHeatPump",
        alias = "ExhaustAirMixedHeatPump",
        alias = "ExhaustAirMevHeatPump",
        alias = "ExhaustAirMvhrHeatPump",
        alias = "HybridHeatPump"
    )]
    HeatPump {
        source_type: HeatPumpSourceType,
        sink_type: HeatPumpSinkType,
        #[serde(rename = "backup_ctrl_type")]
        backup_control_type: HeatPumpBackupControlType,
        #[serde(deserialize_with = "deserialize_numeric_bool_or_bool")]
        modulating_control: bool,
        #[serde(rename = "min_modulation_rate_35")]
        minimum_modulation_rate_35: Option<Decimal>,
        #[serde(rename = "min_modulation_rate_55")]
        minimum_modulation_rate_55: Option<Decimal>,
        #[serde(rename = "time_constant_onoff_operation")]
        time_constant_on_off_operation: i32,
        temp_return_feed_max: Decimal,
        temp_lower_operating_limit: Decimal,
        min_temp_diff_flow_return_for_hp_to_operate: i32,
        #[serde(rename = "var_flow_temp_ctrl_during_test")]
        variable_temp_control: bool,
        power_heating_circ_pump: Option<Decimal>,
        power_heating_warm_air_fan: Option<Decimal>,
        power_source_circ_pump: Decimal,
        power_standby: Decimal,
        power_crankcase_heater: Decimal,
        power_off: Decimal,
        #[serde(rename = "power_max_backup")]
        power_maximum_backup: Option<Decimal>,
        #[serde(rename = "test_data_EN14825")]
        test_data: Vec<HeatPumpTestDatum>,
    },
    #[serde(
        alias = "RegularBoiler",
        alias = "CombiBoiler",
        rename_all = "camelCase"
    )]
    Boiler {
        fuel: FuelType,
        fuel_aux: FuelType,
        rated_power: Decimal,
        efficiency_full_load: Decimal,
        efficiency_part_load: Decimal,
        boiler_location: BoilerLocation,
        modulation_load: Decimal,
        electricity_circ_pump: Decimal,
        electricity_part_load: Decimal,
        electricity_full_load: Decimal,
        electricity_standby: Decimal,
    },
    #[serde(rename = "HeatBatteryPCM")]
    HeatBatteryPcm {
        #[serde(rename = "A")]
        a: Decimal,
        #[serde(rename = "B")]
        b: Decimal,
        inlet_diameter_mm: Decimal,
        electricity_circ_pump: Decimal,
        electricity_standby: Decimal,
        flow_rate_l_per_min: Decimal,
        #[serde(rename = "heat_storage_kJ_per_K_above_Phase_transition")]
        heat_storage_kj_per_k_above_phase_transition: Decimal,
        #[serde(rename = "heat_storage_kJ_per_K_below_Phase_transition")]
        heat_storage_kj_per_k_below_phase_transition: Decimal,
        #[serde(rename = "heat_storage_kJ_per_K_during_Phase_transition")]
        heat_storage_kj_per_k_during_phase_transition: Decimal,
        max_rated_losses: Decimal,
        max_temperature: Decimal,
        phase_transition_temperature_upper: Decimal,
        phase_transition_temperature_lower: Decimal,
        rated_charge_power: Decimal,
        simultaneous_charging_and_discharging: bool,
        #[serde(rename = "velocity_in_HEX_tube_at_1_l_per_min_m_per_s")]
        velocity_in_hex_tube_at_1_l_per_min_m_per_s: Decimal,
    },
    HeatBatteryDryCore {
        fuel: FuelType,
        electricity_circ_pump: Decimal,
        electricity_standby: Decimal,
        /// Charging power (kW)
        pwr_in: Decimal,
        /// Rated instantaneous power output (kW)
        rated_power_instant: Decimal,
        /// Heat storage capacity (kWh)
        heat_storage_capacity: Decimal,
        /// Fan power (W)
        fan_pwr: Decimal,
        #[serde(rename = "testData")]
        test_data: Vec<HeatBatteryPcmTestDatum>,
        // TODO: state_of_charge_init needs to come from somewhere = account for this
    },
    #[serde(rename = "HeatInterfaceUnit")]
    Hiu {
        // TODO: complete fields
    },
    #[serde(rename = "InstantaneousWwhrSystem")]
    Wwhrs {
        number_of_flow_rates: usize,
        /// Utilisation factor for system (fraction between 0 and 1)
        // this field can appear in a field from the PCDB with this typo ("utililsation_factor"), until we are told otherwise
        #[serde(alias = "utililsation_factor")]
        utilisation_factor: Decimal,
        test_data: Vec<WwhrsTestDatum>,
    },
    #[serde(rename = "StorageHeater")]
    ElectricStorageHeater {
        /// Maximum heat storage capacity in kWh
        storage_capacity: Decimal,
        fuel: FuelType,
        pwr_in: Decimal,
        /// Output power from in-built boost heater in kW
        rated_power_instant: Decimal,
        air_flow_type: StorageHeaterAirFlowType,
        /// Rated power of fan in W. 0 if no fan
        fan_pwr: Decimal,
        /// Proportion of heat output that is convective (0 to 1)
        frac_convective: Decimal,
        #[serde(rename = "testData")]
        test_data: Vec<ElectricStorageHeaterTestDatum>,
    },
    #[serde(rename = "ConvectorRadiator")]
    Radiator {
        /// Exponent used in heat output calculation formula
        n: Decimal,
        /// Convective heat output fraction (unitless)
        frac_convective: Decimal,
        /// Thermal mass of the radiator, measured in kilowatt hours per kelvin per meter length (kWh/K)/m
        thermal_mass_per_m: Decimal,
        /// C-value for the radiator in Watt per meter (W/m)
        c: Decimal,
    },
    #[serde(rename = "UnderFloorHeating")]
    UnderfloorHeating {
        /// System performance factor determined according to BEAMA guidance in W/m²K (up to 6 chs; eg xx.xxx)
        system_performance_factor: Decimal,
        /// Equivalent specific thermal mass of system determined according to BEAMA guidance in Kj/m²K (up to 6 chs; eg xxx.xx)
        equivalent_specific_thermal_mass: Decimal,
        /// Convective heat output fraction (unitless)
        frac_convective: Decimal,
    },
    #[serde(rename = "FanCoils")]
    FanCoil {
        /// The number of fan speeds (n) for which data are provided in the record (maximum 5)
        number_of_fan_speeds: usize,
        #[serde(rename = "number_of_test_point_deltaT")]
        number_of_test_point_delta_t: usize,
        /// fraction of heat that comes from convective
        frac_convective: Decimal,
        #[serde(rename = "testData")]
        test_data: Vec<FanCoilTestDatum>,
    },
}

// special deserialization logic so that booleans that are indicated by 0 or 1 are deserialized as true or false
pub(crate) fn deserialize_numeric_bool_or_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Bool(b) => Ok(b),
        Value::Number(bool_int) => Ok(bool_int == Number::from(1)),
        _ => Err(serde::de::Error::custom("expected boolean or integer")),
    }
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

#[derive(Debug, Deserialize_enum_str, Serialize_enum_str)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FuelType {
    #[serde(rename(serialize = "mains elec"))]
    Electricity,
    #[serde(rename(serialize = "mains gas"))]
    MainsGas,
    #[serde(rename = "LPG_bulk")]
    LpgBulk,
    #[serde(rename = "LPG_bottled")]
    LpgBottled,
    #[serde(rename = "LPG_condition_11F")]
    LpgCondition11F,
    HeatingOil,
}

#[derive(Clone, Copy, Debug, Deserialize_enum_str, Serialize_enum_str)]
#[serde(rename_all = "lowercase")]
pub(crate) enum BoilerLocation {
    Internal,
    External,
    Unknown,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HeatBatteryPcmTestDatum {
    /// Charge level (e.g., percentage or step index)
    charge_level: Decimal,
    /// Minimum output (kW)
    dry_core_min_output: Decimal,
    /// Maximum output (kW)
    dry_core_max_output: Decimal,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WwhrsTestDatum {
    flow_rate: Decimal,
    /// Heat recovery efficiency of Instantaneous WWHR system (%).
    efficiency: Decimal,
    system_type: WwhrsSystemType,
}

#[derive(Clone, Copy, Debug, Deserialize_enum_str)]
pub(crate) enum WwhrsSystemType {
    A,
    B,
    C,
}

#[derive(Clone, Copy, Debug, Deserialize_enum_str)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum StorageHeaterAirFlowType {
    DamperOnly,
    FanAssisted,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ElectricStorageHeaterTestDatum {
    /// Test point number (0 to 100 (TODO: ??? maybe to 1) during heat discharge test)
    test_point: Decimal,
    /// Minimum heat output at test points (0 to 100) during heat discharge test, in kW
    dry_core_min_output: Decimal,
    /// Maximum heat output at test points (0 to 100) during heat discharge test, in kW
    dry_core_max_output: Decimal,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FanCoilTestDatum {
    /// fan speeds (n) for which data are provided in the record
    fan_speed: Decimal,
    /// DeltaT (difference between mean feed water temperature and room air temperature) at test point heat output test, in K., up to 6 chs, e.g. xxxx.x
    temperature_diff: Decimal,
    /// power_output at deltaT and fan speed, in kW. up to 5 chs, e.g. xx.xx
    power_output: Decimal,
    /// Electrical power consumed by fan at fan different speeds in W., up to 5 chs, e.g. xxx.x
    #[serde(rename = "fan_power_W")]
    fan_power_w: Decimal,
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
