//! Describes the refinery core configuration
//!
//!

use toml;
use serde::{Serialize, Deserialize};

/// A small utility which serialises a config
///
/// Use this function instead of toml::to_string directly
/// to avoid having to use the toml crate in all places
/// that use this function
pub fn serialize(cfg: &Config) -> Option<String> {
    return match toml::to_string(cfg) {
        Ok(s) => Some(s),
        _ => None,
    };
}

/// A small utility which deserialises a config
///
/// Use this function instead of toml::from_str directly
/// to avoid having to use the toml crate in all places
/// that use this function
pub fn deserialize(s: String) -> Option<Config> {
    return match toml::from_str(&s) {
        Ok(cfg) => Some(cfg),
        _ => None,
    };
}

/// This config format **will** change.
/// Please don't get married to it.
/// 
/// And expect it to break 🤷
#[derive(Serialize, Deserialize)]
pub struct Config {
    pub main: Main,
}

#[derive(Serialize, Deserialize)]
pub struct Main {
    pub env: ConfigEnvType,
    pub db_type: String,
    pub db_path: String,
    pub db_user: String,
    pub db_pw: String,
}

#[derive(Serialize, Deserialize)]
pub enum ConfigEnvType {
    Develop,
    Staging,
    Production,
}
