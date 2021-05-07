use repc::configuration::Configuration as RepcConfiguration;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Configuration {
    pub repc: RepcConfiguration,
}
