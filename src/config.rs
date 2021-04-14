use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::convert::{AsRef, TryFrom, TryInto};
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum RouteType {
    Statsd,
    Processor,
}

impl TryFrom<&str> for RouteType {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "statsd" => Ok(RouteType::Statsd),
            "processor" => Ok(RouteType::Processor),
            _ => Err(Error::UnknownRouteType(value.to_string())),
        }
    }
}

impl From<&RouteType> for &str {
    fn from(t: &RouteType) -> Self {
        match t {
            RouteType::Statsd => "statsd",
            RouteType::Processor => "processor",
        }
    }
}

impl fmt::Display for RouteType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: &str = self.into();
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone)]
pub struct Route {
    pub route_type: RouteType,
    pub route_to: String,
}

impl fmt::Display for Route {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.route_type, self.route_to)
    }
}

impl<'de> Deserialize<'de> for Route {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        let parts: Vec<&str> = s.split(':').collect();
        if let [ty, to] = &parts[..] {
            Ok(Route {
                route_type: (*ty).try_into().map_err(serde::de::Error::custom)?,
                route_to: (*to).into(),
            })
        } else {
            Err(Error::MalformedRoute(s.to_string())).map_err(serde::de::Error::custom)
        }
    }
}

impl Serialize for Route {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(format!("{}:{}", self.route_type, self.route_to).as_str())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Processor {
    Sampler {
        counter_cardinality: Option<u32>,
        sampling_threshold: Option<u32>,
        sampling_window: Option<u32>,

        gauge_cardinality: Option<u32>,
        gauge_sampling_threshold: Option<u32>,
        gauge_sampling_window: Option<u32>,

        timer_cardinality: Option<u32>,
        timer_sampling_threshold: Option<u32>,
        timer_sampling_window: Option<u32>,
        reservoir_size: Option<u32>,

        route: Vec<Route>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Processors {
    pub processors: HashMap<String, Processor>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StatsdBackendConfig {
    #[serde(default)]
    pub shard_map: Vec<String>,
    pub shard_map_source: Option<String>,
    pub suffix: Option<String>,
    pub prefix: Option<String>,
    pub input_blocklist: Option<String>,
    pub input_filter: Option<String>,
    pub max_queue: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StatsdServerConfig {
    pub bind: String,
    pub socket: Option<String>,
    pub route: Vec<Route>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StatsdConfig {
    pub servers: HashMap<String, StatsdServerConfig>,
    pub backends: HashMap<String, StatsdBackendConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DiscoveryTransform {
    Format { pattern: String },
    Repeat { count: u32 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct S3DiscoverySource {
    pub bucket: String,
    pub key: String,
    pub interval: u32,
    pub transforms: Option<Vec<DiscoveryTransform>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PathDiscoverySource {
    pub path: String,
    pub interval: u32,
    pub transforms: Option<Vec<DiscoveryTransform>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DiscoverySource {
    StaticFile(PathDiscoverySource),
    S3(S3DiscoverySource),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Discovery {
    pub sources: HashMap<String, DiscoverySource>,
}

impl Default for Discovery {
    fn default() -> Self {
        Discovery {
            sources: HashMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AdminConfig {
    pub port: u16,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub admin: Option<AdminConfig>,
    pub statsd: StatsdConfig,
    pub discovery: Option<Discovery>,
    pub processor: Option<Processors>,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("could not locate discovery source {0}")]
    UnknownDiscoverySource(String),
    #[error("malformed route {0}")]
    MalformedRoute(String),
    #[error("invalid route type {0}")]
    UnknownRouteType(String),
    #[error("invalid routing destination {0}")]
    UnknownRoutingDestination(Route),
}

fn check_config_route(config: &Config) -> Result<(), Error> {
    for (_, statsd) in config.statsd.servers.iter() {
        for route in statsd.route.iter() {
            match route.route_type {
                RouteType::Statsd => {
                    config
                        .statsd
                        .backends
                        .get(route.route_to.as_str())
                        .ok_or(Error::UnknownRoutingDestination(route.clone()))
                        .map(|_| ())?;
                }
                RouteType::Processor => {
                    if let Some(procs) = &config.processor {
                        procs
                            .processors
                            .get(route.route_to.as_str())
                            .ok_or(Error::UnknownRoutingDestination(route.clone()))
                            .map(|_| ())?;
                    } else {
                        return Err(Error::UnknownRoutingDestination(route.clone()));
                    }
                }
            }
        }
    }
    Ok(())
}

fn check_config_discovery(config: &Config, discovery: &Discovery) -> anyhow::Result<()> {
    for (_, statsd_dupl) in config.statsd.backends.iter() {
        if let Some(source) = &statsd_dupl.shard_map_source {
            if let None = discovery.sources.get(source) {
                return Err(Error::UnknownDiscoverySource(source.clone()).into());
            }
        }
    }
    Ok(())
}

fn check_config(config: &Config) -> anyhow::Result<()> {
    let default = Discovery::default();
    let discovery = &config.discovery.as_ref().unwrap_or(&default);
    // Every reference to a shard_map needs a reference to a valid discovery block
    check_config_discovery(config, discovery)?;
    check_config_route(config)?;
    Ok(())
}

pub fn load(path: &str) -> anyhow::Result<Config> {
    let input = std::fs::read_to_string(path)?;
    let config: Config = serde_json::from_str(input.as_ref())?;
    // Perform some high level validation
    check_config(&config)?;
    Ok(config)
}

#[cfg(test)]
pub mod test {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn load_example_config() {
        let config = r#"
        {
            "statsd": {
                "servers": {
                    "default":
                        {
                            "bind": "127.0.0.1:BIND_STATSD_PORT",
                            "route": ["statsd:test1"]
                        }
                },
                "backends": {
                    "test1":
                       {
                            "prefix": "test-1.",
                            "shard_map": [
                                "127.0.0.1:SEND_STATSD_PORT"
                            ],
                            "suffix": ".suffix"
                        },
                "mapsource":
                        {
                            "input_filter": "^(?=dontmatchme)",
                            "prefix": "test-2.",
                            "shard_map_source": "my_s3"
                        }
                }
            },
            "discovery": {
                "sources": {
                    "file": {
                        "type":"static_file",
                        "path":"/tmp/file",
                        "interval":5
                    },
                    "my_s3": {
                        "type": "s3",
                        "bucket": "foo",
                        "key": "bar",
                        "interval": 3,
                        "transforms": [
                            {
                                "type": "repeat",
                                "count": 3
                            },
                            {
                                "type": "format",
                                "pattern": "{}:123"
                            }
                        ]
                    }
                }
            }
        }
        "#;
        let mut tf = NamedTempFile::new().unwrap();
        tf.write_all(config.as_bytes()).unwrap();
        let config = load(tf.path().to_str().unwrap()).unwrap();
        // Check servers
        let default_server = config.statsd.servers.get("default").unwrap();
        assert_eq!(
            default_server.bind,
            "127.0.0.1:BIND_STATSD_PORT".to_string()
        );
        // Check discovery
        let discovery = config.discovery.unwrap();
        assert_eq!(2, discovery.sources.len());
        let s3_source = discovery.sources.get("my_s3").unwrap();
        match s3_source {
            DiscoverySource::S3(source) => {
                assert!(source.bucket == "foo");
            }
            _ => panic!("not an s3 source"),
        };
    }
}
