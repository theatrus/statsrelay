use crate::config;
use crate::processors;
use crate::statsd_proto::Sample;
use thiserror::Error;
use parking_lot::Mutex;

use std::{time::Instant};

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid sampler configuration")]
    InvalidConfig,
}


struct Counter {
    value: f64,
    samples: f64,
    last_updated: Instant
}

struct Timer {
    values: Mutex<Vec<f64>>,
    last_updated: Instant
}

struct Gauge {
    value: f64,
    last_updated: Instant
}

pub struct Sampler {
    config: config::processor::Sampler,
}

impl Sampler {
    pub fn new(config: &config::processor::Sampler) -> Result<Self, Error> {
        Ok(Sampler { config: config.clone() })
    }
}

impl processors::Processor for Sampler {
    fn provide_statsd(&self, sample: &Sample) -> Option<processors::Output> {
        None
    }
}
