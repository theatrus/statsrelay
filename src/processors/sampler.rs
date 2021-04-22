use crate::config;
use crate::processors;
use crate::statsd_proto::Sample;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid sampler configuration")]
    InvalidConfig,
}

pub struct Sampler {
    config: config::processor::Sampler,
}

impl Sampler {
    pub fn new(config: config::processor::Sampler) -> Result<Self, Error> {
        Ok(Sampler { config: config })
    }
}

impl processors::Processor for Sampler {
    fn provide_statsd(&self, sample: &Sample) -> Option<processors::Output> {
        None
    }
}
