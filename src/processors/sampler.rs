use crate::config;
use crate::backends::StatsdSample;
use crate::processors;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid sampler configuration")]
    InvalidConfig,
}

pub struct Sampler {

}

impl Sampler {
    fn new(config: config::Processor) -> Result<Self, Error> {
        Ok(Sampler {

        })
    }
}

impl processors::Processor for Sampler {
    fn provide_statsd(&self, sample: &StatsdSample) -> Option<processors::Output> {
        None
    }
}
