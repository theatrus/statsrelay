use crate::backends::StatsdSample;
use crate::config;

pub mod sampler;
pub mod tag;

pub struct Output {
    pub sample: StatsdSample,
    pub route: Vec<config::Route>,
}
pub trait Processor {
    fn provide_statsd(&self, sample: &StatsdSample) -> Option<Output>;
}
