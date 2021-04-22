use crate::config;
use crate::statsd_proto::Sample;

pub mod sampler;
pub mod tag;

pub struct Output {
    pub sample: Sample,
    pub route: Vec<config::Route>,
}
pub trait Processor {
    fn provide_statsd(&self, sample: &Sample) -> Option<Output>;
}
