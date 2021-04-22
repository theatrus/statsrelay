use crate::config;
use crate::statsd_proto::Sample;

pub mod cardinality;
pub mod sampler;
pub mod tag;

pub struct Output<'a> {
    /// Lists a new sample type returned if the processor has modified the
    /// sample in any way. If this is none but a route is set, downstream
    /// processors will be called with the original reference to the Sample
    pub new_sample: Option<Sample>,
    pub route: &'a [config::Route],
}
pub trait Processor {
    fn provide_statsd(&self, sample: &Sample) -> Option<Output>;
}
