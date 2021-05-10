use super::backends::Backends;
use crate::config;
use crate::statsd_proto::Event;
use smallvec::SmallVec;

pub mod cardinality;
pub mod sampler;
pub mod tag;

pub struct Output<'a> {
    /// Lists of new events returned if the processor has modified the
    /// sample in any way. If this is none but a route is set, downstream
    /// processors will be called with the original reference to the Sample
    pub new_events: Option<SmallVec<[Event; 4]>>,
    pub route: &'a [config::Route],
}
pub trait Processor {
    /// Tick is designed for processors to do any internal housekeeping. A copy
    /// of the called time is provided for mocking, and a reference to the
    /// Backends structure is provided to re-inject messages into processor
    /// framework if desired.
    fn tick(&self, _time: std::time::SystemTime, _backends: &Backends) {}
    fn provide_statsd(&self, sample: &Event) -> Option<Output>;
}
