use super::{Processor, Output};
use super::super::statsd_proto::Sample;
use super::super::config;

pub struct Cardinality{
    route: Vec<config::Route>
}

impl Cardinality {
    pub fn new(from_config: &config::processor::Cardinality) -> Self {
        Cardinality { route: from_config.route.clone() }
    }
}

impl Processor for Cardinality {
    
    fn provide_statsd(&self, sample: &Sample) -> Option<Output> {
        Some(Output {
            route: self.route.as_ref(),
            new_sample: None,
        })
    }
}
