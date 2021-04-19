use crate::backends::StatsdSample;
use crate::config;
use crate::processors;
use crate::statsd_proto;
use std::convert::TryInto;

pub struct TagNormalizer {
    route: Vec<config::Route>,
}

impl TagNormalizer {
    pub fn new(route: &[config::Route]) -> Self {
        TagNormalizer {
            route: route.to_vec(),
        }
    }
}

impl processors::Processor for TagNormalizer {
    fn provide_statsd(&self, sample: &StatsdSample) -> Option<processors::Output> {
        let owned: Result<statsd_proto::Owned, _> = sample.try_into();
        owned
            .map(|inp| {
                let out = statsd_proto::convert::to_inline_tags(inp);
                processors::Output {
                    sample: StatsdSample::Parsed(out),
                    route: self.route.clone(),
                }
            })
            .ok()
    }
}
