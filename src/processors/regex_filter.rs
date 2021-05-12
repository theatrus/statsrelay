use regex::RegexSet;

use super::{Output, Processor};
use crate::stats;
use crate::{config::processor, statsd_proto::Event};
use crate::{config::Route, statsd_proto::Parsed};

pub struct RegexFilter {
    allow: Option<RegexSet>,
    remove: Option<RegexSet>,
    route: Vec<Route>,

    counter_remove: stats::Counter,
}

impl RegexFilter {
    pub fn new(
        scope: stats::Scope,
        from_config: &processor::RegexFilter,
    ) -> Result<Self, regex::Error> {
        let allow = from_config.allow.as_ref().map(RegexSet::new).transpose()?;
        let remove = from_config.remove.as_ref().map(RegexSet::new).transpose()?;
        Ok(RegexFilter {
            allow,
            remove,
            route: from_config.route.clone(),
            counter_remove: scope.counter("removed").unwrap(),
        })
    }
}

impl Processor for RegexFilter {
    fn provide_statsd(&self, event: &Event) -> Option<Output> {
        let name = std::str::from_utf8(match event {
            Event::Parsed(parsed) => parsed.id().name.as_ref(),
            Event::Pdu(pdu) => pdu.name(),
        })
        .ok()?;
        if let Some(allow) = &self.allow {
            if !allow.is_match(name) {
                self.counter_remove.inc();
                return None;
            }
        }
        if let Some(remove) = &self.remove {
            if remove.is_match(name) {
                self.counter_remove.inc();
                return None;
            }
        }
        Some(Output {
            new_events: None,
            route: self.route.as_ref(),
        })
    }
}

#[cfg(test)]
pub mod test {

    use super::*;

    #[test]
    fn build_filter() {
        let c = processor::RegexFilter {
            route: vec![],
            remove: Some(vec![r"^hello.*".to_owned(), r"^goodbye.*".to_owned()]),
            allow: None,
        };
        let sink = stats::Collector::default();
        let scope = sink.scope("prefix");
        let filter = RegexFilter::new(scope, &c).unwrap();

        let event1 = Event::Pdu(
            crate::statsd_proto::Pdu::parse(bytes::Bytes::from_static(b"hello.world:c|1")).unwrap(),
        );
        let event2 = Event::Pdu(
            crate::statsd_proto::Pdu::parse(bytes::Bytes::from_static(b"goodbye.world:c|1"))
                .unwrap(),
        );
        let event3 = Event::Pdu(
            crate::statsd_proto::Pdu::parse(bytes::Bytes::from_static(b"pineapples:c|1")).unwrap(),
        );

        assert!(filter.provide_statsd(&event1).is_none(), "should remove");
        assert!(filter.provide_statsd(&event2).is_none(), "should remove");
        assert!(
            filter.provide_statsd(&event3).is_some(),
            "should not remove"
        );
    }
}
