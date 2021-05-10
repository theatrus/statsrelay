use crate::config;
use crate::processors;
use crate::statsd_proto;
use crate::statsd_proto::Event;
use std::convert::TryInto;

use smallvec::smallvec;

pub struct Normalizer {
    route: Vec<config::Route>,
}

impl Normalizer {
    pub fn new(route: &[config::Route]) -> Self {
        Normalizer {
            route: route.to_vec(),
        }
    }
}

impl processors::Processor for Normalizer {
    fn provide_statsd(&self, sample: &Event) -> Option<processors::Output> {
        let owned: Result<statsd_proto::Owned, _> = sample.try_into();
        owned
            .map(|inp| {
                let out = statsd_proto::convert::to_inline_tags(inp);
                processors::Output {
                    new_events: Some(smallvec![Event::Parsed(out)]),
                    route: self.route.as_ref(),
                }
            })
            .ok()
    }
}

#[cfg(test)]
pub mod test {
    use processors::Processor;
    use statsd_proto::Parsed;

    use super::*;

    #[test]
    fn make_normalizer() {
        let route = vec![config::Route {
            route_type: config::RouteType::Processor,
            route_to: "null".to_string(),
        }];

        let tn = Normalizer::new(&route);
        let pdu =
            statsd_proto::Pdu::parse(bytes::Bytes::from_static(b"foo.bar:3|c|#tags:value|@1.0"))
                .unwrap();
        let sample = Event::Pdu(pdu);
        let result = tn.provide_statsd(&sample).unwrap();

        let first_sample = &result.new_events.as_ref().unwrap()[0];
        let owned: statsd_proto::Owned = first_sample.try_into().unwrap();
        assert_eq!(owned.name(), b"foo.bar.__tags=value");
        assert_eq!(route, result.route);
    }
}
