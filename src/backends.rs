use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::convert::TryInto;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use parking_lot::RwLock;
use regex::bytes::RegexSet;
use thiserror::Error;

use crate::discovery;
use crate::shard::{statsrelay_compat_hash, Ring};
use crate::stats;
use crate::statsd_client::StatsdClient;
use crate::statsd_proto;
use crate::{config, processors};

use log::warn;

/// An either type representing one of the two forms of statsd protocols
///
/// In order to allow backends to operate on different levels of protocol
/// decoding (fully decoded or just tokenized), backends take a Sample enum
/// which represent either of the two formats, with easy conversions between
/// them.
///
/// # Examples
/// ```
/// use statsrelay::statsd_proto;
/// use bytes::Bytes;
/// use statsrelay::backends::StatsdSample;
///
/// let input = Bytes::from_static(b"foo.bar:3|c|#tags:value|@1.0");
/// let sample = &StatsdSample::PDU(statsd_proto::PDU::parse(input).unwrap());
/// let parsed: statsd_proto::PDU = sample.into();
/// ```
#[derive(Clone, Debug)]
pub enum StatsdSample {
    PDU(statsd_proto::PDU),
    Parsed(statsd_proto::Owned),
}

impl TryFrom<&StatsdSample> for statsd_proto::Owned {
    type Error = statsd_proto::ParseError;
    fn try_from(inp: &StatsdSample) -> Result<Self, Self::Error> {
        match inp {
            StatsdSample::Parsed(p) => Ok(p.to_owned()),
            StatsdSample::PDU(pdu) => pdu.try_into(),
        }
    }
}

impl TryFrom<StatsdSample> for statsd_proto::Owned {
    type Error = statsd_proto::ParseError;
    fn try_from(inp: StatsdSample) -> Result<Self, Self::Error> {
        match inp {
            StatsdSample::Parsed(p) => Ok(p),
            StatsdSample::PDU(pdu) => pdu.try_into(),
        }
    }
}

impl From<StatsdSample> for statsd_proto::PDU {
    fn from(inp: StatsdSample) -> Self {
        match inp {
            StatsdSample::PDU(pdu) => pdu,
            StatsdSample::Parsed(p) => p.into(),
        }
    }
}

impl From<&StatsdSample> for statsd_proto::PDU {
    fn from(inp: &StatsdSample) -> Self {
        match inp {
            StatsdSample::PDU(pdu) => pdu.clone(),
            StatsdSample::Parsed(p) => p.into(),
        }
    }
}

struct StatsdBackend {
    conf: config::StatsdBackendConfig,
    ring: Ring<StatsdClient>,
    input_filter: Option<RegexSet>,
    warning_log: AtomicU64,
    backend_sends: stats::Counter,
    backend_fails: stats::Counter,
}

impl StatsdBackend {
    fn new(
        stats: stats::Scope,
        conf: &config::StatsdBackendConfig,
        client_ref: Option<&StatsdBackend>,
        discovery_update: Option<&discovery::Update>,
    ) -> anyhow::Result<Self> {
        let mut filters: Vec<String> = Vec::new();

        // This is ugly, sorry
        if conf.input_blocklist.is_some() {
            filters.push(conf.input_blocklist.as_ref().unwrap().clone());
        }
        if conf.input_filter.is_some() {
            filters.push(conf.input_filter.as_ref().unwrap().clone());
        }
        let input_filter = if filters.len() > 0 {
            Some(RegexSet::new(filters).unwrap())
        } else {
            None
        };

        let mut ring: Ring<StatsdClient> = Ring::new();

        // Use the same backend for the same endpoint address, caching the lookup locally
        let mut memoize: HashMap<String, StatsdClient> =
            client_ref.map_or_else(|| HashMap::new(), |b| b.clients());

        let use_endpoints = discovery_update
            .map(|u| u.sources())
            .unwrap_or(&conf.shard_map);
        for endpoint in use_endpoints {
            if let Some(client) = memoize.get(endpoint) {
                ring.push(client.clone())
            } else {
                let client = StatsdClient::new(
                    stats.scope("statsd_client"),
                    endpoint.as_str(),
                    conf.max_queue.unwrap_or(100000) as usize,
                );
                memoize.insert(endpoint.clone(), client.clone());
                ring.push(client);
            }
        }

        let backend = StatsdBackend {
            conf: conf.clone(),
            ring: ring,
            input_filter: input_filter,
            warning_log: AtomicU64::new(0),
            backend_fails: stats.counter("backend_fails").unwrap(),
            backend_sends: stats.counter("backend_sends").unwrap(),
        };

        Ok(backend)
    }

    // Capture the old ring contents into a memoization map by endpoint,
    // letting us re-use any old client connections and buffers. Note we
    // won't start tearing down connections until the memoization buffer and
    // old ring are both dropped.
    fn clients(&self) -> HashMap<String, StatsdClient> {
        let mut memoize: HashMap<String, StatsdClient> = HashMap::new();
        for i in 0..self.ring.len() {
            let client = self.ring.pick_from(i as u32);
            memoize.insert(String::from(client.endpoint()), client.clone());
        }
        memoize
    }

    fn provide_statsd(&self, input: &StatsdSample) {
        let pdu: statsd_proto::PDU = input.into();
        if !self
            .input_filter
            .as_ref()
            .map_or(true, |inf| inf.is_match(pdu.name()))
        {
            return;
        }

        let ring_read = &self.ring;
        let code = match ring_read.len() {
            0 => return, // In case of nothing to send, do nothing
            1 => 1 as u32,
            _ => statsrelay_compat_hash(&pdu),
        };
        let client = ring_read.pick_from(code);
        let sender = client.sender();

        // Assign prefix and/or suffix
        let pdu_clone = if self.conf.prefix.is_some() || self.conf.suffix.is_some() {
            pdu.with_prefix_suffix(
                self.conf
                    .prefix
                    .as_ref()
                    .map(|p| p.as_bytes())
                    .unwrap_or_default(),
                self.conf
                    .suffix
                    .as_ref()
                    .map(|s| s.as_bytes())
                    .unwrap_or_default(),
            )
        } else {
            pdu
        };
        match sender.try_send(pdu_clone) {
            Err(_e) => {
                self.backend_fails.inc();
                let count = self
                    .warning_log
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if count % 1000 == 0 {
                    warn!(
                        "error pushing to queue full (endpoint {}, total failures {})",
                        client.endpoint(),
                        count
                    );
                }
            }
            Ok(_) => {
                self.backend_sends.inc();
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum BackendError {
    #[error("Index not valid for backend {0}")]
    InvalidIndex(usize),
}

struct BackendsInner {
    statsd: HashMap<String, StatsdBackend>,
    processors: HashMap<String, Box<dyn processors::Processor + Send + Sync>>,
    stats: stats::Scope,
}

impl BackendsInner {
    fn new(stats: stats::Scope) -> Self {
        BackendsInner {
            statsd: HashMap::new(),
            processors: HashMap::new(),
            stats: stats,
        }
    }

    fn replace_statsd_backend(
        &mut self,
        name: &String,
        c: &config::StatsdBackendConfig,
        discovery_update: Option<&discovery::Update>,
    ) -> anyhow::Result<()> {
        let previous = self.statsd.get(name);
        let backend = StatsdBackend::new(
            self.stats.scope(name.as_str()),
            c,
            previous,
            discovery_update,
        )?;
        self.statsd.insert(name.clone(), backend);
        Ok(())
    }

    fn len(&self) -> usize {
        self.statsd.len()
    }

    fn remove_statsd_backend(&mut self, name: &String) -> anyhow::Result<()> {
        self.statsd.remove(name);
        Ok(())
    }

    fn backend_names(&self) -> HashSet<&String> {
        self.statsd.keys().collect()
    }

    fn provide_statsd(&self, pdu: &StatsdSample, route: &[config::Route]) {
        let _r: Vec<_> = route
            .iter()
            .map(|dest| match dest.route_type {
                config::RouteType::Statsd => {
                    self.statsd
                        .get(dest.route_to.as_str())
                        .map(|backend| backend.provide_statsd(pdu));
                }
                config::RouteType::Processor => {
                    self.processors
                        .get(dest.route_to.as_str())
                        .map(|proc| proc.provide_statsd(pdu))
                        .flatten()
                        .map(|chain| self.provide_statsd(&chain.sample, chain.route.as_ref()));
                }
            })
            .collect();
    }
}

///
/// Backends provides a cloneable container for various protocol backends,
/// handling logic like sharding, sampling, and other detectors.
///
#[derive(Clone)]
pub struct Backends {
    inner: Arc<RwLock<BackendsInner>>,
}

impl Backends {
    pub fn new(stats: stats::Scope) -> Self {
        Backends {
            inner: Arc::new(RwLock::new(BackendsInner::new(stats))),
        }
    }

    pub fn replace_statsd_backend(
        &self,
        name: &String,
        c: &config::StatsdBackendConfig,
        discovery_update: Option<&discovery::Update>,
    ) -> anyhow::Result<()> {
        self.inner
            .write()
            .replace_statsd_backend(name, c, discovery_update)
    }

    pub fn remove_statsd_backend(&self, name: &String) -> anyhow::Result<()> {
        self.inner.write().remove_statsd_backend(name)
    }

    pub fn backend_names(&self) -> HashSet<String> {
        self.inner
            .read()
            .backend_names()
            .iter()
            .map(|s| (*s).clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn provide_statsd_pdu(&self, pdu: statsd_proto::PDU, route: &[config::Route]) {
        self.inner
            .read()
            .provide_statsd(&StatsdSample::PDU(pdu), route)
    }
}

#[cfg(test)]
pub mod test {

    use super::*;
    use crate::processors;
    use crate::statsd_proto::Parsed;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    struct AssertProc<T>
    where
        T: Fn(&StatsdSample) -> (),
    {
        proc: T,
        count: Arc<AtomicU32>,
    }

    impl<T: Fn(&StatsdSample) -> ()> processors::Processor for AssertProc<T> {
        fn provide_statsd(&self, sample: &StatsdSample) -> Option<processors::Output> {
            (self.proc)(sample);
            self.count.fetch_add(1, Ordering::Acquire);
            None
        }
    }

    #[test]
    fn simple_nil_backend() {
        let scope = crate::stats::Collector::default().scope("prefix");
        let _backend = Backends::new(scope);
    }

    #[test]
    fn processor_tag_test() {
        // Create the backend
        let scope = crate::stats::Collector::default().scope("prefix");
        let backend = Backends::new(scope);

        // Create a mock processor to receive all messages
        let route_final = vec![config::Route {
            route_type: config::RouteType::Processor,
            route_to: "final".to_owned(),
        }];
        let counter = Arc::new(AtomicU32::new(0));
        let proc = Box::new(AssertProc {
            proc: |sample| {
                let owned: statsd_proto::Owned = sample.try_into().unwrap();
                assert_eq!(owned.name(), b"foo.bar.__tags=value");
            },
            count: counter.clone(),
        });
        // Insert the assert processors
        backend
            .inner
            .write()
            .processors
            .insert("final".to_owned(), proc);

        // Create the processor under test
        let tn = processors::tag::Normalizer::new(&route_final);
        backend
            .inner
            .write()
            .processors
            .insert("tag".to_owned(), Box::new(tn));

        let pdu =
            statsd_proto::PDU::parse(bytes::Bytes::from_static(b"foo.bar:3|c|#tags:value|@1.0"))
                .unwrap();
        let route = vec![config::Route {
            route_type: config::RouteType::Processor,
            route_to: "tag".to_owned(),
        }];
        backend.provide_statsd_pdu(pdu, &route);

        // Check how many messages the mock has received
        let actual_count = counter.load(Ordering::Acquire);
        assert_eq!(1, actual_count);
    }
}
