use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use stream_cancel::Tripwire;
use thiserror::Error;

use crate::discovery;
use crate::stats;
use crate::statsd_backend::StatsdBackend;
use crate::statsd_proto::Event;
use crate::{config, processors};

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
            stats,
        }
    }

    fn replace_processor(
        &mut self,
        name: &str,
        processor: Box<dyn processors::Processor + Send + Sync>,
    ) -> anyhow::Result<()> {
        self.processors.insert(name.to_owned(), processor);
        Ok(())
    }

    fn replace_statsd_backend(
        &mut self,
        name: &str,
        c: &config::StatsdBackendConfig,
        discovery_update: Option<&discovery::Update>,
    ) -> anyhow::Result<()> {
        let previous = self.statsd.get(name);
        let backend = StatsdBackend::new(self.stats.scope(name), c, previous, discovery_update)?;
        self.statsd.insert(name.to_owned(), backend);
        Ok(())
    }

    fn len(&self) -> usize {
        self.statsd.len()
    }

    fn remove_statsd_backend(&mut self, name: &str) -> anyhow::Result<()> {
        self.statsd.remove(name);
        Ok(())
    }

    fn backend_names(&self) -> HashSet<&String> {
        self.statsd.keys().collect()
    }

    fn provide_statsd(&self, pdu: &Event, route: &[config::Route]) {
        for dest in route {
            match dest.route_type {
                config::RouteType::Statsd => {
                    if let Some(backend) = self.statsd.get(dest.route_to.as_str()) {
                        backend.provide_statsd(pdu)
                    }
                }
                config::RouteType::Processor => {
                    if let Some(chain) = self
                        .processors
                        .get(dest.route_to.as_str())
                        .map(|proc| proc.provide_statsd(pdu))
                        .flatten()
                    {
                        match chain.new_events {
                            None => self.provide_statsd(pdu, chain.route),
                            Some(sv) => {
                                for pdu in sv.as_ref() {
                                    self.provide_statsd(pdu, chain.route);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Provide a periodic "tick" function to drive processors background
    /// housekeeping tasks asynchronously.
    fn processor_tick(&self, now: std::time::SystemTime, backends: &Backends) {
        for (_, proc) in self.processors.iter() {
            proc.tick(now, backends);
        }
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

    pub fn replace_processor(
        &self,
        name: &str,
        processor: Box<dyn processors::Processor + Send + Sync>,
    ) -> anyhow::Result<()> {
        self.inner.write().replace_processor(name, processor)
    }

    pub fn replace_statsd_backend(
        &self,
        name: &str,
        c: &config::StatsdBackendConfig,
        discovery_update: Option<&discovery::Update>,
    ) -> anyhow::Result<()> {
        self.inner
            .write()
            .replace_statsd_backend(name, c, discovery_update)
    }

    pub fn remove_statsd_backend(&self, name: &str) -> anyhow::Result<()> {
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn provide_statsd(&self, pdu: &Event, route: &[config::Route]) {
        self.inner.read().provide_statsd(pdu, route)
    }

    pub fn provide_statsd_slice(&self, pdu: &[Event], route: &[config::Route]) {
        let lock = self.inner.read();
        for p in pdu {
            lock.provide_statsd(p, route);
        }
    }

    pub fn processor_tick(&self, now: std::time::SystemTime) {
        self.inner.read().processor_tick(now, self);
    }
}

pub async fn ticker(tripwire: Tripwire, backends: Backends) {
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now(),
        tokio::time::Duration::from_secs(1),
    );
    loop {
        tokio::select! {
            _ = tripwire.clone() => { return; }
            _ = ticker.tick() => {
                let back = backends.clone();
                tokio::task::spawn_blocking(move || {
                    back.processor_tick(std::time::SystemTime::now())
                }).await.unwrap();
            }
        }
    }
}

#[cfg(test)]
pub mod test {

    use super::*;
    use crate::processors::{self, Processor};
    use crate::statsd_proto;
    use crate::statsd_proto::Parsed;

    use std::convert::TryInto;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    struct AssertProc<T>
    where
        T: Fn(&Event),
    {
        proc: T,
        count: Arc<AtomicU32>,
    }

    impl<T: Fn(&Event)> processors::Processor for AssertProc<T> {
        fn provide_statsd(&self, sample: &Event) -> Option<processors::Output> {
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

    fn make_counting_mock() -> (Arc<AtomicU32>, Box<dyn Processor + Send + Sync>) {
        let counter = Arc::new(AtomicU32::new(0));
        let proc = Box::new(AssertProc {
            proc: |_| {},
            count: counter.clone(),
        });
        (counter, proc)
    }

    fn make_asserting_mock<T: Fn(&Event) + Send + Sync + 'static>(
        t: T,
    ) -> (Arc<AtomicU32>, Box<dyn Processor + Send + Sync>) {
        let counter = Arc::new(AtomicU32::new(0));
        let proc = Box::new(AssertProc {
            proc: t,
            count: counter.clone(),
        });
        (counter, proc)
    }

    fn insert_proc(backend: &Backends, name: &str, proc: Box<dyn Processor + Send + Sync>) {
        backend
            .inner
            .write()
            .processors
            .insert(name.to_owned(), proc);
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
        let (counter, proc) = make_asserting_mock(|sample| {
            let owned: statsd_proto::Owned = sample.try_into().unwrap();
            assert_eq!(owned.name(), b"foo.bar.__tags=value");
        });

        // Insert the assert processors
        insert_proc(&backend, "final", proc);

        // Create the processor under test
        let tn = processors::tag::Normalizer::new(&route_final);
        insert_proc(&backend, "tag", Box::new(tn));

        let pdu =
            statsd_proto::Pdu::parse(bytes::Bytes::from_static(b"foo.bar:3|c|#tags:value|@1.0"))
                .unwrap();
        let route = vec![config::Route {
            route_type: config::RouteType::Processor,
            route_to: "tag".to_owned(),
        }];
        backend.provide_statsd(&Event::Pdu(pdu), &route);

        // Check how many messages the mock has received
        let actual_count = counter.load(Ordering::Acquire);
        assert_eq!(1, actual_count);
    }

    #[test]
    fn processor_fanout_test() {
        // Create the backend
        let scope = crate::stats::Collector::default().scope("prefix");
        let backend = Backends::new(scope);

        // Create a mock processor to receive all messages, 2x over
        let route_final = vec![
            config::Route {
                route_type: config::RouteType::Processor,
                route_to: "final1".to_owned(),
            },
            config::Route {
                route_type: config::RouteType::Processor,
                route_to: "final2".to_owned(),
            },
        ];
        let (counter1, proc1) = make_counting_mock();
        let (counter2, proc2) = make_counting_mock();

        // Insert the assert processors
        insert_proc(&backend, "final1", proc1);
        insert_proc(&backend, "final2", proc2);

        // Create the processor under test
        let tn = processors::tag::Normalizer::new(&route_final);
        insert_proc(&backend, "tag", Box::new(tn));

        let pdu =
            statsd_proto::Pdu::parse(bytes::Bytes::from_static(b"foo.bar:3|c|#tags:value|@1.0"))
                .unwrap();
        let route = vec![config::Route {
            route_type: config::RouteType::Processor,
            route_to: "tag".to_owned(),
        }];
        backend.provide_statsd(&Event::Pdu(pdu), &route);

        // Check how many messages the mock has received
        let actual_count = counter1.load(Ordering::Acquire);
        assert_eq!(1, actual_count);
        let actual_count2 = counter2.load(Ordering::Acquire);
        assert_eq!(1, actual_count2);
    }
}
