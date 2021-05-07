use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

use regex::bytes::RegexSet;

use crate::config;
use crate::discovery;
use crate::shard::{statsrelay_compat_hash, Ring};
use crate::stats;
use crate::statsd_client::StatsdClient;
use crate::statsd_proto;
use crate::statsd_proto::Event;

use log::warn;

pub struct StatsdBackend {
    conf: config::StatsdBackendConfig,
    ring: Ring<StatsdClient>,
    input_filter: Option<RegexSet>,
    warning_log: AtomicU64,
    backend_sends: stats::Counter,
    backend_fails: stats::Counter,
}

impl StatsdBackend {
    pub fn new(
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
        let input_filter = if !filters.is_empty() {
            Some(RegexSet::new(filters).unwrap())
        } else {
            None
        };

        let mut ring: Ring<StatsdClient> = Ring::new();

        // Use the same backend for the same endpoint address, caching the lookup locally
        let mut memoize: HashMap<String, StatsdClient> =
            client_ref.map_or_else(HashMap::new, |b| b.clients());

        let use_endpoints = discovery_update
            .map(|u| u.sources())
            .unwrap_or(&conf.shard_map);
        for endpoint in use_endpoints {
            if endpoint.is_empty() {
                continue;
            }
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
            ring,
            input_filter,
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

    pub fn provide_statsd(&self, input: &Event) {
        let pdu: statsd_proto::Pdu = input.into();
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
            1 => 1_u32,
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
