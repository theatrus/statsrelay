use crate::processors;
use crate::statsd_proto::Id;
use crate::statsd_proto::{Owned, Sample, Type};
use crate::{config, statsd_proto::Parsed};

use ahash::RandomState;
use parking_lot::Mutex;
use std::cell::RefCell;
use thiserror::Error;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryInto;

use super::Output;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid sampler configuration")]
    InvalidConfig,
}

#[derive(Debug, Default)]
struct Counter {
    value: f64,
    samples: f64,
}

#[derive(Debug)]
struct Timer {
    values: Vec<f64>,
    filled_count: usize,
}

#[derive(Debug, Default)]
struct Gauge {
    value: f64,
}

#[derive(Debug)]
pub struct Sampler {
    config: config::processor::Sampler,
    counters: Mutex<RefCell<HashMap<Id, Counter, RandomState>>>,
    timers: Mutex<RefCell<HashMap<Id, Timer, RandomState>>>,
    gauges: Mutex<RefCell<HashMap<Id, Gauge, RandomState>>>,

    route_to: Vec<config::Route>,
}

impl Sampler {
    pub fn new(config: &config::processor::Sampler) -> Result<Self, Error> {
        let counters: RefCell<HashMap<Id, Counter, RandomState>> = RefCell::new(HashMap::default());
        let timers: RefCell<HashMap<Id, Timer, RandomState>> = RefCell::new(HashMap::default());
        let gauges: RefCell<HashMap<Id, Gauge, RandomState>> = RefCell::new(HashMap::default());
        Ok(Sampler {
            config: config.clone(),
            counters: Mutex::new(counters),
            timers: Mutex::new(timers),
            gauges: Mutex::new(gauges),
            route_to: config.route.clone(),
        })
    }
}

impl processors::Processor for Sampler {
    fn provide_statsd(&self, sample: &Sample) -> Option<processors::Output> {
        let owned: Result<Owned, _> = sample.try_into();
        match owned {
            Err(_) => None,
            Ok(owned) if owned.metric_type() == &Type::Timer => unimplemented!(),
            Ok(owned) if owned.metric_type() == &Type::Counter => {
                let lock = self.counters.lock();
                let mut hm = lock.borrow_mut();
                unimplemented!();
            }
            Ok(owned) if owned.metric_type() == &Type::Gauge => {
                let lock = self.gauges.lock();
                let mut hm = lock.borrow_mut();
                // Note: Using the entry API would make logical sense to avoid
                // re-hashing the same Id on insert, however it costs more to
                // clone the Id as the entry API does not allow for trait Clone
                // key references and supporting lazy-cloning.
                match hm.get_mut(owned.id()) {
                    Some(v) => v.value = owned.value(),
                    None => {
                        hm.insert(
                            owned.id().clone(),
                            Gauge {
                                value: owned.value(),
                            },
                        );
                    }
                };
                None
            }
            Ok(_) => Some(Output {
                route: &self.route_to,
                new_sample: None,
            }),
        }
    }
}
