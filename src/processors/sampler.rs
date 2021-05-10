use super::Output;
use crate::backends::Backends;
use crate::processors;
use crate::statsd_proto::Id;
use crate::statsd_proto::{Event, Owned, Type};
use crate::{config, statsd_proto::Parsed};

use ahash::RandomState;
use parking_lot::Mutex;
use std::cell::RefCell;
use thiserror::Error;

use std::collections::HashMap;
use std::convert::TryInto;

const DEFAULT_RESERVOIR: u32 = 100;

fn scale(value: f64, sample_rate: Option<f64>) -> (f64, f64) {
    match sample_rate {
        None => (value, 1_f64),
        Some(rate) => {
            let scale = 1_f64 / rate;
            if scale > 0_f64 && scale <= 1_f64 {
                (value * scale, scale)
            } else {
                (value, 1_f64)
            }
        }
    }
}

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

impl Counter {
    fn to_event(&self, id: &Id) -> Event {
        let value = self.value / self.samples;
        let sample_rate = 1_f64 / self.samples;
        Event::Parsed(Owned::new(id.clone(), value, Some(sample_rate)))
    }
}

#[derive(Debug)]
struct Timer {
    values: Vec<f64>,
    filled_count: u32,
    reservoir_size: u32,
    count: f64,
    sum: f64,
}

impl Timer {
    fn new(reservoir_size: u32) -> Self {
        Timer {
            values: Vec::with_capacity(reservoir_size as usize),
            filled_count: 0,
            reservoir_size,
            count: 0_f64,
            sum: 0_f64,
        }
    }

    fn add(&mut self, value: f64, sample_rate: Option<f64>) {
        // Do an initial fill if we haven't filled the full reservoir
        if self.values.len() < self.reservoir_size as usize {
            self.values.push(value);
        } else {
            match fastrand::u32(..) % self.filled_count {
                idx if idx < self.reservoir_size => self.values[idx as usize] = value,
                _ => (),
            }
        }
        let (sum, count) = scale(value, sample_rate);
        // Keep track of a sample rate scaled count independently from the
        // reservoir sample fill
        self.count += count;
        self.sum += sum;
        self.filled_count += 1;
    }
}

#[derive(Debug, Default)]
struct Gauge {
    value: f64,
}

impl Gauge {
    fn to_event(&self, id: &Id) -> Event {
        Event::Parsed(Owned::new(id.clone(), self.value, None))
    }
}

#[derive(Debug)]
pub struct Sampler {
    config: config::processor::Sampler,
    counters: Mutex<RefCell<HashMap<Id, Counter, RandomState>>>,
    timers: Mutex<RefCell<HashMap<Id, Timer, RandomState>>>,
    gauges: Mutex<RefCell<HashMap<Id, Gauge, RandomState>>>,

    last_flush: Mutex<RefCell<std::time::SystemTime>>,

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
            last_flush: Mutex::new(RefCell::new(std::time::SystemTime::now())),
        })
    }

    fn record_timer(&self, owned: &Owned) {
        let lock = self.timers.lock();
        let mut hm = lock.borrow_mut();

        match hm.get_mut(owned.id()) {
            Some(v) => {
                v.add(owned.value(), owned.sample_rate());
            }
            None => {
                let mut timer = Timer::new(
                    self.config
                        .timer_reservoir_size
                        .unwrap_or(DEFAULT_RESERVOIR),
                );
                timer.add(owned.value(), owned.sample_rate());
                hm.insert(owned.id().clone(), timer);
            }
        }
    }

    fn record_gauge(&self, owned: &Owned) {
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
    }

    fn record_counter(&self, owned: &Owned) {
        // Adjust values based on sample rate. In the end, emission will
        // re-scale everything back to the sample rate.
        let (scaled, counts) = scale(owned.value(), owned.sample_rate());

        let lock = self.counters.lock();
        let mut hm = lock.borrow_mut();

        match hm.get_mut(owned.id()) {
            Some(v) => {
                v.value += scaled;
                v.samples += counts;
            }
            None => {
                hm.insert(
                    owned.id().clone(),
                    Counter {
                        value: scaled,
                        samples: counts,
                    },
                );
            }
        }
    }
}

impl processors::Processor for Sampler {
    fn provide_statsd(&self, sample: &Event) -> Option<processors::Output> {
        let owned: Result<Owned, _> = sample.try_into();
        match owned {
            Err(_) => None,
            Ok(owned) if owned.metric_type() == &Type::Timer => {
                self.record_timer(&owned);
                None
            }
            Ok(owned) if owned.metric_type() == &Type::Counter => {
                self.record_counter(&owned);
                None
            }
            Ok(owned) if owned.metric_type() == &Type::Gauge => {
                self.record_gauge(&owned);
                None
            }
            Ok(_) => Some(Output {
                route: &self.route_to,
                new_events: None,
            }),
        }
    }

    fn tick(&self, time: std::time::SystemTime, backends: &Backends) {
        // Take a lock on the last flush, which guards all other flushes.
        let flush_lock = self.last_flush.lock();
        let earlier = *flush_lock.borrow();
        match time.duration_since(earlier) {
            Err(_) => {
                return;
            }
            Ok(duration) if duration.as_secs() < self.config.window as u64 => {
                return;
            }
            Ok(_) => (),
        }

        let mut gauges = self.gauges.lock().replace(HashMap::default());
        for (id, gauge) in gauges.drain() {
            let pdu = gauge.to_event(&id);
            backends.provide_statsd(&pdu, self.route_to.as_ref())
        }

        let mut counters = self.counters.lock().replace(HashMap::default());
        for (id, counter) in counters.drain() {
            let pdu = counter.to_event(&id);
            backends.provide_statsd(&pdu, self.route_to.as_ref());
        }

        let mut timers = self.timers.lock().replace(HashMap::default());
        for (id, timer) in timers.drain() {
            let sample_rate = timer.values.len() as f64 / timer.count;
            for value in timer.values {
                let pdu = Event::Parsed(Owned::new(id.clone(), value, Some(sample_rate)));
                backends.provide_statsd(&pdu, self.route_to.as_ref());
            }
        }

        flush_lock.replace(time);
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn fill_timer() {
        let mut timer = Timer::new(100);
        for x in 0..200 {
            timer.add(x as f64, None);
        }
        assert_eq!(timer.filled_count, 200);
        assert_eq!(timer.count, 200_f64);
        assert_eq!(timer.sum, 19900_f64);
        assert_eq!(timer.values.len(), 100);
    }
}
