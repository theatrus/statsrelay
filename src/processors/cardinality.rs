use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime};

use super::super::config;
use super::super::statsd_proto::Sample;
use super::{Output, Processor};

use ahash::AHasher;
use cuckoofilter::{self, CuckooFilter};
use parking_lot::Mutex;

struct TimeBoundedCuckoo<H>
where
    H: Hasher + Default,
{
    filter: CuckooFilter<H>,
    valid_until: SystemTime,
}

impl<H> TimeBoundedCuckoo<H>
where
    H: Hasher + Default,
{
    fn new(valid_until: SystemTime) -> Self {
        TimeBoundedCuckoo {
            filter: CuckooFilter::with_capacity(cuckoofilter::DEFAULT_CAPACITY),
            valid_until: valid_until,
        }
    }
}

struct MultiCuckoo<H>
where
    H: Hasher + Default,
{
    buckets: usize,
    window: Duration,
    filters: Vec<TimeBoundedCuckoo<H>>,
}

impl<H> MultiCuckoo<H>
where
    H: Hasher + Default,
{
    fn new(buckets: usize, window: &Duration) -> Self {
        assert!(buckets > 0);
        let now = SystemTime::now();
        let cuckoos: Vec<_> = (1..(buckets + 1))
            .map(|bucket| TimeBoundedCuckoo::new(now + (*window * bucket as u32)))
            .collect();
        MultiCuckoo {
            buckets: buckets,
            window: *window,
            filters: cuckoos,
        }
    }

    fn len(&self) -> usize {
        self.filters[0].filter.len()
    }

    fn contains<T: ?Sized + Hash>(&self, data: &T) -> bool {
        self.filters[0].filter.contains(data)
    }

    fn add<T: ?Sized + Hash>(&mut self, data: &T) -> Result<(), cuckoofilter::CuckooError> {
        let results: Result<Vec<_>, _> = self
            .filters
            .iter_mut()
            .map(|filter| filter.filter.add(data))
            .collect();
        results.map(|_| ())
    }

    fn rotate(&mut self, with_time: SystemTime) {
        match self.filters[0].valid_until.duration_since(with_time) {
            // duration_since returns err if the given is later then the valid_until time, aka expired
            Err(_) => {
                self.filters.remove(0);
                self.filters.push(TimeBoundedCuckoo::new(
                    with_time + (self.window * (self.buckets + 1) as u32),
                ));
            }
            _ => (),
        }
    }
}

pub struct Cardinality {
    route: Vec<config::Route>,
    filter: Mutex<MultiCuckoo<AHasher>>,
    limit: usize,
}

impl Cardinality {
    pub fn new(from_config: &config::processor::Cardinality) -> Self {
        let window = Duration::from_secs(from_config.rotate_after_seconds);
        Cardinality {
            route: from_config.route.clone(),
            filter: Mutex::new(MultiCuckoo::new(from_config.buckets, &window)),
            limit: from_config.size_limit as usize,
        }
    }

    fn rotate(&self) {
        self.filter.lock().rotate(SystemTime::now())
    }
}

impl Processor for Cardinality {
    fn provide_statsd(&self, sample: &Sample) -> Option<Output> {
        let mut filter = self.filter.lock();
        let contains = filter.contains(sample);
        if !contains && filter.len() > self.limit {
            return None;
        }
        let _ = filter.add(sample);
        Some(Output {
            route: self.route.as_ref(),
            new_sample: None,
        })
    }

    fn tick(&self, _time: std::time::SystemTime) -> () {
        self.rotate();
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn cuckoo_simple_contains() {
        let a = "a".to_string();
        let b = "b".to_string();

        let mut mc: MultiCuckoo<AHasher> = MultiCuckoo::new(2, &Duration::from_secs(60));

        mc.add(&a).unwrap();
        assert!(!mc.contains(&b));
        assert!(mc.contains(&a));
        mc.add(&b).unwrap();
        assert!(mc.contains(&b));
    }

    #[test]
    fn cuckoo_simple_rotate() {
        let a = "a".to_string();
        let b = "b".to_string();

        let now = SystemTime::now();
        let mut mc: MultiCuckoo<AHasher> = MultiCuckoo::new(2, &Duration::from_secs(60));

        mc.add(&a).unwrap();
        assert!(!mc.contains(&b));
        assert!(mc.contains(&a));
        mc.add(&b).unwrap();
        assert!(mc.contains(&b));
        // Rotate once, add only a
        mc.rotate(now + Duration::from_secs(61));
        assert!(mc.contains(&a));
        assert!(mc.contains(&b));
        assert!(mc.len() == 2);
        mc.add(&a).unwrap();
        // Rotate again, b should drop out
        mc.rotate(now + Duration::from_secs(122));
        assert!(mc.contains(&a));
        assert!(!mc.contains(&b));
        assert!(mc.len() == 1);
    }
}
