use std::collections::VecDeque;
use std::time::{Duration, Instant};

struct Event {
    instant: Instant,
    bytes: u64,
}

// Measure per minute transfer stats
#[derive(Default)]
pub struct TransferStats {
    // list of entries, we keep entries not older than 1m
    events: VecDeque<Event>,
    // sum of bytes for all entries
    bytes_per_min: u64,
}

#[derive(Eq, PartialEq, Debug)]
pub struct MinuteStats {
    // bytes per minute
    pub bytes_per_min: u64,
    // count per minute
    pub count_per_min: usize,
}

impl TransferStats {
    // Record event
    // TODO(#5245) Remove instant once we add time mocking
    pub fn record(&mut self, bytes: u64, instant: Instant) {
        self.bytes_per_min += bytes;
        self.events.push_back(Event { instant, bytes })
    }

    // Get MinuteStats
    // TODO(#5245) Remove instant once we add time mocking
    pub fn minute_stats(&mut self, instant: Instant) -> MinuteStats {
        self.remove_old_entries(instant);
        MinuteStats { bytes_per_min: self.bytes_per_min, count_per_min: self.events.len() }
    }

    // Remove entries older than
    // TODO(#5245) Remove instant once we add time mocking
    pub fn remove_old_entries(&mut self, instant: Instant) {
        while let Some(event) = self.events.front() {
            if instant.duration_since(event.instant) > Duration::from_secs(60) {
                self.bytes_per_min -= event.bytes;
                self.events.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_transfer_stats() {
        let mut ts = TransferStats::default();
        let now = Instant::now();
        assert_eq!(ts.minute_stats(now), MinuteStats { bytes_per_min: 0, count_per_min: 0 });

        ts.record(10, now);

        assert_eq!(ts.minute_stats(now), MinuteStats { bytes_per_min: 10, count_per_min: 1 });

        let now = now + Duration::from_secs(45);
        ts.record(100, now);
        assert_eq!(ts.minute_stats(now), MinuteStats { bytes_per_min: 110, count_per_min: 2 });

        let now = now + Duration::from_secs(14);
        ts.record(1000, now);
        assert_eq!(ts.minute_stats(now), MinuteStats { bytes_per_min: 1111, count_per_min: 3 });

        let now = now + Duration::from_secs(2);
        assert_eq!(ts.minute_stats(now), MinuteStats { bytes_per_min: 1110, count_per_min: 2 });

        let now = now + Duration::from_secs(59);
        assert_eq!(ts.minute_stats(now), MinuteStats { bytes_per_min: 0, count_per_min: 0 });
    }
}
