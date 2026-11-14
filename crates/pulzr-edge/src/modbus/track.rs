//! Report on change past a deadband, plus a heartbeat.

use super::codec::Sample;

/// Remembers what was last reported for one field.
#[derive(Debug, Default)]
pub struct Tracker {
    last: Option<(Sample, f64)>,
}

impl Tracker {
    /// Whether `now` (monotonic seconds) calls for a report of `s`: the first value
    /// always, then a change past `deadband` (any change when 0), or a heartbeat.
    pub fn due(&self, s: Sample, now: f64, deadband: f64, heartbeat: f64) -> bool {
        let Some((last, at)) = self.last else {
            return true;
        };
        if now - at >= heartbeat {
            return true;
        }
        match (s, last) {
            (Sample::Num(a), Sample::Num(b)) => {
                if deadband > 0.0 {
                    (a - b).abs() > deadband
                } else {
                    a != b
                }
            }
            (a, b) => a != b,
        }
    }

    /// Record that `s` was reported at `now`.
    pub fn reported(&mut self, s: Sample, now: f64) {
        self.last = Some((s, now));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_value_always_reports() {
        assert!(Tracker::default().due(Sample::Num(1.0), 0.0, 5.0, 300.0));
    }

    #[test]
    fn deadband_is_measured_from_the_last_report_not_the_last_sample() {
        let mut t = Tracker::default();
        t.reported(Sample::Num(20.0), 0.0);
        assert!(
            !t.due(Sample::Num(20.2), 10.0, 0.2, 300.0),
            "equal to the deadband is inside it"
        );
        assert!(!t.due(Sample::Num(20.1), 20.0, 0.2, 300.0));
        assert!(t.due(Sample::Num(20.21), 30.0, 0.2, 300.0));
        assert!(
            t.due(Sample::Num(19.79), 30.0, 0.2, 300.0),
            "both directions"
        );
    }

    #[test]
    fn zero_deadband_reports_any_change() {
        let mut t = Tracker::default();
        t.reported(Sample::Num(1.0), 0.0);
        assert!(!t.due(Sample::Num(1.0), 1.0, 0.0, 300.0));
        assert!(t.due(Sample::Num(1.000001), 1.0, 0.0, 300.0));
    }

    #[test]
    fn heartbeat_fires_even_when_flat() {
        let mut t = Tracker::default();
        t.reported(Sample::Num(5.0), 100.0);
        assert!(!t.due(Sample::Num(5.0), 399.9, 1.0, 300.0));
        assert!(t.due(Sample::Num(5.0), 400.0, 1.0, 300.0));
        t.reported(Sample::Num(5.0), 400.0);
        assert!(
            !t.due(Sample::Num(5.0), 401.0, 1.0, 300.0),
            "the heartbeat restarts"
        );
    }

    #[test]
    fn booleans_report_any_change() {
        let mut t = Tracker::default();
        t.reported(Sample::Bool(false), 0.0);
        assert!(!t.due(Sample::Bool(false), 1.0, 9.0, 300.0));
        assert!(t.due(Sample::Bool(true), 1.0, 9.0, 300.0));
    }
}
