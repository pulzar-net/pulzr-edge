//! One interval's advertisements, folded to one entry per device: the
//! latest data and the best signal heard, as `pulzr-ble` has always sent.

use std::collections::HashMap;

use bluetooth_host::{Address, Report};

/// Devices remembered per interval; past this, new devices wait for the next.
const MAX_DEVICES: usize = 4096;

/// What one device sent during an interval.
#[derive(Debug, Clone, PartialEq)]
pub struct Heard {
    /// The address it advertised from.
    pub address: Address,
    /// The identity a private address resolved to.
    pub identity: Option<Address>,
    /// The latest advertising data.
    pub data: Vec<u8>,
    /// The latest scan response, when scanning actively.
    pub scan_response: Option<Vec<u8>>,
    /// The best RSSI of the interval, dBm.
    pub rssi: i8,
    /// TX power from the extended header, dBm.
    pub tx_power: Option<i8>,
    /// The legacy PDU type (0 ADV_IND ... 3 ADV_NONCONN_IND) for legacy advertising.
    pub adv_type: Option<u8>,
    /// What kind of advertisement: `adv_ind`, `adv_scan_ind`, `adv_nonconn_ind`,
    /// `adv_direct_ind`, or `ext_*` for extended advertising.
    pub kind: &'static str,
    /// When it was last heard, seconds since the epoch.
    pub ts: f64,
}

impl Heard {
    /// The identity if resolved, else the address.
    pub fn device(&self) -> Address {
        self.identity.unwrap_or(self.address)
    }
}

fn kind(r: &Report) -> (&'static str, Option<u8>) {
    match (r.legacy, r.connectable, r.scannable, r.directed) {
        (true, true, _, true) => ("adv_direct_ind", Some(1)),
        (true, true, _, false) => ("adv_ind", Some(0)),
        (true, false, true, _) => ("adv_scan_ind", Some(2)),
        (true, false, false, _) => ("adv_nonconn_ind", Some(3)),
        (false, true, _, _) => ("ext_connectable", None),
        (false, false, true, _) => ("ext_scannable", None),
        (false, false, false, _) => ("ext_nonconn", None),
    }
}

/// The current interval.
pub struct Window {
    prefixes: Vec<Vec<u8>>,
    min_rssi: i8,
    heard: HashMap<Address, Heard>,
    dropped: u64,
}

impl Window {
    /// Admits devices whose address or identity starts with one of
    /// `prefixes` (most significant byte first); none admits all.
    pub fn new(prefixes: Vec<Vec<u8>>, min_rssi: i8) -> Self {
        Window {
            prefixes,
            min_rssi,
            heard: HashMap::new(),
            dropped: 0,
        }
    }

    fn admitted(&self, a: &Address) -> bool {
        if self.prefixes.is_empty() {
            return true;
        }
        let msb: Vec<u8> = a.bytes.iter().rev().copied().collect();
        self.prefixes.iter().any(|p| msb.starts_with(p))
    }

    /// Folds one report in.
    pub fn add(&mut self, r: &Report, ts: f64) {
        if r.rssi < self.min_rssi
            || !(self.admitted(&r.address) || r.identity.is_some_and(|i| self.admitted(&i)))
        {
            return;
        }
        let device = r.device();
        if !self.heard.contains_key(&device) && self.heard.len() >= MAX_DEVICES {
            self.dropped += 1;
            return;
        }
        let (kind, adv_type) = kind(r);
        let h = self.heard.entry(device).or_insert_with(|| Heard {
            address: r.address,
            identity: r.identity,
            data: Vec::new(),
            scan_response: None,
            rssi: r.rssi,
            tx_power: None,
            adv_type,
            kind,
            ts,
        });
        h.address = r.address;
        h.identity = r.identity;
        h.data = r.data.clone();
        if r.scan_response.is_some() {
            h.scan_response = r.scan_response.clone();
        }
        h.rssi = h.rssi.max(r.rssi);
        h.tx_power = r.tx_power.or(h.tx_power);
        h.adv_type = adv_type;
        h.kind = kind;
        h.ts = ts;
    }

    /// The interval's devices; starts the next interval.
    pub fn drain(&mut self) -> Vec<Heard> {
        if self.dropped > 0 {
            tracing::warn!(
                dropped = self.dropped,
                "more devices than one interval holds"
            );
            self.dropped = 0;
        }
        let mut v: Vec<Heard> = self.heard.drain().map(|(_, h)| h).collect();
        v.sort_by_key(Heard::device);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn report(addr: &str, rssi: i8, data: &[u8]) -> Report {
        Report {
            address: addr.parse().unwrap(),
            identity: None,
            rssi,
            tx_power: None,
            primary_phy: bluetooth_host::Phy::Le1m,
            secondary_phy: None,
            sid: None,
            periodic_interval: None,
            connectable: false,
            scannable: false,
            directed: false,
            legacy: true,
            direct_address: None,
            data: data.to_vec(),
            scan_response: None,
            truncated: false,
            time: SystemTime::now(),
        }
    }

    #[test]
    fn latest_data_best_signal_admitted_only() {
        let mut w = Window::new(vec![vec![0xbc, 0x57, 0x29]], -100);
        w.add(&report("BC:57:29:00:00:01", -80, &[1]), 1.0);
        w.add(&report("BC:57:29:00:00:01", -60, &[2]), 2.0);
        w.add(&report("BC:57:29:00:00:01", -90, &[3]), 3.0);
        w.add(&report("11:22:33:44:55:66", -40, &[9]), 3.0);
        w.add(&report("BC:57:29:00:00:02", -101, &[9]), 3.0);
        let v = w.drain();
        assert_eq!(v.len(), 1);
        assert_eq!((v[0].data.clone(), v[0].rssi, v[0].ts), (vec![3], -60, 3.0));
        assert_eq!((v[0].kind, v[0].adv_type), ("adv_nonconn_ind", Some(3)));
        assert!(w.drain().is_empty(), "a new interval starts empty");
    }
}
