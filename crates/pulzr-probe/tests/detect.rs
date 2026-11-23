//! pulzr-probe against sysfs trees laid out as the kernel lays them out, and a
//! prober that records every port it was asked to write to.

use pulzr_probe::detect::{
    parse_umdns, Context, Decisions, Detected, Identified, ProbeState, Report, State, Use,
};
use pulzr_probe::probe::{Answer, Outcome, Port, Prober};
use pulzr_probe::sx130x::SpiTarget;
use pulzr_probe::table::{ProbeKind, Protocol, Table};
use std::cell::RefCell;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

struct Usb<'a> {
    port: &'a str,
    vid: &'a str,
    pid: &'a str,
    product: Option<&'a str>,
    manufacturer: Option<&'a str>,
    serial: Option<&'a str>,
}

impl<'a> Usb<'a> {
    fn new(port: &'a str, vid: &'a str, pid: &'a str) -> Self {
        Usb {
            port,
            vid,
            pid,
            product: None,
            manufacturer: None,
            serial: None,
        }
    }
    fn product(mut self, p: &'a str) -> Self {
        self.product = Some(p);
        self
    }
    fn serial(mut self, s: &'a str) -> Self {
        self.serial = Some(s);
        self
    }
    fn manufacturer(mut self, m: &'a str) -> Self {
        self.manufacturer = Some(m);
        self
    }
}

struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        for d in ["sys/class/tty", "sys/class/bluetooth", "dev", "proc"] {
            fs::create_dir_all(dir.path().join(d)).unwrap();
        }
        Fixture { dir }
    }

    fn root(&self) -> &Path {
        self.dir.path()
    }

    fn usb_dir(&self, usb: &Usb) -> PathBuf {
        let d = self
            .root()
            .join("sys/devices/platform/soc/usb1")
            .join(usb.port);
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join("idVendor"), format!("{}\n", usb.vid)).unwrap();
        fs::write(d.join("idProduct"), format!("{}\n", usb.pid)).unwrap();
        for (file, value) in [
            ("product", usb.product),
            ("manufacturer", usb.manufacturer),
            ("serial", usb.serial),
        ] {
            if let Some(v) = value {
                fs::write(d.join(file), format!("{v}\n")).unwrap();
            }
        }
        d
    }

    /// A usb-serial driver's port (cp210x, ch341, ftdi_sio): the tty's
    /// `device` is the usb-serial port under the interface.
    fn tty(&self, usb: &Usb, interface: u8, name: &str) {
        let iface = self
            .usb_dir(usb)
            .join(format!("{}:1.{interface}", usb.port));
        fs::create_dir_all(&iface).unwrap();
        fs::write(iface.join("bInterfaceNumber"), format!("{interface:02x}\n")).unwrap();
        let class_dev = iface.join(name).join("tty").join(name);
        fs::create_dir_all(&class_dev).unwrap();
        symlink("../..", class_dev.join("device")).unwrap();
        let rel = Path::new("../../devices/platform/soc/usb1")
            .join(usb.port)
            .join(format!("{}:1.{interface}", usb.port))
            .join(name)
            .join("tty")
            .join(name);
        symlink(rel, self.root().join("sys/class/tty").join(name)).unwrap();
        fs::write(self.root().join("dev").join(name), "").unwrap();
    }

    fn hci(&self, usb: &Usb, name: &str) {
        let iface = self.usb_dir(usb).join(format!("{}:1.0", usb.port));
        let class_dev = iface.join("bluetooth").join(name);
        fs::create_dir_all(&class_dev).unwrap();
        let rel = Path::new("../../devices/platform/soc/usb1")
            .join(usb.port)
            .join(format!("{}:1.0", usb.port))
            .join("bluetooth")
            .join(name);
        symlink(rel, self.root().join("sys/class/bluetooth").join(name)).unwrap();
    }

    /// A Pi's own Bluetooth, on a UART: the board's to declare.
    fn onboard_hci(&self, name: &str) {
        let d = self
            .root()
            .join("sys/devices/platform/soc/serial0/serial0-0/bluetooth")
            .join(name);
        fs::create_dir_all(&d).unwrap();
        symlink(
            Path::new("../../devices/platform/soc/serial0/serial0-0/bluetooth").join(name),
            self.root().join("sys/class/bluetooth").join(name),
        )
        .unwrap();
    }

    /// The onboard UART, which is not a USB port.
    fn onboard_tty(&self, name: &str) {
        let d = self
            .root()
            .join("sys/devices/platform/soc/fe201000.serial/tty")
            .join(name);
        fs::create_dir_all(&d).unwrap();
        symlink("../..", d.join("device")).unwrap();
        symlink(
            Path::new("../../devices/platform/soc/fe201000.serial/tty").join(name),
            self.root().join("sys/class/tty").join(name),
        )
        .unwrap();
    }

    /// Process `pid` has the device open, as /proc shows it.
    fn open(&self, pid: u32, name: &str) {
        let fd = self.root().join("proc").join(pid.to_string()).join("fd");
        fs::create_dir_all(&fd).unwrap();
        symlink(format!("/dev/{name}"), fd.join("3")).unwrap();
    }

    fn unplug(&self, name: &str) {
        fs::remove_file(self.root().join("sys/class/tty").join(name)).unwrap();
    }
}

/// Answers by device name and kind; records every port it was asked about.
#[derive(Default)]
struct Bench {
    answers: Vec<(&'static str, ProbeKind, Answer)>,
    asked: RefCell<Vec<(String, ProbeKind)>>,
}

impl Bench {
    fn answering(mut self, dev: &'static str, kind: ProbeKind, firmware: &str) -> Self {
        self.answers.push((
            dev,
            kind,
            Answer {
                firmware: Some(firmware.into()),
                ..Default::default()
            },
        ));
        self
    }
    fn asked_about(&self, dev: &str) -> Vec<ProbeKind> {
        self.asked
            .borrow()
            .iter()
            .filter(|(d, _)| d == dev)
            .map(|(_, k)| *k)
            .collect()
    }
}

impl Prober for Bench {
    fn probe(&self, kind: ProbeKind, port: Port) -> Outcome {
        let name = match port {
            Port::Serial(p) => p.file_name().unwrap().to_string_lossy().into_owned(),
            Port::Tcp(h, p) => format!("{h}:{p}"),
            Port::Spi(t) => t.dev.file_name().unwrap().to_string_lossy().into_owned(),
        };
        self.asked.borrow_mut().push((name.clone(), kind));
        match self
            .answers
            .iter()
            .find(|(d, k, _)| *d == name && *k == kind)
        {
            Some((_, _, a)) => Outcome::Answered(a.clone()),
            None => Outcome::NoAnswer,
        }
    }
}

struct Run<'a> {
    table: Table,
    decisions: Decisions,
    previous: Report,
    bench: &'a Bench,
}

impl<'a> Run<'a> {
    fn new(bench: &'a Bench) -> Self {
        Run {
            table: Table::builtin(),
            decisions: Decisions::default(),
            previous: Report::default(),
            bench,
        }
    }
    fn ctx<'b>(&'b self, f: &'b Fixture, now: u64) -> Context<'b> {
        Context {
            root: f.root(),
            table: &self.table,
            prober: self.bench,
            decisions: &self.decisions,
            previous: &self.previous,
            lan: &[],
            spi: &[],
            claimed: &[],
            now,
        }
    }
    fn scan(&self, f: &Fixture) -> Report {
        self.ctx(f, 1_795_618_931).scan()
    }
}

fn by_path<'a>(r: &'a Report, dev: &str) -> &'a Detected {
    r.detected
        .iter()
        .find(|d| d.path.as_deref() == Some(dev))
        .unwrap_or_else(|| panic!("no {dev} in {r:?}"))
}

fn zbt1(port: &str) -> Usb<'_> {
    Usb::new(port, "10c4", "ea60")
        .product("Home Assistant Connect ZBT-1")
        .manufacturer("Nabu Casa")
        .serial("9e2adbd1")
}

#[test]
fn a_known_dongle_is_named_and_probed_with_what_the_table_allows() {
    let f = Fixture::new();
    f.tty(&zbt1("1-1.2"), 0, "ttyUSB0");
    let bench = Bench::default().answering("ttyUSB0", ProbeKind::Ezsp, "7.4.4.0");
    let r = Run::new(&bench).scan(&f);
    let d = by_path(&r, "/dev/ttyUSB0");
    assert_eq!(d.id, "usb-10c4_ea60-9e2adbd1");
    assert_eq!(d.port.as_deref(), Some("1-1.2"));
    assert_eq!(d.adapter.as_deref(), Some("nabu-casa-zbt-1"));
    assert_eq!(d.label, "Home Assistant Connect ZBT-1");
    assert_eq!(
        (d.protocol, d.ncp.as_deref()),
        (Some(Protocol::Zigbee), Some("ezsp"))
    );
    assert_eq!(d.firmware.as_deref(), Some("7.4.4.0"));
    assert_eq!((d.probe, d.state), (Some(ProbeState::Ok), State::Free));
    assert_eq!(
        bench.asked_about("ttyUSB0"),
        vec![ProbeKind::Ezsp],
        "stops at the first answer"
    );
}

#[test]
fn a_bare_bridge_is_never_probed() {
    let f = Fixture::new();
    // The RS-485 adapter on a Modbus site: a CP2102N saying only what it is.
    f.tty(
        &Usb::new("1-1.3", "10c4", "ea60")
            .product("CP2102N USB to UART Bridge Controller")
            .serial("0001"),
        0,
        "ttyUSB0",
    );
    // A CH9102 that says nothing at all.
    f.tty(&Usb::new("1-1.4", "1a86", "55d4"), 0, "ttyACM0");
    let bench = Bench::default();
    let r = Run::new(&bench).scan(&f);
    let cp = by_path(&r, "/dev/ttyUSB0");
    assert_eq!((cp.adapter.as_deref(), cp.protocol), (None, None));
    assert_eq!(
        (cp.state, cp.probe),
        (State::Unidentified, Some(ProbeState::NotAllowed))
    );
    assert_eq!(cp.label, "USB serial adapter (CP210x)");
    let ch = by_path(&r, "/dev/ttyACM0");
    assert_eq!(ch.label, "USB serial adapter (CH9102)");
    assert_eq!(ch.id, "usb-1a86_55d4-1-1.4", "no serial: the port names it");
    assert!(
        bench.asked.borrow().is_empty(),
        "nothing was written to either: {:?}",
        bench.asked.borrow()
    );
}

#[test]
fn a_bridge_entry_without_a_product_string_cannot_ship_or_match() {
    let bad = r#"{"schema":1,"bridges":[{"vid":"10c4","pid":"ea60","chip":"CP210x"}],
      "adapters":[{"id":"any-cp2102","label":"x","protocols":["zigbee"],"probes":["ezsp"],
                   "match":{"vid":"10c4","pid":"ea60"}}]}"#;
    let err = Table::parse(bad).unwrap_err();
    assert!(err.contains("bridge chip"), "{err}");
    // Even a table that skipped the check does not name a bridge by its id.
    let table: Table = serde_json::from_str(bad).unwrap();
    let usb = pulzr_probe::table::UsbIdentity {
        vid: "10c4",
        pid: "ea60",
        product: Some("CP2102N"),
        interface: None,
    };
    assert!(table.adapter(&usb).is_none());

    for (why, table) in [
        (
            "too short",
            r#"{"schema":1,"bridges":[],"adapters":[{"id":"a1","label":"x","protocols":["zigbee"],"probes":["ezsp"],"match":{"vid":"1234","pid":"5678","product":["Zb"]}}]}"#,
        ),
        (
            "overlap",
            r#"{"schema":1,"bridges":[],"adapters":[
            {"id":"a1","label":"x","protocols":["zigbee"],"probes":["ezsp"],"match":{"vid":"1234","pid":"5678","product":["Stick 7"]}},
            {"id":"a2","label":"y","protocols":["zigbee"],"probes":["ezsp"],"match":{"vid":"1234","pid":"5678","product":["Z-Stick 7"]}}]}"#,
        ),
        (
            "no probe",
            r#"{"schema":1,"bridges":[],"adapters":[{"id":"a1","label":"x","protocols":["zigbee"],"match":{"vid":"1234","pid":"5678"}}]}"#,
        ),
        (
            "wrong probe",
            r#"{"schema":1,"bridges":[],"adapters":[{"id":"a1","label":"x","protocols":["zigbee"],"probes":["zwave-serial"],"match":{"vid":"1234","pid":"5678"}}]}"#,
        ),
    ] {
        assert!(Table::parse(table).is_err(), "{why}");
    }
}

#[test]
fn the_table_that_ships_passes_its_own_check() {
    assert_eq!(
        Table::parse(pulzr_probe::table::BUILTIN).map(|_| ()),
        Ok(())
    );
}

#[test]
fn unique_ids_multi_interface_sticks_and_bluetooth() {
    let f = Fixture::new();
    f.tty(
        &Usb::new("1-1.1", "1cf1", "0030")
            .product("ConBee II")
            .serial("DE2412345"),
        0,
        "ttyACM0",
    );
    let husbzb = Usb::new("1-1.2", "10c4", "8a2a")
        .product("HubZ Smart Home Controller")
        .serial("C1300C9F");
    f.tty(&husbzb, 0, "ttyUSB0");
    f.tty(&husbzb, 1, "ttyUSB1");
    f.tty(&Usb::new("1-1.3", "0658", "0200"), 0, "ttyACM1");
    f.hci(
        &Usb::new("1-1.4", "0bda", "8771").product("Bluetooth Radio"),
        "hci1",
    );
    f.onboard_hci("hci0");
    f.onboard_tty("ttyAMA0");
    let bench = Bench::default();
    let r = Run::new(&bench).scan(&f);
    assert_eq!(r.detected.len(), 5, "{r:#?}");

    let conbee = by_path(&r, "/dev/ttyACM0");
    assert_eq!(
        (conbee.adapter.as_deref(), conbee.ncp.as_deref()),
        (Some("dresden-conbee-2"), Some("deconz"))
    );
    assert_eq!(conbee.id, "usb-1cf1_0030-de2412345");
    let zw = by_path(&r, "/dev/ttyUSB0");
    let zb = by_path(&r, "/dev/ttyUSB1");
    assert_eq!(
        (zw.adapter.as_deref(), zw.protocol),
        (Some("nortek-husbzb-1-zwave"), Some(Protocol::Zwave))
    );
    assert_eq!(
        (zb.adapter.as_deref(), zb.protocol),
        (Some("nortek-husbzb-1-zigbee"), Some(Protocol::Zigbee))
    );
    assert_eq!(
        (zw.id.as_str(), zb.id.as_str()),
        ("usb-10c4_8a2a-c1300c9f", "usb-10c4_8a2a-c1300c9f.1")
    );
    let stick = by_path(&r, "/dev/ttyACM1");
    assert_eq!(
        (stick.adapter.as_deref(), stick.probe),
        (Some("zwave-serial-api"), Some(ProbeState::NoAnswer))
    );

    let bt = r
        .detected
        .iter()
        .find(|d| d.protocol == Some(Protocol::Ble))
        .unwrap();
    assert_eq!(
        (bt.adapter.as_deref(), bt.ncp.as_deref(), bt.state),
        (Some("realtek-rtl8761b"), Some("hci"), State::Free)
    );
    assert_eq!(bt.port.as_deref(), Some("1-1.4"));
    assert_eq!(bt.probe, None, "the kernel named it; nothing to ask");
    assert!(bench.asked_about("hci1").is_empty());
}

#[test]
fn without_a_stack_a_known_dongle_is_reported_as_it_ships() {
    let f = Fixture::new();
    f.tty(&zbt1("1-1.2"), 0, "ttyUSB0");
    let table = Table::builtin();
    let ctx = Context {
        root: f.root(),
        table: &table,
        prober: &pulzr_probe::probe::NoStacks,
        decisions: &Decisions::default(),
        previous: &Report::default(),
        lan: &[],
        spi: &[],
        claimed: &[],
        now: 5,
    };
    let d = ctx.scan().detected.remove(0);
    assert_eq!(
        (d.protocol, d.ncp.as_deref(), d.firmware),
        (Some(Protocol::Zigbee), Some("ezsp"), None)
    );
    assert_eq!(
        (d.probe, d.state, d.since),
        (Some(ProbeState::Unavailable), State::Free, 5)
    );
    assert_eq!(d.protocols, vec![Protocol::Zigbee, Protocol::Thread]);
}

#[test]
fn a_port_something_has_open_is_not_probed() {
    let f = Fixture::new();
    f.tty(&zbt1("1-1.2"), 0, "ttyUSB0");
    f.open(4242, "ttyUSB0");
    let bench = Bench::default().answering("ttyUSB0", ProbeKind::Ezsp, "7.4.4.0");
    let r = Run::new(&bench).scan(&f);
    assert_eq!(by_path(&r, "/dev/ttyUSB0").state, State::InUse);
    assert!(bench.asked.borrow().is_empty());
}

#[test]
fn a_modbus_adapter_is_never_probed_again_not_even_by_identify() {
    let f = Fixture::new();
    let rs485 = Usb::new("1-1.3", "1a86", "7523").product("USB Serial");
    f.tty(&rs485, 0, "ttyUSB0");
    let bench = Bench::default().answering("ttyUSB0", ProbeKind::Ezsp, "x");
    let mut run = Run::new(&bench);
    run.decisions
        .assigned
        .insert("usb-1a86_7523-1-1.3".into(), Use::Modbus);
    let r = run.scan(&f);
    let d = by_path(&r, "/dev/ttyUSB0");
    assert_eq!(
        (d.state, d.use_, d.probe),
        (State::InUse, Some(Use::Modbus), None)
    );
    let err = run.ctx(&f, 1).identify("usb-1a86_7523-1-1.3").unwrap_err();
    assert!(err.contains("Modbus"), "{err}");
    assert!(bench.asked.borrow().is_empty());
}

#[test]
fn identify_is_a_persons_probe_of_a_port_nobody_named() {
    let f = Fixture::new();
    // A Zooz 700-series stick: a CP2102N with the bridge's own product string.
    f.tty(
        &Usb::new("1-1.2", "10c4", "ea60")
            .product("CP2102N USB to UART Bridge Controller")
            .serial("a0b1"),
        0,
        "ttyUSB0",
    );
    let bench = Bench::default().answering("ttyUSB0", ProbeKind::ZwaveSerial, "7.18");
    let mut run = Run::new(&bench);
    assert!(bench.asked.borrow().is_empty());
    assert_eq!(run.scan(&f).detected[0].state, State::Unidentified);

    let found = run
        .ctx(&f, 1)
        .identify("usb-10c4_ea60-a0b1")
        .unwrap()
        .unwrap();
    assert_eq!(
        found,
        Identified {
            protocol: Protocol::Zwave,
            ncp: "zwave-serial".into(),
            firmware: Some("7.18".into()),
            ezsp: None,
            chip: None
        }
    );
    assert_eq!(
        bench.asked_about("ttyUSB0"),
        vec![ProbeKind::Ezsp, ProbeKind::Znp, ProbeKind::ZwaveSerial]
    );

    run.decisions
        .identified
        .insert("usb-10c4_ea60-a0b1".into(), found);
    bench.asked.borrow_mut().clear();
    let d = run.scan(&f).detected.remove(0);
    assert_eq!(
        (d.protocol, d.state, d.probe, d.adapter),
        (
            Some(Protocol::Zwave),
            State::Free,
            Some(ProbeState::Ok),
            None
        )
    );
    assert!(
        bench.asked.borrow().is_empty(),
        "what Identify found is kept, not asked again"
    );

    assert!(run.ctx(&f, 1).identify("usb-dead_beef-x").is_err());
}

#[test]
fn replugging_keeps_the_radio_and_does_not_ask_again() {
    let f = Fixture::new();
    f.tty(&zbt1("1-1.2"), 0, "ttyUSB0");
    let bench = Bench::default()
        .answering("ttyUSB0", ProbeKind::Ezsp, "7.4.4.0")
        .answering("ttyUSB1", ProbeKind::Ezsp, "7.4.4.0");
    let mut run = Run::new(&bench);
    let first = run.ctx(&f, 100).scan();
    run.previous = first.clone();
    bench.asked.borrow_mut().clear();

    let again = run.ctx(&f, 200).scan();
    assert_eq!(again, first, "a scan on an unrelated event changes nothing");
    assert!(bench.asked.borrow().is_empty());

    // Unplugged: gone from the list (the platform says it is missing).
    f.unplug("ttyUSB0");
    run.previous = run.ctx(&f, 300).scan();
    assert!(run.previous.detected.is_empty());

    // Back, into another port: the same radio by its serial number, a new
    // `since`, and asked once more.
    f.tty(&zbt1("1-1.4"), 0, "ttyUSB1");
    let back = run.ctx(&f, 400).scan();
    assert_eq!(back.detected[0].id, first.detected[0].id);
    assert_eq!(
        (back.detected[0].since, back.detected[0].port.as_deref()),
        (400, Some("1-1.4"))
    );
    assert_eq!(bench.asked_about("ttyUSB1"), vec![ProbeKind::Ezsp]);
}

#[test]
fn lan_coordinators_from_mdns() {
    let hosts = parse_umdns(
        r#"{"_slzb-06._tcp": {"SLZB-06-a1b2": {"host": "slzb-06-a1b2.local", "port": 6638, "ipv4": "192.168.8.20"}},
            "_http._tcp": {"printer": {"host": "printer.local", "port": 80}}}"#,
    );
    assert_eq!(hosts.len(), 2);
    let f = Fixture::new();
    let table = Table::builtin();
    let bench = Bench::default();
    let ctx = Context {
        root: f.root(),
        table: &table,
        prober: &bench,
        decisions: &Decisions::default(),
        previous: &Report::default(),
        lan: &hosts,
        spi: &[],
        claimed: &[],
        now: 1,
    };
    let r = ctx.scan();
    assert_eq!(r.detected.len(), 1, "a printer is not a radio");
    let d = &r.detected[0];
    assert_eq!(
        (d.id.as_str(), d.adapter.as_deref()),
        ("lan-slzb-06-a1b2.local", Some("smlight-slzb-06"))
    );
    assert_eq!(d.path.as_deref(), Some("slzb-06-a1b2.local:6638"));
    assert_eq!(
        bench.asked_about("slzb-06-a1b2.local:6638"),
        vec![ProbeKind::Znp, ProbeKind::Ezsp]
    );
    assert!(bench.asked_about("printer.local:80").is_empty());
}

#[test]
fn the_report_is_pulzr_protocol_detected_radios() {
    let schema: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../pulzr-protocol/schema/protocol.schema.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let state = jsonschema::validator_for(
        &serde_json::json!({"$defs": schema["$defs"], "$ref": "#/$defs/state"}),
    )
    .unwrap();

    let f = Fixture::new();
    f.tty(&zbt1("1-1.2"), 0, "ttyUSB0");
    f.tty(&Usb::new("1-1.3", "1a86", "55d4"), 0, "ttyACM0");
    f.tty(
        &Usb::new("1-1.5", "1a86", "7523").product("USB Serial"),
        0,
        "ttyUSB1",
    );
    f.hci(&Usb::new("1-1.4", "0bda", "8771"), "hci0");
    f.tty(&rak5146("1-1.6"), 0, "ttyACM1");
    let mut bench = Bench::default()
        .answering("ttyUSB0", ProbeKind::Ezsp, "7.4.4.0")
        .answering("ttyACM1", ProbeKind::Sx130xUsb, "01.00.02");
    bench.answers.push((
        "spidev0.0",
        ProbeKind::Sx130xSpi,
        Answer {
            chip: Some("SX1302".into()),
            ..Default::default()
        },
    ));
    let mut run = Run::new(&bench);
    run.decisions
        .assigned
        .insert("usb-1a86_7523-1-1.5".into(), Use::Modbus);
    let spi = [hat()];
    let report = Context {
        spi: &spi,
        ..run.ctx(&f, 1_795_618_931)
    }
    .scan();
    assert_eq!(report.detected.len(), 6);
    let body = serde_json::json!({"v": 1, "radios": serde_json::to_value(&report).unwrap()});
    let errors: Vec<String> = state
        .iter_errors(&body)
        .map(|e| format!("{e} at {}", e.instance_path()))
        .collect();
    assert!(errors.is_empty(), "{errors:?}\n{body:#}");
}

// ── LoRaWAN concentrators ─────────────────────────────────────────────────

/// A RAK5146 USB, as it enumerates: the STM32 of Semtech's USB reference.
fn rak5146(port: &str) -> Usb<'_> {
    Usb::new(port, "0483", "5740").product("STM32 Virtual ComPort")
}

fn hat() -> SpiTarget {
    SpiTarget::parse("/dev/spidev0.0:17:18", Path::new("/dev/gpiochip0")).unwrap()
}

#[test]
fn a_usb_concentrator_is_named_and_pinged() {
    let f = Fixture::new();
    f.tty(&rak5146("1-1.3"), 0, "ttyACM0");
    let bench = Bench::default().answering("ttyACM0", ProbeKind::Sx130xUsb, "01.00.02");
    let r = Run::new(&bench).scan(&f);
    let d = by_path(&r, "/dev/ttyACM0");
    assert_eq!(d.protocol, Some(Protocol::Lorawan));
    assert_eq!(d.ncp.as_deref(), Some("sx130x"));
    assert_eq!(d.firmware.as_deref(), Some("01.00.02"));
    assert_eq!(d.state, State::Free);
    assert_eq!(bench.asked_about("ttyACM0"), vec![ProbeKind::Sx130xUsb]);
}

#[test]
fn a_concentrator_lorawan_owns_is_never_asked() {
    let f = Fixture::new();
    f.tty(&rak5146("1-1.3"), 0, "ttyACM0");
    let bench = Bench::default().answering("ttyACM0", ProbeKind::Sx130xUsb, "01.00.02");
    let run = Run::new(&bench);
    let ctx = Context {
        claimed: &[Protocol::Lorawan],
        ..run.ctx(&f, 1)
    };
    let r = ctx.scan();
    let d = by_path(&r, "/dev/ttyACM0");
    assert_eq!(d.protocol, Some(Protocol::Lorawan));
    assert_eq!(d.state, State::InUse);
    assert!(bench.asked.borrow().is_empty());
}

#[test]
fn an_stm32_that_is_not_the_reference_is_not_a_concentrator() {
    let f = Fixture::new();
    f.tty(
        &Usb::new("1-1.3", "0483", "5740").product("Some Printer"),
        0,
        "ttyACM0",
    );
    let bench = Bench::default();
    let r = Run::new(&bench).scan(&f);
    assert_eq!(by_path(&r, "/dev/ttyACM0").protocol, None);
    assert!(bench.asked.borrow().is_empty());
}

#[test]
fn an_spi_slot_is_listed_when_a_part_answers() {
    let f = Fixture::new();
    let bench = Bench {
        answers: vec![(
            "spidev0.0",
            ProbeKind::Sx130xSpi,
            Answer {
                chip: Some("SX1302".into()),
                ..Default::default()
            },
        )],
        ..Default::default()
    };
    let run = Run::new(&bench);
    let spi = [hat()];
    let r = Context {
        spi: &spi,
        ..run.ctx(&f, 1)
    }
    .scan();
    let d = by_path(&r, "/dev/spidev0.0");
    assert_eq!(d.id, "spi-spidev0.0");
    assert_eq!(d.label, "LoRaWAN concentrator (SX1302)");
    assert_eq!(d.protocol, Some(Protocol::Lorawan));
    assert_eq!(d.chip.as_deref(), Some("SX1302"));
    assert_eq!(d.state, State::Free);

    // Nothing on the header: not listed at all.
    let quiet = Bench::default();
    let run = Run::new(&quiet);
    assert!(Context {
        spi: &spi,
        ..run.ctx(&f, 1)
    }
    .scan()
    .detected
    .is_empty());
    assert_eq!(quiet.asked_about("spidev0.0"), vec![ProbeKind::Sx130xSpi]);
}

#[test]
fn a_scan_without_spi_keeps_the_last_spi_answer_and_never_resets() {
    let f = Fixture::new();
    let found = Bench {
        answers: vec![(
            "spidev0.0",
            ProbeKind::Sx130xSpi,
            Answer {
                chip: Some("SX1303".into()),
                ..Default::default()
            },
        )],
        ..Default::default()
    };
    let spi = [hat()];
    let first = {
        let run = Run::new(&found);
        Context {
            spi: &spi,
            ..run.ctx(&f, 1)
        }
        .scan()
    };

    // A hotplug scan: no --spi. The slot stands, and nothing was asked.
    let bench = Bench::default();
    let mut run = Run::new(&bench);
    run.previous = first.clone();
    let r = run.ctx(&f, 2).scan();
    assert_eq!(r.detected, first.detected);
    assert!(bench.asked.borrow().is_empty());

    // Once LoRaWAN is installed it owns the slot, even on a boot scan.
    let r = Context {
        spi: &spi,
        claimed: &[Protocol::Lorawan],
        ..run.ctx(&f, 3)
    }
    .scan();
    assert_eq!(by_path(&r, "/dev/spidev0.0").state, State::InUse);
    assert!(bench.asked.borrow().is_empty());
}
