//! pulzr-probe [scan] [--root DIR] [--report FILE] [--decisions FILE] [--lan FILE]
//! pulzr-probe identify <radio id>        a person's Identify, from the console
//! pulzr-probe assign <radio id> modbus   a person's "This is a Modbus adapter"
//! pulzr-probe assign <radio id> none     undo that
//! pulzr-probe check [FILE]               check a radios.json
//! pulzr-probe sx130x --spi SLOT           read one concentrator slot now, for
//!                                         the LoRaWAN stack's own detection
//!
//!   --spi <spidev>[:<reset>[:<power>[:<companion reset>...]]]
//!                                         a concentrator slot on SPI, the
//!                                         lines as offsets on --gpiochip
//!                                         (default /dev/gpiochip0); repeatable
//!   --claimed <protocol>                  its stack owns these radios: name
//!                                         them, never ask; repeatable
//!
//! Writes the report (`{"detected": [...]}`) and prints it. Hotplug runs
//! `scan` on every tty and bluetooth event and once at boot; it is the only
//! thing on the gateway that opens a serial port nobody has claimed. Only the
//! boot scan is given `--spi`: a part on SPI does not come and go, and its
//! read pulses the module's reset.

use pulzr_probe::detect::{parse_umdns, Context, Decisions, Paths, Report, Use};
use pulzr_probe::probe::{first_answer, Builtin, Outcome, Port};
use pulzr_probe::sx130x::SpiTarget;
use pulzr_probe::table::{ProbeKind, Protocol, Table};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

struct Args {
    command: String,
    rest: Vec<String>,
    root: PathBuf,
    paths: Paths,
    spi: Vec<SpiTarget>,
    claimed: Vec<Protocol>,
}

fn args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let mut a = Args {
        command: "scan".into(),
        rest: Vec::new(),
        root: PathBuf::from("/"),
        paths: Paths::default(),
        spi: Vec::new(),
        claimed: Vec::new(),
    };
    let mut command = None;
    let mut spi = Vec::new();
    let mut gpiochip = PathBuf::from("/dev/gpiochip0");
    while let Some(arg) = it.next() {
        let mut value = |name: &str| it.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--root" => a.root = value("--root")?.into(),
            "--report" => a.paths.report = value("--report")?.into(),
            "--decisions" => a.paths.decisions = value("--decisions")?.into(),
            "--lan" => a.paths.lan = value("--lan")?.into(),
            "--spi" => spi.push(value("--spi")?),
            "--gpiochip" => gpiochip = value("--gpiochip")?.into(),
            "--claimed" => {
                let p = value("--claimed")?;
                a.claimed.push(
                    serde_json::from_value(serde_json::Value::String(p.clone()))
                        .map_err(|_| format!("--claimed {p}: not a protocol"))?,
                );
            }
            "--version" | "-V" => {
                println!("pulzr-probe {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            s if s.starts_with('-') => return Err(format!("unknown option {s}")),
            s if command.is_none() => command = Some(s.to_owned()),
            s => a.rest.push(s.to_owned()),
        }
    }
    if let Some(c) = command {
        a.command = c;
    }
    for s in &spi {
        a.spi.push(SpiTarget::parse(s, &gpiochip)?);
    }
    Ok(a)
}

fn read_json<T: serde::de::DeserializeOwned + Default>(path: &Path) -> T {
    fs::read_to_string(path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

/// Written whole or not at all: the claim report reads this file at any
/// moment.
fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), String> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let tmp = path.with_extension("tmp");
    let mut f = File::create(&tmp).map_err(|e| format!("{}: {e}", tmp.display()))?;
    serde_json::to_writer(&mut f, value).map_err(|e| e.to_string())?;
    f.write_all(b"\n")
        .and_then(|_| f.sync_all())
        .map_err(|e| e.to_string())?;
    fs::rename(&tmp, path).map_err(|e| format!("{}: {e}", path.display()))
}

/// One pulzr-probe at a time: hotplug fires one per event, and two probes on
/// one port at once would each read the other's answer.
fn lock(paths: &Paths) -> Result<File, String> {
    let file = paths.report.with_extension("lock");
    if let Some(dir) = file.parent() {
        fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let f = File::create(&file).map_err(|e| format!("{}: {e}", file.display()))?;
    f.lock().map_err(|e| format!("{}: {e}", file.display()))?;
    Ok(f)
}

fn run() -> Result<(), String> {
    let a = args()?;
    if a.command == "check" {
        let text = match a.rest.first() {
            Some(f) => fs::read_to_string(f).map_err(|e| format!("{f}: {e}"))?,
            None => pulzr_probe::table::BUILTIN.to_owned(),
        };
        Table::parse(&text)?;
        println!("ok");
        return Ok(());
    }
    let table = Table::builtin();
    let prober = Builtin;
    let _lock = lock(&a.paths)?;
    if a.command == "sx130x" {
        let [target] = a.spi.as_slice() else {
            return Err("sx130x takes one --spi".to_owned());
        };
        return match first_answer(&prober, &[ProbeKind::Sx130xSpi], Port::Spi(target)) {
            Outcome::Answered(found) => {
                println!(
                    "{}",
                    serde_json::json!({ "chip": found.chip, "path": target.dev })
                );
                Ok(())
            }
            Outcome::NoAnswer => Err(format!(
                "{}: no concentrator answered",
                target.dev.display()
            )),
            Outcome::Unavailable => Err("this build cannot read SPI".to_owned()),
            Outcome::Error(e) => Err(e),
        };
    }
    let mut decisions: Decisions = read_json(&a.paths.decisions);
    let previous: Report = read_json(&a.paths.report);
    let lan = fs::read_to_string(&a.paths.lan)
        .map(|t| parse_umdns(&t))
        .unwrap_or_default();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    match a.command.as_str() {
        "scan" => {}
        "identify" => {
            let id = a.rest.first().ok_or("identify needs a radio id")?;
            let ctx = Context {
                root: &a.root,
                table: &table,
                prober: &prober,
                decisions: &decisions,
                previous: &previous,
                lan: &lan,
                spi: &[],
                claimed: &a.claimed,
                now,
            };
            match ctx.identify(id)? {
                Some(found) => {
                    decisions.identified.insert(id.clone(), found);
                    write_json(&a.paths.decisions, &decisions)?;
                }
                None => return Err(format!("{id} did not answer as any radio")),
            }
        }
        "assign" => {
            let id = a.rest.first().ok_or("assign needs a radio id")?;
            match a.rest.get(1).map(String::as_str) {
                Some("modbus") => {
                    decisions.assigned.insert(id.clone(), Use::Modbus);
                    decisions.identified.remove(id);
                }
                Some("none") => {
                    decisions.assigned.remove(id);
                }
                _ => return Err("assign takes modbus or none".to_owned()),
            }
            write_json(&a.paths.decisions, &decisions)?;
        }
        other => return Err(format!("unknown command {other}")),
    }

    let ctx = Context {
        root: &a.root,
        table: &table,
        prober: &prober,
        decisions: &decisions,
        previous: &previous,
        lan: &lan,
        spi: &a.spi,
        claimed: &a.claimed,
        now,
    };
    let report = ctx.scan();
    write_json(&a.paths.report, &report)?;
    println!(
        "{}",
        serde_json::to_string(&report).map_err(|e| e.to_string())?
    );
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("pulzr-probe: {e}");
            ExitCode::FAILURE
        }
    }
}
