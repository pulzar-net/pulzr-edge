//! What the kernel says is plugged in, read from sysfs and never guessed from
//! a port's name: `ttyUSB0` today is somebody else's adapter after a replug.
//!
//! Every path is under a root, `/` on a gateway and a fixture tree in tests.

use std::fs;
use std::path::{Path, PathBuf};

/// A USB device's identity, from its device directory (`.../1-1.2`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Usb {
    pub vid: String,
    pub pid: String,
    pub manufacturer: Option<String>,
    pub product: Option<String>,
    pub serial: Option<String>,
    /// The physical port path, `1-1.2`: where it is plugged in.
    pub port: String,
    pub interface: Option<u8>,
}

/// A serial port a USB device provides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerialPort {
    /// `ttyUSB0`, `ttyACM0`.
    pub name: String,
    pub usb: Usb,
}

/// A Bluetooth controller on USB. An onboard one (a Pi's, on a UART) is the
/// board's to declare and is not listed here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hci {
    /// `hci0`.
    pub name: String,
    pub usb: Usb,
    pub address: Option<String>,
}

fn read(path: &Path) -> Option<String> {
    let s = fs::read_to_string(path).ok()?;
    let s = s.trim();
    (!s.is_empty()).then(|| s.chars().filter(|c| !c.is_control()).take(128).collect())
}

/// Walks up from a class device to the USB device that owns it: the first
/// ancestor with `idVendor`. On the way it passes the interface directory
/// (`1-1.2:1.0`), whose `bInterfaceNumber` says which port of a multi-port
/// device this is.
fn usb_of(device: &Path, root: &Path) -> Option<Usb> {
    let mut dir = fs::canonicalize(device).ok()?;
    let root = fs::canonicalize(root).ok()?;
    let mut interface = None;
    while dir.starts_with(&root) && dir != root {
        if interface.is_none() {
            if let Some(n) = read(&dir.join("bInterfaceNumber")) {
                interface = u8::from_str_radix(&n, 16).ok();
            }
        }
        if let Some(vid) = read(&dir.join("idVendor")) {
            let pid = read(&dir.join("idProduct"))?;
            let port = dir.file_name()?.to_string_lossy().into_owned();
            return Some(Usb {
                vid: vid.to_lowercase(),
                pid: pid.to_lowercase(),
                manufacturer: read(&dir.join("manufacturer")),
                product: read(&dir.join("product")),
                serial: read(&dir.join("serial")),
                port,
                interface,
            });
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

fn class(root: &Path, class: &str) -> Vec<(String, PathBuf)> {
    let dir = root.join("sys/class").join(class);
    let mut out: Vec<(String, PathBuf)> = fs::read_dir(&dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| (e.file_name().to_string_lossy().into_owned(), e.path()))
        .collect();
    out.sort();
    out
}

/// Every USB serial port: `ttyUSB*` (usb-serial drivers: cp210x, ch341,
/// ftdi_sio, pl2303) and `ttyACM*` (cdc-acm).
pub fn serial_ports(root: &Path) -> Vec<SerialPort> {
    class(root, "tty")
        .into_iter()
        .filter(|(name, _)| name.starts_with("ttyUSB") || name.starts_with("ttyACM"))
        .filter_map(|(name, path)| {
            let usb = usb_of(&path.join("device"), root)?;
            Some(SerialPort { name, usb })
        })
        .collect()
}

pub fn bluetooth(root: &Path) -> Vec<Hci> {
    class(root, "bluetooth")
        .into_iter()
        .filter(|(name, _)| name.starts_with("hci") && !name.contains(':'))
        .filter_map(|(name, path)| {
            let usb = usb_of(&path, root)?;
            let address = read(&path.join("address")).map(|a| a.to_lowercase());
            Some(Hci { name, usb, address })
        })
        .collect()
}

/// Whether a process has this device open: the Modbus daemon on its bus, a
/// stack on its coordinator, somebody's minicom. A port that is open is never
/// probed, whatever the table says, because a probe frame would land in the
/// middle of whatever that process is saying.
pub fn open_elsewhere(root: &Path, dev: &Path) -> bool {
    let Ok(target) = fs::canonicalize(dev) else {
        return false;
    };
    let me = std::process::id().to_string();
    for proc_entry in fs::read_dir(root.join("proc"))
        .into_iter()
        .flatten()
        .flatten()
    {
        let pid = proc_entry.file_name();
        let pid = pid.to_string_lossy();
        if !pid.bytes().all(|b| b.is_ascii_digit()) || pid == me {
            continue;
        }
        for fd in fs::read_dir(proc_entry.path().join("fd"))
            .into_iter()
            .flatten()
            .flatten()
        {
            if let Ok(link) = fs::read_link(fd.path()) {
                let link = if link.is_absolute() && root != Path::new("/") {
                    root.join(link.strip_prefix("/").unwrap_or(&link))
                } else {
                    link
                };
                if fs::canonicalize(&link).is_ok_and(|l| l == target) {
                    return true;
                }
            }
        }
    }
    false
}
