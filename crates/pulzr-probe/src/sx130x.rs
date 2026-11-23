//! LoRaWAN concentrators: a Semtech SX1301, SX1302 or SX1303, on SPI or
//! behind the USB reference design's STM32.
//!
//! In pulzr-probe itself, as Spinel is: the stack that runs a concentrator is
//! ChirpStack Concentratord (PulzrOS's pulzr-lorawan add-on), which is the
//! thing being offered, so a gateway without it must still be able to tell
//! that it has one. Both exchanges are the HAL's own (Semtech's sx1302_hal):
//!
//! - USB: the MCU's `REQ_PING` (`loragw_mcu.c`), which answers its unique id
//!   and firmware version and changes nothing.
//! - SPI: the chip version register, after one reset with the pins the caller
//!   names. SPI has no discovery, so reading the part is the only answer; but
//!   only with those pins, never a sweep: trying lines until one resets a chip
//!   is how a Waveshare HAT's GNSS was put into standby. The lines are held
//!   for the read and handed back when it is done, which concentratord needs
//!   (it claims the same lines, and fails on a held one).

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use crate::probe::{Answer, Outcome};
use crate::table::Protocol;
use std::path::{Path, PathBuf};

/// The parts that answer, by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chip {
    Sx1301,
    Sx1302,
    Sx1303,
}

impl Chip {
    pub fn name(self) -> &'static str {
        match self {
            Chip::Sx1301 => "SX1301",
            Chip::Sx1302 => "SX1302",
            Chip::Sx1303 => "SX1303",
        }
    }
}

/// An SX1302/SX1303 read of `COMMON_VERSION` (0x5606): SPI mux target 0,
/// read bit clear, the address, and a dummy byte; the part shifts the value
/// out during a fifth (`loragw_spi.c`: `command_size = 5`, data in byte 4).
pub const SX1302_READ_VERSION: [u8; 5] = [0x00, 0x56, 0x06, 0x00, 0x00];
/// An SX1301 read of register 1, its version: a different HAL, two bytes.
pub const SX1301_READ_VERSION: [u8; 2] = [0x01, 0x00];

/// The part a version read names. Only versions a real part gives: a Pi with
/// nothing on the header reads a constant (0xa2 on spidev0.1 was measured)
/// for every register, and 0x00 or 0xff is a bus with nothing on it.
pub fn chip_of(sx1302: u8, sx1301: Option<u8>) -> Option<Chip> {
    match (sx1302, sx1301) {
        (0x10, _) => Some(Chip::Sx1302),
        (0x12, _) => Some(Chip::Sx1303),
        (_, Some(0x67)) => Some(Chip::Sx1301),
        _ => None,
    }
}

const ORDER_REQ_PING: u8 = 0x00;
const ORDER_ACK_PING: u8 = 0x40;
/// `ACK_PING_SIZE`: a 12-byte unique id, then the version in 9 bytes.
const ACK_PING_SIZE: usize = 21;

/// `write_req(REQ_PING)`: a request id, a 16-bit payload size (none), the
/// order.
pub fn ping_request(id: u8) -> [u8; 4] {
    [id, 0, 0, ORDER_REQ_PING]
}

/// The firmware version in an `ACK_PING`, once all of it is in `bytes`.
pub fn ping_answer(bytes: &[u8]) -> Option<String> {
    let [_, msb, lsb, order, payload @ ..] = bytes else {
        return None;
    };
    let size = usize::from(*msb) << 8 | usize::from(*lsb);
    if *order != ORDER_ACK_PING || size < ACK_PING_SIZE || payload.len() < ACK_PING_SIZE {
        return None;
    }
    let version: String = payload[12..ACK_PING_SIZE]
        .iter()
        .take_while(|&&b| b != 0)
        .map(|&b| b as char)
        .filter(|c| c.is_ascii_graphic())
        .collect();
    (!version.is_empty()).then_some(version)
}

fn answered(chip: Option<Chip>, firmware: Option<String>) -> Outcome {
    Outcome::Answered(Answer {
        protocol: Some(Protocol::Lorawan),
        ncp: Some("sx130x"),
        firmware,
        ezsp: None,
        chip: chip.map(|c| c.name().to_owned()),
    })
}

/// A concentrator slot on SPI, as the board wires it: the spidev, and the
/// GPIO lines (offsets on `chip`) that hold the module's supply and reset.
/// `companions` are the active-low resets of the parts beside the SX130x on a
/// CoreCell-style module (the SX1261, the AD5338R that sets transmit power),
/// which must be out of reset for it to come up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiTarget {
    pub dev: PathBuf,
    pub chip: PathBuf,
    pub reset: Option<u32>,
    pub power: Option<u32>,
    pub companions: Vec<u32>,
}

const SPEC: &str = "<spidev>[:<reset>[:<power>[:<companion reset>...]]]";

impl SpiTarget {
    /// `<spidev>[:<reset>[:<power>[:<companion reset>...]]]`, the lines as
    /// offsets on `chip`; an empty or `-` field is none.
    pub fn parse(spec: &str, chip: &Path) -> Result<SpiTarget, String> {
        let mut it = spec.split(':');
        let dev = it
            .next()
            .filter(|d| d.starts_with('/'))
            .ok_or_else(|| format!("--spi {spec}: wants {SPEC}"))?;
        let line = |s: Option<&str>| -> Result<Option<u32>, String> {
            match s {
                None | Some("") | Some("-") => Ok(None),
                Some(n) => n
                    .parse()
                    .map(Some)
                    .map_err(|_| format!("--spi {spec}: {n} is not a GPIO line")),
            }
        };
        let reset = line(it.next())?;
        let power = line(it.next())?;
        let mut companions = Vec::new();
        for c in it {
            companions.extend(line(Some(c))?);
        }
        if companions.len() > 4 {
            return Err(format!("--spi {spec}: wants {SPEC}"));
        }
        Ok(SpiTarget {
            dev: dev.into(),
            chip: chip.into(),
            reset,
            power,
            companions,
        })
    }

    /// The report's id for it: `spi-spidev0.0`.
    pub fn id(&self) -> String {
        let name = self
            .dev
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        format!("spi-{name}")
    }
}

#[cfg(not(target_os = "linux"))]
pub fn probe_usb(_path: &Path) -> Outcome {
    Outcome::Unavailable
}

#[cfg(not(target_os = "linux"))]
pub fn probe_spi(_target: &SpiTarget) -> Outcome {
    Outcome::Unavailable
}

#[cfg(target_os = "linux")]
pub use linux::{probe_spi, probe_usb};

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    /// The USB reference answers at once; a second is plenty.
    const PING_WAIT: Duration = Duration::from_millis(1000);
    /// What the HAL's reset script and PulzrOS's waited between edges.
    const SETTLE: Duration = Duration::from_millis(100);
    /// The HAL's default SPI clock.
    const SPI_HZ: u32 = 2_000_000;

    /// Ask the STM32 behind a USB concentrator for its firmware. A CDC-ACM
    /// port ignores the baud rate; 115200 is what the HAL sets.
    pub fn probe_usb(path: &Path) -> Outcome {
        let mut file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(f) => f,
            Err(e) => return Outcome::Error(e.to_string()),
        };
        if let Err(e) = set_raw(&file) {
            return Outcome::Error(e.to_string());
        }
        if let Err(e) = file.write_all(&ping_request(0x5a)) {
            return Outcome::Error(e.to_string());
        }
        let start = Instant::now();
        let mut got = Vec::new();
        let mut buf = [0u8; 64];
        while start.elapsed() < PING_WAIT && got.len() < 256 {
            match file.read(&mut buf) {
                Ok(n) => got.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
            if let Some(version) = ping_answer(&got) {
                // The USB reference carries an SX1302 or an SX1303, and says
                // which only to a full HAL start; the firmware is enough to
                // know it is one.
                return answered(None, Some(version));
            }
        }
        Outcome::NoAnswer
    }

    fn set_raw(file: &File) -> std::io::Result<()> {
        let fd = file.as_raw_fd();
        // SAFETY: termios is plain data, and the fd is open for the call's length.
        unsafe {
            let mut t: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(fd, &mut t) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            libc::cfmakeraw(&mut t);
            libc::cfsetispeed(&mut t, libc::B115200);
            libc::cfsetospeed(&mut t, libc::B115200);
            t.c_cflag |= libc::CLOCAL | libc::CREAD;
            t.c_cflag &= !libc::CRTSCTS;
            t.c_cc[libc::VMIN] = 0;
            t.c_cc[libc::VTIME] = 1;
            if libc::tcsetattr(fd, libc::TCSANOW, &t) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            libc::tcflush(fd, libc::TCIOFLUSH);
        }
        Ok(())
    }

    /// Reset the part with the target's lines, read its version, hand the
    /// lines back.
    pub fn probe_spi(target: &SpiTarget) -> Outcome {
        let spi = match OpenOptions::new().read(true).write(true).open(&target.dev) {
            Ok(f) => f,
            Err(e) => return Outcome::Error(format!("{}: {e}", target.dev.display())),
        };
        if let Err(e) = spi_setup(&spi) {
            return Outcome::Error(format!("{}: {e}", target.dev.display()));
        }
        // Held until the read is done; dropping it releases them.
        let _lines = match reset(target) {
            Ok(l) => l,
            Err(e) => return Outcome::Error(format!("{}: {e}", target.chip.display())),
        };
        let sx1302 = match transfer(&spi, &SX1302_READ_VERSION) {
            Ok(rx) => rx[4],
            Err(e) => return Outcome::Error(e.to_string()),
        };
        let sx1301 = match chip_of(sx1302, None) {
            Some(_) => None,
            None => transfer(&spi, &SX1301_READ_VERSION).ok().map(|rx| rx[1]),
        };
        match chip_of(sx1302, sx1301) {
            Some(chip) => answered(Some(chip), None),
            None => Outcome::NoAnswer,
        }
    }

    // ── ioctl numbers ──────────────────────────────────────────────────────
    //
    // linux/ioctl.h: MIPS and PowerPC put three direction bits at 29 and
    // number WRITE 4; the rest put two at 30 and number WRITE 1.
    #[cfg(any(
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "powerpc",
        target_arch = "powerpc64"
    ))]
    const IOC: (u32, u32, u32) = (29, 4, 2);
    #[cfg(not(any(
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "powerpc",
        target_arch = "powerpc64"
    )))]
    const IOC: (u32, u32, u32) = (30, 1, 2);

    const fn ioc(dir: u32, ty: u8, nr: u8, size: usize) -> u32 {
        (dir << IOC.0) | ((size as u32) << 16) | ((ty as u32) << 8) | nr as u32
    }
    const fn iow(ty: u8, nr: u8, size: usize) -> u32 {
        ioc(IOC.1, ty, nr, size)
    }
    const fn iowr(ty: u8, nr: u8, size: usize) -> u32 {
        ioc(IOC.1 | IOC.2, ty, nr, size)
    }

    fn ioctl<T>(fd: RawFd, request: u32, arg: *mut T) -> std::io::Result<()> {
        // SAFETY: the caller passes the argument type the request is sized for.
        if unsafe { libc::ioctl(fd, request as libc::Ioctl, arg) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    // ── spidev (linux/spi/spidev.h) ────────────────────────────────────────

    #[repr(C)]
    #[derive(Default)]
    struct SpiIocTransfer {
        tx_buf: u64,
        rx_buf: u64,
        len: u32,
        speed_hz: u32,
        delay_usecs: u16,
        bits_per_word: u8,
        cs_change: u8,
        tx_nbits: u8,
        rx_nbits: u8,
        word_delay_usecs: u8,
        pad: u8,
    }

    const SPI_IOC_WR_MODE: u32 = iow(b'k', 1, 1);
    const SPI_IOC_WR_BITS_PER_WORD: u32 = iow(b'k', 3, 1);
    const SPI_IOC_WR_MAX_SPEED_HZ: u32 = iow(b'k', 4, 4);
    const SPI_IOC_MESSAGE_1: u32 = iow(b'k', 0, std::mem::size_of::<SpiIocTransfer>());

    fn spi_setup(spi: &File) -> std::io::Result<()> {
        let fd = spi.as_raw_fd();
        let mut mode: u8 = 0;
        let mut bits: u8 = 8;
        let mut speed: u32 = SPI_HZ;
        ioctl(fd, SPI_IOC_WR_MODE, &mut mode)?;
        ioctl(fd, SPI_IOC_WR_BITS_PER_WORD, &mut bits)?;
        ioctl(fd, SPI_IOC_WR_MAX_SPEED_HZ, &mut speed)
    }

    /// One full-duplex transfer: what came back while `tx` went out.
    fn transfer<const N: usize>(spi: &File, tx: &[u8; N]) -> std::io::Result<[u8; N]> {
        let mut rx = [0u8; N];
        let mut t = SpiIocTransfer {
            tx_buf: tx.as_ptr() as u64,
            rx_buf: rx.as_mut_ptr() as u64,
            len: N as u32,
            speed_hz: SPI_HZ,
            bits_per_word: 8,
            ..Default::default()
        };
        ioctl(spi.as_raw_fd(), SPI_IOC_MESSAGE_1, &mut t)?;
        Ok(rx)
    }

    // ── GPIO character device, v2 (linux/gpio.h) ───────────────────────────

    const GPIO_V2_LINES_MAX: usize = 64;
    const GPIO_V2_LINE_NUM_ATTRS_MAX: usize = 10;
    const GPIO_V2_LINE_FLAG_OUTPUT: u64 = 1 << 3;
    const GPIO_V2_LINE_ATTR_ID_OUTPUT_VALUES: u32 = 2;

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct LineAttribute {
        id: u32,
        padding: u32,
        /// The union of flags, values and debounce_period_us: values here.
        values: u64,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct LineConfigAttribute {
        attr: LineAttribute,
        mask: u64,
    }

    #[repr(C)]
    struct LineConfig {
        flags: u64,
        num_attrs: u32,
        padding: [u32; 5],
        attrs: [LineConfigAttribute; GPIO_V2_LINE_NUM_ATTRS_MAX],
    }

    #[repr(C)]
    struct LineRequest {
        offsets: [u32; GPIO_V2_LINES_MAX],
        consumer: [u8; 32],
        config: LineConfig,
        num_lines: u32,
        event_buffer_size: u32,
        padding: [u32; 5],
        fd: i32,
    }

    #[repr(C)]
    struct LineValues {
        bits: u64,
        mask: u64,
    }

    const GPIO_V2_GET_LINE_IOCTL: u32 = iowr(0xb4, 0x07, std::mem::size_of::<LineRequest>());
    const GPIO_V2_LINE_SET_VALUES_IOCTL: u32 = iowr(0xb4, 0x0f, std::mem::size_of::<LineValues>());

    /// Lines requested as outputs, low, held while the fd is open.
    fn request(chip: &Path, offsets: &[u32]) -> std::io::Result<OwnedFd> {
        let dev = File::open(chip)?;
        let mut req = LineRequest {
            offsets: [0; GPIO_V2_LINES_MAX],
            consumer: [0; 32],
            config: LineConfig {
                flags: GPIO_V2_LINE_FLAG_OUTPUT,
                num_attrs: 1,
                padding: [0; 5],
                attrs: [LineConfigAttribute::default(); GPIO_V2_LINE_NUM_ATTRS_MAX],
            },
            num_lines: offsets.len() as u32,
            event_buffer_size: 0,
            padding: [0; 5],
            fd: -1,
        };
        req.offsets[..offsets.len()].copy_from_slice(offsets);
        req.consumer[..11].copy_from_slice(b"pulzr-probe");
        req.config.attrs[0] = LineConfigAttribute {
            attr: LineAttribute {
                id: GPIO_V2_LINE_ATTR_ID_OUTPUT_VALUES,
                padding: 0,
                values: 0,
            },
            mask: (1u64 << offsets.len()) - 1,
        };
        ioctl(dev.as_raw_fd(), GPIO_V2_GET_LINE_IOCTL, &mut req)?;
        // SAFETY: the kernel returned a new fd for the request, ours to own.
        Ok(unsafe { OwnedFd::from_raw_fd(req.fd) })
    }

    fn set(lines: &OwnedFd, index: usize, high: bool) -> std::io::Result<()> {
        let mut v = LineValues {
            bits: if high { 1 << index } else { 0 },
            mask: 1 << index,
        };
        ioctl(lines.as_raw_fd(), GPIO_V2_LINE_SET_VALUES_IOCTL, &mut v)
    }

    /// Supply on, then the SX130x's reset high and low, then each
    /// companion's low and high, as Semtech's `reset_lgw.sh` does. None when
    /// the board names no lines: the part is read as it is.
    fn reset(target: &SpiTarget) -> std::io::Result<Option<OwnedFd>> {
        let mut offsets = Vec::new();
        offsets.extend(target.power);
        offsets.extend(target.reset);
        offsets.extend(&target.companions);
        if offsets.is_empty() {
            return Ok(None);
        }
        let lines = request(&target.chip, &offsets)?;
        let mut i = 0;
        if target.power.is_some() {
            set(&lines, i, true)?;
            sleep(SETTLE);
            i += 1;
        }
        if target.reset.is_some() {
            set(&lines, i, true)?;
            sleep(SETTLE);
            set(&lines, i, false)?;
            sleep(SETTLE);
            i += 1;
        }
        for _ in &target.companions {
            set(&lines, i, false)?;
            sleep(SETTLE);
            set(&lines, i, true)?;
            sleep(SETTLE);
            i += 1;
        }
        Ok(Some(lines))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ping_is_the_hals() {
        // write_req(fd, ORDER_ID__REQ_PING, NULL, 0): id, size 0, order 0.
        assert_eq!(ping_request(7), [7, 0, 0, 0]);
    }

    #[test]
    fn an_ack_ping() {
        let mut ack = vec![7, 0, 21, 0x40];
        ack.extend([0xaa; 12]);
        ack.extend(b"01.00.02\0");
        assert_eq!(ping_answer(&ack).as_deref(), Some("01.00.02"));
        // Not all of it yet, the wrong order, or no version: no answer.
        assert_eq!(ping_answer(&ack[..20]), None);
        let mut wrong = ack.clone();
        wrong[3] = 0x41;
        assert_eq!(ping_answer(&wrong), None);
        let mut blank = ack.clone();
        blank[16..].fill(0);
        assert_eq!(ping_answer(&blank), None);
    }

    #[test]
    fn only_real_versions_name_a_part() {
        assert_eq!(chip_of(0x10, None), Some(Chip::Sx1302));
        assert_eq!(chip_of(0x12, None), Some(Chip::Sx1303));
        assert_eq!(chip_of(0x00, Some(0x67)), Some(Chip::Sx1301));
        // An empty header, and the constant an unconnected spidev0.1 gave.
        assert_eq!(chip_of(0xff, Some(0xff)), None);
        assert_eq!(chip_of(0xa2, Some(0xa2)), None);
    }

    #[test]
    fn spi_targets() {
        let chip = Path::new("/dev/gpiochip0");
        let t = SpiTarget::parse("/dev/spidev0.0:17:18", chip).unwrap();
        assert_eq!((t.reset, t.power), (Some(17), Some(18)));
        assert_eq!(t.id(), "spi-spidev0.0");
        assert!(t.companions.is_empty());
        let t = SpiTarget::parse("/dev/spidev0.1:-", chip).unwrap();
        assert_eq!((t.reset, t.power), (None, None));
        // A Waveshare SX1302 HAT: reset 23, power 18, SX1261 22, AD5338R 13.
        let t = SpiTarget::parse("/dev/spidev0.0:23:18:22:13", chip).unwrap();
        assert_eq!(t.companions, vec![22, 13]);
        assert!(SpiTarget::parse("spidev0.0", chip).is_err());
        assert!(SpiTarget::parse("/dev/spidev0.0:x", chip).is_err());
        assert!(SpiTarget::parse("/dev/spidev0.0:1:2:3:4:5:6:7", chip).is_err());
    }
}
