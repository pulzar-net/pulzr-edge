//! pulzr-probe: what radios are plugged into this gateway.
//!
//! 1. Read each USB serial port's and Bluetooth controller's identity from
//!    sysfs ([`sysfs`]).
//! 2. Name it from the table of known adapters ([`table`], `radios.json`).
//! 3. Ask it what it runs, only if the table named it and nothing has it open
//!    ([`detect`], through a stack's probe, [`probe`]).
//!
//! The result is `radios.detected` (pulzr-protocol's `detectedRadio`), which
//! PulzrOS puts in its claim report and its retained MQTT state.

pub mod detect;
pub mod probe;
pub mod sysfs;
pub mod table;
