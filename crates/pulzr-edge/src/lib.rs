//! Pulzr edge daemon: speaks the Pulzar protocol for the devices behind a gateway.

#[cfg(target_endian = "little")]
pub mod ble;
pub mod buffer;
pub mod config;
pub mod modbus;
pub mod mqtt;
pub mod outbox;
#[cfg(feature = "sim")]
pub mod sim;
