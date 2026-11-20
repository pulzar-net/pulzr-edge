//! Pulzr edge daemon: speaks the Pulzar protocol for the devices behind a gateway.

#[cfg(target_endian = "little")]
pub mod ble;
pub mod buffer;
pub mod camera;
pub mod config;
#[cfg(feature = "matter")]
pub mod matter;
pub mod modbus;
pub mod mqtt;
pub mod outbox;
pub mod radio;
#[cfg(feature = "sim")]
pub mod sim;
#[cfg(feature = "zigbee")]
pub mod zigbee;
#[cfg(feature = "zwave")]
pub mod zwave;
