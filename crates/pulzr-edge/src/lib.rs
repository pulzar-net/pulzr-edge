//! Pulzr edge daemon: speaks the Pulzar protocol for the devices behind a gateway.

#[cfg(all(target_endian = "little", feature = "ble"))]
pub mod ble;
pub mod buffer;
pub mod config;
#[cfg(feature = "ip")]
pub mod ip;
#[cfg(feature = "matter")]
pub mod matter;
#[cfg(feature = "modbus")]
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
