# pulzr-edge

The Rust daemon that runs on a PulzarOS gateway and speaks the
[Pulzar protocol](https://github.com/pulzar-net/pulzr-protocol) for the devices
behind the gateway. One program, one MQTT 5 session to the platform broker, a
module per bus. The first module is **Modbus RTU and Modbus TCP**; Bluetooth and
Zigbee follow as further modules.

```
 Modbus RTU (RS-485) ─┐
 Modbus TCP (LAN) ────┼─► pulzr-edge ─► disk buffer ─► MQTT 5 + TLS ─► platform broker
                      │      ▲
            cmd / cmd/res ───┘   settings.set, register.write
```

Not deployed anywhere yet. PulzarOS packages it (see "Before it can ship" below).

## What it does

- Each Modbus unit is a device with the key `modbus:<gateway-eui>.<bus>.<unit>`.
- Polls each unit on its register map's schedule, one request in flight per bus
  (polls and writes never interleave), with a timeout, retries and backoff.
- Reports a field on **change past its deadband** or when its **heartbeat** runs
  out, as `up` with a per-device `seq`, `r`, `rx` and `link.modbus`.
- Publishes per-unit `state` (`online`, `last_heard`, `package`) when a unit stops
  answering (after `offline_after` failed polls) and when it comes back.
- Takes `cmd` for `settings.set` and `register.write`, only for registers the map
  marks writable, and answers `cmd/res` with `queued`, `sent`, `delivered`, `ok`
  (write echoed by the device), or `refused` (`invalid`, `unknown`) / `failed`
  (`expired`, `busy`, `undeliverable`, `error`). A successful write also publishes
  `settings` with `by` set to the command id.
- Everything it publishes goes through a bounded on-disk queue first. While the
  broker is away messages wait; on reconnect they are sent oldest first, one at a
  time, each until its PubAck. `seq` is kept per device on disk and only moves
  forward across restarts (gapless after a clean stop; after a crash it skips
  ahead, which the platform's `seq` dedup accepts).

Topics are `pulzr/a/<adapter>/d/<key>/{up,state,settings,cmd/res}` out and
`pulzr/a/<adapter>/d/<key>/cmd` in. The adapter id defaults to
`edge-<gateway-eui>`; it must match the name in the client certificate.

## Layout

| Path | What |
|---|---|
| `crates/pulzr-protocol` | message types generated at build time from `schema/protocol.schema.json` with [typify](https://crates.io/crates/typify); round-trip and schema tests against the protocol repo's examples |
| `crates/pulzr-edge` | the daemon: `config`, `buffer`, `outbox`, `mqtt`, `modbus/{regmap,codec,track,plan,bus}` |
| `crates/pulzr-probe` | names the radios plugged into a gateway; `radios.json` is its table of known adapters |
| `schema/register-map.schema.json` | JSON Schema of the register map, generated from the Rust types |
| `examples/` | a config and one register map for a made-up energy meter |

The protocol crate keeps a copy of the protocol schema. To take a new version of
the contract, copy `schema/protocol.schema.json` and the `examples/` from the
protocol repo into `crates/pulzr-protocol/{schema,tests/fixtures}` and run the
tests. Cross-field rules the Rust types cannot carry (`seq` or `dedup`; a reason
with `refused` and `failed`) are stripped for generation and checked against the
schema in the tests; the daemon builds its messages through the generated types so
the string patterns (ids, keys, reading names) are enforced when it publishes.

## Configuration

`pulzr-edge <config.toml>`. See `examples/config.toml`.

```toml
gateway_eui = "aabbccddeeff0011"
state_dir = "/var/lib/pulzr-edge"       # buffer and seq counters

[broker]
host = "mqtt.example.net"
port = 8883
ca = "/etc/ssl/certs/ca-certificates.crt"
username = "aabbccddeeff0011"            # the gateway's broker login ...
password_file = "/var/run/pulzr-edge/password"
# ... or a client certificate of its own instead of the two lines above:
# cert = "/etc/pulzr-edge/client.pem"
# key = "/etc/pulzr-edge/client.key"

[buffer]
max_bytes = 8388608

[[bus]]                                  # RS-485
id = "rs485-0"
kind = "rtu"
serial = "/dev/ttyUSB0"
baud = 9600                              # parity "none"|"even"|"odd", data_bits, stop_bits
timeout_ms = 1000
retries = 2

[[bus.unit]]
id = 17
map = "maps/generic-energy-meter.toml"   # relative to the config file

[[bus]]                                  # Modbus TCP
id = "lan-0"
kind = "tcp"
address = "192.168.1.50:502"

[[bus.unit]]
id = 1
map = "maps/generic-energy-meter.toml"
```

Broker auth is `cert` + `key` (mutual TLS) or `username` + `password_file`, never
both and never neither; config load refuses it otherwise. `password_file` holds
the password alone (a trailing newline is trimmed) and is read at every connect,
so a rotated token is picked up on the next reconnect without a restart. With
either, TLS verifies the server against `ca`. `tls = false` in `[broker]` is for a local test broker only. Put `state_dir` on
storage that survives reboots and tolerates writes (the buffer is append-only and
the `seq` file is rewritten once per 1000 messages per device, but a small flash
overlay is still the wrong place).

## Register map

TOML or JSON (by file extension), validated on load and described by
[`schema/register-map.schema.json`](schema/register-map.schema.json). It is the
Modbus part of a device package: which registers exist and what they mean.

```toml
# ILLUSTRATIVE: a made-up energy meter. Addresses and scaling are invented to show the
# format, not taken from any vendor's documentation.
schema = 0
model = "example-energy-meter"
poll_interval = 10
heartbeat = 300

[[fields]]
id = "power"             # reading key in `up`
function = 4             # read: 1 coils, 2 discrete inputs, 3 holding, 4 input registers
address = 4              # zero-based, as on the wire
type = "f32"             # u16 i16 u32 i32 f32 bool
unit = "W"
deadband = 10            # report when it moved by more than this (0 = any change)
poll_interval = 5

[[fields]]
id = "energy"
function = 4
address = 6
type = "u32"
word_order = "little"    # words: big (AB CD, default) or little (CD AB)
byte_order = "big"       # bytes inside a word: big (default) or little
scale = 0.01             # engineering value = raw * scale + offset
unit = "kWh"
heartbeat = 900

[[fields]]
id = "report_period"
function = 3
address = 100
type = "u16"
writable = true          # settings.set / register.write may change it
min = 1                  # limits applied to writes
max = 3600
```

The full example is `examples/maps/generic-energy-meter.toml`.

- A 32 bit value on the wire as `12 34 56 78` is `word_order = big, byte_order =
  big`; `56 78 12 34` is `little` words; `34 12 78 56` is `little` bytes; both
  reversed is `78 56 34 12`.
- `bool` is a coil or discrete input (functions 1, 2), or a register that is
  non-zero (3, 4), or one bit of a register with `bit = n`.
- Writable: functions 1 and 3 only. The write function is derived (5 for a coil,
  6 for one register, 16 for two) and `write_function` overrides it (5, 15, 6, 16)
  for devices that insist on one.
- Fields within `max_gap` registers (default 4) of each other, same function and
  poll interval, share one request.

### Commands

```json
{ "v": 1, "id": "c7e1a2b3-0001", "name": "settings.set", "params": { "report_period": 60, "relay": true } }
{ "v": 1, "id": "c7e1a2b3-0002", "name": "register.write", "params": { "field": "report_period", "value": 60 } }
{ "v": 1, "id": "c7e1a2b3-0003", "name": "register.write", "params": { "address": 100, "function": 3, "value": 60 } }
```

All of a command's writes are checked before the first one is made; any field that
is not writable, unknown, out of range or of the wrong type refuses the whole
command with `invalid`.

## Simulator

`pulzr-edge-sim` is a Modbus TCP device simulator for testing the whole Modbus path
on a gateway without hardware. It reads the same register maps as the daemon and
uses the daemon's own codec, so word order, byte order, scale and types match.

```sh
pulzr-edge-sim --listen 127.0.0.1:5020 --unit 17 --map examples/maps/generic-energy-meter.toml \
               [--unit 18 --map other.toml]...
```

Point a `tcp` bus of the daemon at it. Readable fields follow a slow sine or random
walk inside the field's `min`/`max` (else 0 to 100, widened to cross the deadband);
bools toggle now and then. Writable fields (functions 1 and 3) accept functions 5, 6,
15 and 16 and keep the value, so `settings.set` and `register.write` round-trip; any
other write answers illegal data address. Every write is logged (`RUST_LOG=info`).
Build it from source with `cargo build --release --features sim --bin pulzr-edge-sim`;
releases publish `pulzr-edge-sim-<arch>-linux-musl` next to the daemon.

## Bluetooth LE (`pulzr-edge ble`)

The gateway's own Bluetooth controller, through `bluetooth-host` from
[bluetooth-bridge](https://github.com/pulzar-net/bluetooth-bridge) (a host
stack over raw HCI: the Linux HCI user channel, H4 serial, or TCP; no BlueZ).
It runs as its own process, `pulzr-edge ble <config.toml>`, with its own
buffer under `state_dir/ble` and its own MQTT session (client id
`<adapter>-ble`) under the same adapter and login. It runs only with
`[ble] enabled = true`, and an empty `admit` list is refused unless
`admit_all = true`.

- Each device heard is `ble:<EUI-64>` (its address, or the identity its
  private address resolves to, with `fffe` in the middle). Once per
  `interval_secs` it gets one `up`: the latest advertising data (and scan
  response) as `raw`, the best RSSI of the interval in `rx`, and `link.ble`
  (`adv`, `adv_type` for legacy PDUs, `tx_power`, and with `parsed = true`
  every AD field and decoded beacon). The platform's BLE stage decodes `raw`
  as it does for every adapter.
- The controller is the device `ble:<its EUI-64>`: `state` online while the
  stack runs, and the intents `ble.advertise` (`data`, `scan_response`,
  `interval_ms`, `connectable`, `legacy`; answers `ok {set}`) and
  `ble.advertise.stop` (`set`).
- A device's key takes `gatt.discover`, `gatt.read` (`characteristic`,
  `service`) and `gatt.write` (`characteristic`, `value` base64, `response`),
  all with `secure` to pair or encrypt first: `queued`, `delivered` once
  connected, then `ok` (with `value` or `services`), `failed undeliverable`
  when this gateway has not heard it connectable or cannot connect.
- Every process of an adapter receives the adapter's commands: the Modbus
  process answers only `modbus:` keys and the BLE process only `ble:` keys.

Tests: `tests/ble.rs` checks the bodies against the protocol schema, and runs
the module end to end against Google's Bumble (a virtual controller and a
scripted KKM beacon, `tests/bumble/beacon.py`) in CI.

## pulzr-probe

What radios are plugged into this gateway, for PulzrOS's claim report and its
retained MQTT state (`radios.detected`, pulzr-protocol's `detectedRadio`).
PulzrOS ships it in every image, not as an add-on, so a gateway can say what it
has before anything is installed; it has to stay under 500 KB on flash
(squashfs, so xz-compressed: about 200 KB on aarch64; release.yml fails the
build otherwise), which is why it has no async runtime and no argument parser.
The binary itself is about 490 KB on aarch64 and 740 KB on mipsel, most of it
std's backtrace support and musl.

```sh
pulzr-probe                       # scan: write /var/run/pulzr-probe/radios.json and print it
pulzr-probe identify <radio id>   # a person's Identify: probe a port nobody could name
pulzr-probe assign <radio id> modbus|none   # "This is a Modbus adapter", or undo it
pulzr-probe check [radios.json]   # check a table
```

1. **Identity from sysfs**, never from the port's name: USB vendor and product
   id, manufacturer, product and serial strings, interface number and physical
   port path (`1-1.2`), for every `ttyUSB*`, `ttyACM*` and USB Bluetooth
   controller (`hciN`). An onboard UART or Bluetooth chip is the board's to
   declare and is not listed.
2. **Named from `radios.json`.** A match on a unique USB id (ConBee II
   `1cf1:0030`, a Z-Wave controller's `0658:0200`) or, for an id in `bridges`,
   a product string that names a radio ("Home Assistant Connect ZBT-1",
   "SLZB-07", "ConBee III"). A bridge chip (CP210x, CH9102, CH340, FTDI,
   PL2303) is never named by its id: those sit inside the RS-485 adapters
   Modbus sites use, and a probe frame on a live fieldbus is garbage on it.
   `pulzr-probe check` (and the test suite, on the table that ships) refuses
   a bridge entry with no product string, a product string under four
   characters, and two entries that could both match one device. The table
   grows by product strings, never by widening a match.
3. **Probed only when named**, only with the probes its entry lists, never
   while any process has the port open, never once a person has said it is a
   Modbus adapter. An adapter nobody could name is `unidentified` with
   `probe: not-allowed` until a person runs Identify on it from the console;
   what Identify finds is kept in `/etc/pulzr-probe/decisions.json` with the
   Modbus assignments. A radio still in the same place keeps its `since` and its
   answer and is not asked again on the next hotplug event.

What a probe asks sits behind `probe::Prober`, and `Builtin` is what ships:

| Kind | Frames | From |
|---|---|---|
| `zwave-serial` | NAK, `GET_SERIAL_API_CAPABILITIES`; then, only from a controller, its version, id, init data, capabilities and RF region (reads only) | `zwave_serial::probe` in pulzar-net/zwave-bridge (feature `zwave`, on by default) |
| `spinel` | `NCP_VERSION` and `CAPS` over HDLC-lite (reads only) | pulzr-probe's own `spinel.rs` |
| `ezsp`, `znp`, `deconz` | none yet | `zigbee-host::probe` in pulzar-net/zigbee-bridge, once tagged |

A kind with no probe built in answers `probe: unavailable`: the adapter is
reported as the table says it ships (`protocol`, `ncp`) and nothing is written
to its port. Each probe is bounded (the Z-Wave one at 2.9 s) and closes the
port before it returns.

Coordinators on the LAN (SLZB-06, UZG-01) come from mDNS: PulzrOS saves
`ubus call umdns browse` to `/var/run/pulzr-probe/mdns.json`, and a service type
in the table's `lan` list becomes a radio with `bus: lan`.

## Build and test

```sh
cargo test --workspace            # unit tests, protocol round trips, in-process Modbus TCP server
UPDATE_SCHEMA=1 cargo test -p pulzr-edge --test schema   # regenerate schema/register-map.schema.json
cargo build --release
```

Release builds are size optimised (`opt-level = "s"`, LTO, stripped, abort on panic).

### Targets

Gateways run musl, and the release binaries are static. A tag `v<version>`
(the workspace version) runs `.github/workflows/release.yml`, which runs the
whole test suite for each target under qemu-user, builds, and publishes the
binaries with a `SHA256SUMS`:

| Asset | Target | PulzarOS arch |
|---|---|---|
| `pulzr-edge-aarch64-linux-musl` | `aarch64-unknown-linux-musl` | `aarch64_cortex-a53`, `-a72`, `-a76` (Pi 3, 4, 5) |
| `pulzr-edge-mipsel-linux-musl` | `mipsel-unknown-linux-musl` | `mipsel_24kc` (RAK7268, RAK7289, GL-SFT1200) |
| `pulzr-edge-mips-linux-musl` | `mips-unknown-linux-musl` | `mips_24kc` (Dragino LPS8N) |

`pulzr-edge-sim-*` and `pulzr-probe-*` are published the same way.

By hand, with [zig](https://ziglang.org) and
[cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild):

```sh
rustup target add aarch64-unknown-linux-musl
cargo zigbuild --release -p pulzr-edge --target aarch64-unknown-linux-musl

# mipsel is tier 3: std is built on nightly, and zig supplies musl's C runtime
rustup toolchain install nightly --profile minimal -c rust-src
RUSTFLAGS="-C target-feature=+crt-static -C link-self-contained=no" \
  cargo +nightly zigbuild --release -p pulzr-edge \
  --target mipsel-unknown-linux-musl -Zbuild-std=std,panic_abort
```

`ring` (under `rustls`) has no MIPS assembly and compiles its portable C there:
slower handshakes, same results. The release workflow pins the nightly it was
tested with.

## Before it can ship as a PulzarOS package

- In an image: PulzarOS has the package (`feed/pulzr-edge`, this repository's
  release binaries by checksum, started only once `/etc/pulzr-edge/config.toml`
  exists, kept across upgrades), but no board includes it yet. That waits for
  the console side below.
- ~~The broker certificate flow~~ Done: the daemon publishes with the gateway's
  existing broker login (`username` + `password_file`), so no certificate of its
  own is issued.
- Console side: add a bus to a gateway, scan unit ids, register maps arriving as
  device packages (this daemon reads a file; the package format and delivery are
  not wired).
- Check against a real broker and real RS-485 hardware: the MQTT session
  (reconnect, PubAck replay) and the serial path are not covered by the automated
  tests, which use an in-process Modbus TCP server and the on-disk queue directly.
- Native RS-485 ports and direction control (`TIOCSRS485`) for boards that have
  one; today RTU relies on a USB adapter that handles direction itself.
- Publishing the `settings` snapshot at start, a Modbus unit scan, and
  function 15/16 batching of adjacent writes are not done.

## License

Apache-2.0.
