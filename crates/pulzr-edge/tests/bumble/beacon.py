"""A scripted Bluetooth beacon for pulzr-edge's `ble` tests.

Google's Bumble runs two virtual controllers on one simulated radio link:
one is reached by the module under test over H4 on TCP, the other is this
beacon: a KKM S7S parking sensor's advertising (Eddystone service data,
KSensor frame) that is also connectable, with a Battery Level of 87 %.

    python beacon.py PORT

Events go to stdout as JSON lines. A line on stdin ends it.
"""

import asyncio
import json
import sys

from bumble import gatt
from bumble.controller import Controller
from bumble.device import Device
from bumble.hci import Address
from bumble.host import Host
from bumble.link import LocalLink
from bumble.transport import open_transport
from bumble.transport.common import AsyncPipeSink

KSENSOR = bytes.fromhex("020106" "1116aafe21001b0bb81980ffff0002000301")


def emit(event, **kw):
    print(json.dumps({"event": event, **kw}), flush=True)


async def main():
    port = int(sys.argv[1])
    link = LocalLink()
    transport = await open_transport(f"tcp-server:127.0.0.1:{port}")
    Controller("DUT", host_source=transport.source, host_sink=transport.sink, link=link)
    controller = Controller("BEACON", link=link)
    device = Device(address=Address("F1:22:33:44:55:66"), host=Host(controller, AsyncPipeSink(controller)))
    device.add_service(
        gatt.Service(
            gatt.GATT_BATTERY_SERVICE,
            [
                gatt.Characteristic(
                    gatt.GATT_BATTERY_LEVEL_CHARACTERISTIC,
                    gatt.Characteristic.Properties.READ,
                    gatt.Characteristic.READABLE,
                    bytes([87]),
                )
            ],
        )
    )
    await device.power_on()
    device.on("connection", lambda c: emit("connected"))
    emit("ready")
    await device.start_advertising(
        advertising_data=KSENSOR,
        advertising_interval_min=20,
        advertising_interval_max=20,
        auto_restart=True,
    )
    emit("advertising", address=str(device.random_address))
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    await reader.readline()
    await transport.close()


if __name__ == "__main__":
    asyncio.run(main())
