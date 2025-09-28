# app/iot/service.py
import asyncio
import random
import string
from typing import Protocol

from .devices import HueLightDevice, SmartSpeakerDevice, SmartToiletDevice
from .message import Message, MessageType


def generate_id(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_uppercase, k=length))


# Helpers de orquestração
async def run_sequence(*coros) -> None:
    for coro in coros:
        await coro


async def run_parallel(*coros) -> None:
    await asyncio.gather(*coros)


class Device(Protocol):
    async def connect(self) -> None:
        ...

    async def disconnect(self) -> None:
        ...

    async def send_message(
        self,
        message_type: MessageType,
        data: str = "",
    ) -> None:
        ...


class IOTService:
    def __init__(self) -> None:
        self.devices: dict[str, Device] = {}

    async def register_device(self, device: Device) -> str:
        await device.connect()
        device_id = generate_id()
        self.devices[device_id] = device
        return device_id

    async def unregister_device(self, device_id: str) -> None:
        device = self.devices.pop(device_id, None)
        if device is not None:
            await device.disconnect()

    def get_device(self, device_id: str) -> Device:
        return self.devices[device_id]

    async def send_msg(self, msg: Message) -> None:
        await self.devices[msg.device_id].send_message(msg.msg_type, msg.data)


# Orquestração end-to-end
async def orchestrate() -> None:
    service = IOTService()

    hue = HueLightDevice()
    spk = SmartSpeakerDevice()
    toi = SmartToiletDevice()

    # registrar devices em paralelo
    hue_id, spk_id, toi_id = await asyncio.gather(
        service.register_device(hue),
        service.register_device(spk),
        service.register_device(toi),
    )

    # wake-up
    print("=====RUNNING PROGRAM======")
    await run_parallel(
        service.send_msg(Message(hue_id, MessageType.SWITCH_ON)),
        run_sequence(
            service.send_msg(Message(spk_id, MessageType.SWITCH_ON)),
            service.send_msg(
                Message(
                    spk_id,
                    MessageType.PLAY_SONG,
                    "Rick Astley - Never Gonna Give You Up",
                )
            ),
        ),
    )
    print("=====END OF PROGRAM======")

    # sleep
    print("=====RUNNING PROGRAM======")
    await run_parallel(
        service.send_msg(Message(hue_id, MessageType.SWITCH_OFF)),
        run_sequence(
            service.send_msg(Message(spk_id, MessageType.SWITCH_OFF)),
        ),
        run_sequence(
            service.send_msg(Message(toi_id, MessageType.FLUSH)),
            service.send_msg(Message(toi_id, MessageType.CLEAN)),
        ),
    )
    print("=====END OF PROGRAM======")

    # desconectar em paralelo
    await asyncio.gather(
        service.unregister_device(hue_id),
        service.unregister_device(spk_id),
        service.unregister_device(toi_id),
    )
