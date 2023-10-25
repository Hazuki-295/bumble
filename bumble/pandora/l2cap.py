# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import abc
import asyncio
import dataclasses
import logging
import grpc
import struct

from bumble import device
from bumble import l2cap
from bumble.utils import EventWatcher
from bumble.pandora import config
from bumble.pandora import utils
from google.protobuf import any_pb2  # pytype: disable=pyi-error
from google.protobuf import empty_pb2  # pytype: disable=pyi-error
from pandora import l2cap_pb2
from pandora import l2cap_grpc_aio
from typing import Any, AsyncGenerator, AsyncIterator, Dict, List, Union


class ChannelProxy(abc.ABC):
    rx: asyncio.Queue[bytes] = asyncio.Queue()

    async def receive(self) -> bytes:
        return await self.rx.get()

    def send(self, data: bytes) -> None:
        ...

    async def disconnect(self) -> None:
        ...

    async def wait_disconnect(self) -> None:
        ...


@dataclasses.dataclass
class DynamicChannelProxy(ChannelProxy):
    channel: Union[l2cap.ClassicChannel, l2cap.LeCreditBasedChannel, None]

    def __post_init__(self) -> None:
        assert self.channel
        self.disconnection_result = asyncio.get_event_loop().create_future()
        self.channel.sink = self.rx.put_nowait

        def on_close() -> None:
            assert not self.disconnection_result.done()
            self.channel = None
            self.disconnection_result.set_result(None)

        self.channel.on('close', on_close)

    def send(self, data: bytes) -> None:
        assert self.channel
        if isinstance(self.channel, l2cap.ClassicChannel):
            self.channel.send_pdu(data)
        else:
            self.channel.write(data)

    async def disconnect(self) -> None:
        assert self.channel
        await self.channel.disconnect()

    async def wait_disconnect(self) -> None:
        return await self.disconnection_result


@dataclasses.dataclass
class FixedChannelProxy(ChannelProxy):
    connection: device.Connection
    cid: int

    def send(self, data: bytes) -> None:
        self.connection.device.send_l2cap_pdu(self.connection.handle, self.cid, data)

    async def disconnect(self) -> None:
        raise RuntimeError('Fixed channel cannot be disconnected')

    async def wait_disconnect(self) -> None:
        raise RuntimeError('Fixed channel cannot be disconnected')


@dataclasses.dataclass
class ChannelIndex:
    connection_handle: int
    cid: int

    @classmethod
    def from_token(cls, token: l2cap_pb2.Channel) -> 'ChannelIndex':
        connection_handle, cid = struct.unpack('>HH', token.cookie.value)
        return cls(connection_handle, cid)

    def into_token(self) -> l2cap_pb2.Channel:
        return l2cap_pb2.Channel(
            cookie=any_pb2.Any(
                value=struct.pack('>HH', self.connection_handle, self.cid)
            )
        )

    def __hash__(self):
        return hash(self.connection_handle | (self.cid << 12))


class L2CAPService(l2cap_grpc_aio.L2CAPServicer):
    channels: Dict[ChannelIndex, ChannelProxy] = {}
    pending: List[l2cap.IncomingConnection.Any] = []
    accepts: List[asyncio.Queue[l2cap.IncomingConnection.Any]] = []

    def __init__(self, dev: device.Device, config: config.Config) -> None:
        self.device = dev
        self.config = config

        @self.device.l2cap_channel_manager.listen
        def _(incoming: l2cap.IncomingConnection.Any) -> None:
            self.pending.append(incoming)
            for acceptor in self.accepts:
                acceptor.put_nowait(incoming)

    def register(self, index: ChannelIndex, proxy: ChannelProxy) -> None:
        self.channels[index] = proxy

        def on_close(*_: Any) -> None:
            # TODO: Fix Bumble L2CAP emit `close` twice.
            if index in self.channels:
                del self.channels[index]

        # Listen for disconnection.
        if isinstance(proxy, FixedChannelProxy):
            proxy.connection.on('disconnection', on_close)
        elif isinstance(proxy, DynamicChannelProxy):
            assert proxy.channel
            proxy.channel.on('close', on_close)

    async def listen(self) -> AsyncIterator[l2cap.IncomingConnection.Any]:
        for incoming in self.pending:
            if incoming.expired():
                self.pending.remove(incoming)
                continue
            yield incoming
        queue = asyncio.Queue()
        self.accepts.append(queue)
        try:
            while incoming := await queue.get():
                yield incoming
        finally:
            self.accepts.remove(queue)

    @utils.rpc
    async def Connect(
        self, request: l2cap_pb2.ConnectRequest, context: grpc.ServicerContext
    ) -> l2cap_pb2.ConnectResponse:
        # Retrieve Bumble `Connection` from request.
        connection_handle = int.from_bytes(request.connection.cookie.value, 'big')
        connection = self.device.lookup_connection(connection_handle)
        if connection is None:
            raise RuntimeError(f'{connection_handle}: not connection for handle')

        if request.type_variant() == 'basic':
            assert request.basic
            channel = await connection.create_l2cap_channel(
                spec=l2cap.ClassicChannelSpec(
                    psm=request.basic.psm, mtu=request.basic.mtu
                )
            )
        elif request.type_variant() == 'le_credit_based':
            assert request.le_credit_based
            channel = await connection.create_l2cap_channel(
                spec=l2cap.LeCreditBasedChannelSpec(
                    psm=request.le_credit_based.spsm,
                    max_credits=request.le_credit_based.initial_credit,
                    mtu=request.le_credit_based.mtu,
                    mps=request.le_credit_based.mps,
                )
            )
        else:
            raise NotImplementedError(f"{request.type_variant()}: unsupported type")

        index = ChannelIndex(channel.connection.handle, channel.source_cid)
        self.register(index, DynamicChannelProxy(channel))
        return l2cap_pb2.ConnectResponse(channel=index.into_token())

    @utils.rpc
    async def WaitConnection(
        self, request: l2cap_pb2.WaitConnectionRequest, context: grpc.ServicerContext
    ) -> l2cap_pb2.WaitConnectionResponse:
        iter = self.listen()
        fut: asyncio.Future[
            Union[l2cap.ClassicChannel, l2cap.LeCreditBasedChannel]
        ] = asyncio.Future()

        # Filter by connection.
        if request.connection:
            handle = int.from_bytes(request.connection.cookie.value, 'big')
            iter = (it async for it in iter if it.connection.handle == handle)

        if request.type_variant() == 'basic':
            assert request.basic
            async for it in (
                it
                async for it in iter
                if isinstance(it, l2cap.IncomingConnection.Basic)
                and it.psm == request.basic.psm
            ):
                pend = l2cap.PendingConnection.Basic(
                    fut.set_result,
                    request.basic.mtu or l2cap.L2CAP_MIN_BR_EDR_MTU,
                )
                if it.accept(pend):
                    break
        elif request.type_variant() == 'le_credit_based':
            assert request.le_credit_based
            async for it in (
                it
                async for it in iter
                if isinstance(it, l2cap.IncomingConnection.LeCreditBased)
                and it.psm == request.le_credit_based.spsm
            ):
                pend = l2cap.PendingConnection.LeCreditBased(
                    fut.set_result,
                    request.le_credit_based.mtu
                    or l2cap.L2CAP_LE_CREDIT_BASED_CONNECTION_DEFAULT_MTU,
                    request.le_credit_based.mps
                    or l2cap.L2CAP_LE_CREDIT_BASED_CONNECTION_DEFAULT_MPS,
                    request.le_credit_based.initial_credit
                    or l2cap.L2CAP_LE_CREDIT_BASED_CONNECTION_DEFAULT_INITIAL_CREDITS,
                )
                if it.accept(pend):
                    break
        else:
            raise NotImplementedError(f"{request.type_variant()}: unsupported type")

        channel = await fut
        index = ChannelIndex(channel.connection.handle, channel.source_cid)
        self.register(index, DynamicChannelProxy(channel))
        return l2cap_pb2.WaitConnectionResponse(channel=index.into_token())

    @utils.rpc
    async def Disconnect(
        self, request: l2cap_pb2.DisconnectRequest, context: grpc.ServicerContext
    ) -> l2cap_pb2.DisconnectResponse:
        channel = self.channels[ChannelIndex.from_token(request.channel)]
        await channel.disconnect()
        return l2cap_pb2.DisconnectResponse(success=empty_pb2.Empty())

    @utils.rpc
    async def WaitDisconnection(
        self, request: l2cap_pb2.WaitDisconnectionRequest, context: grpc.ServicerContext
    ) -> l2cap_pb2.WaitDisconnectionResponse:
        channel = self.channels[ChannelIndex.from_token(request.channel)]
        await channel.wait_disconnect()
        return l2cap_pb2.WaitDisconnectionResponse(success=empty_pb2.Empty())

    @utils.rpc
    async def Receive(
        self, request: l2cap_pb2.ReceiveRequest, context: grpc.ServicerContext
    ) -> AsyncGenerator[l2cap_pb2.ReceiveResponse, None]:
        # TODO: fixed channel `Receive`
        channel = self.channels[ChannelIndex.from_token(request.channel)]
        while packet := await channel.receive():
            yield l2cap_pb2.ReceiveResponse(data=packet)

    @utils.rpc
    async def Send(
        self, request: l2cap_pb2.SendRequest, context: grpc.ServicerContext
    ) -> l2cap_pb2.SendResponse:
        # TODO: fixed channel `Send`
        channel = self.channels[ChannelIndex.from_token(request.channel)]
        channel.send(request.data)
        return l2cap_pb2.SendResponse(success=empty_pb2.Empty())
