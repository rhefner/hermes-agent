"""Unclosed responses must stay owned until transport shutdown, even after GC."""
import asyncio
import fcntl
import gc
import os

import httpx
import pytest

from agent import backend_transport as bt


@pytest.mark.parametrize('asynchronous', [False, True])
def test_dropped_response_shutdown_releases_lock(tmp_path, monkeypatch, asynchronous):
    path = tmp_path / 'backend.lock'
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(fd, fcntl.LOCK_SH)
    monkeypatch.setattr(bt, 'begin_request', lambda request: [fd])
    closed = []

    class Body(httpx.SyncByteStream):
        def __iter__(self):
            yield b'body'
        def close(self):
            closed.append(True)

    class AsyncBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'body'
        async def aclose(self):
            closed.append(True)

    class Inner(httpx.BaseTransport):
        def handle_request(self, request):
            return httpx.Response(200, stream=Body())

    class AsyncInner(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            return httpx.Response(200, stream=AsyncBody())

    async def exercise_async():
        transport = bt.AsyncBackendTransport(AsyncInner())
        response = await transport.handle_async_request(httpx.Request('GET', 'http://fixture/'))
        del response
        gc.collect()
        await transport.aclose()
        assert not transport._outstanding

    try:
        if asynchronous:
            asyncio.run(exercise_async())
        else:
            transport = bt.BackendTransport(Inner())
            response = transport.handle_request(httpx.Request('GET', 'http://fixture/'))
            del response
            gc.collect()
            transport.close()
            assert not transport._outstanding
        assert closed == [True]
        probe = os.open(path, os.O_RDWR)
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(probe)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
