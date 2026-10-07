"""Hold backend reader locks until HTTP bodies/streams actually close.

The transport tracks every response body it handed out and releases the whole
set on shutdown: closing the transport closes the physical response, not just
the descriptor, so an outstanding stream can never keep pinning its backend
lock after the client is gone. Stream close is idempotent.
"""
import os
import weakref

import httpx

from agent.backend_isolation import begin_request


def _lock_handles(begin):
    # The unit seam may hand back a bare descriptor; the real path returns the
    # acquired descriptor list.
    if begin is None:
        return []
    if isinstance(begin, int):
        return [begin]
    return list(begin)


class ResponseStream(httpx.SyncByteStream):
    def __init__(self, stream, fds):
        self.stream, self.fds = stream, list(fds)

    def __iter__(self):
        yield from self.stream

    def close(self):
        try:
            self.stream.close()
        finally:
            fds, self.fds = self.fds, []
            for fd in fds:
                os.close(fd)


class AsyncResponseStream(httpx.AsyncByteStream):
    def __init__(self, stream, fds):
        self.stream, self.fds = stream, list(fds)

    async def __aiter__(self):
        async for chunk in self.stream:
            yield chunk

    async def aclose(self):
        try:
            await self.stream.aclose()
        finally:
            fds, self.fds = self.fds, []
            for fd in fds:
                os.close(fd)


class BackendTransport(httpx.BaseTransport):
    def __init__(self, inner):
        self.inner = inner
        # Weak: a closed-and-dropped response leaves nothing to sweep.
        self._outstanding = weakref.WeakSet()

    @property
    def _pool(self):
        # Existing cancellation closes only this client's in-flight sockets via
        # the shared transport's pool; wrapping must not hide that ownership seam.
        return self.inner._pool

    def handle_request(self, request):
        fds = _lock_handles(begin_request(request))
        try:
            response = self.inner.handle_request(request)
        except BaseException:
            for fd in fds:
                os.close(fd)
            raise
        stream = ResponseStream(response.stream, fds)
        self._outstanding.add(stream)
        response.stream = stream
        return response

    def close(self):
        for stream in list(self._outstanding):
            stream.close()
        self.inner.close()


class AsyncBackendTransport(httpx.AsyncBaseTransport):
    def __init__(self, inner):
        self.inner = inner
        self._outstanding = weakref.WeakSet()

    @property
    def _pool(self):
        return self.inner._pool

    async def handle_async_request(self, request):
        fds = _lock_handles(begin_request(request))
        try:
            response = await self.inner.handle_async_request(request)
        except BaseException:
            for fd in fds:
                os.close(fd)
            raise
        stream = AsyncResponseStream(response.stream, fds)
        self._outstanding.add(stream)
        response.stream = stream
        return response

    async def aclose(self):
        for stream in list(self._outstanding):
            await stream.aclose()
        await self.inner.aclose()
