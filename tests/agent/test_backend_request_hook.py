"""The standalone request hook must preserve model identity on shared facades."""
import asyncio
import httpx
import pytest
from agent import maintenance_inference as guard


def test_shared_facade_hook_keeps_unaffected_model_available(monkeypatch):
    state = {'version': 2, 'phase': 'active',
             'backends': {'spark-pair': ['http://facade:8002'], 'spark-3': ['http://facade:8002']},
             'facade_models': {'primary': 'spark-pair', 'secondary': 'spark-3'},
             'affected': ['spark-pair']}
    monkeypatch.setattr(guard, 'state', lambda **kwargs: state)
    for name in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'):
        monkeypatch.delenv(name, raising=False)
    allowed = httpx.Request('POST', 'http://facade:8002/v1/chat/completions', json={'model': 'secondary'})
    blocked = httpx.Request('POST', 'http://facade:8002/v1/chat/completions', json={'model': 'primary'})
    guard.check_http_request(allowed)
    asyncio.run(guard.check_async_http_request(allowed))
    with pytest.raises(guard.MaintenanceIsolationError):
        guard.check_http_request(blocked)
    with pytest.raises(guard.MaintenanceIsolationError):
        asyncio.run(guard.check_async_http_request(blocked))
