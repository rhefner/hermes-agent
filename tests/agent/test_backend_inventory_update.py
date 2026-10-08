import copy
import pytest
from agent.backend_inventory_update import add_verified_alias
from agent import backend_isolation as b
from agent import maintenance_inference as guard

@pytest.fixture
def state(monkeypatch):
    # Pure values only. Tripwire prevents accidental live configuration reads.
    monkeypatch.setattr(b, 'configuration', lambda: pytest.fail('live configuration read'))
    return {'version': 2, 'phase': 'normal', 'affected': [],
            'backends': {'spark-pair': ['http://spark-1.hef.run:8000', 'http://llm-lab.hef.run:8002'],
                         'spark-3': ['http://spark-3:8000', 'http://llm-lab.hef.run:8002']},
            'facade_models': {'primary': 'spark-pair', 'secondary': 'spark-3'}}

def prepare(state, **kw):
    return add_verified_alias(state, owner='spark-pair', canonical='http://spark-1.hef.run:8000',
                              alias=kw.get('alias', 'http://spark-1:8000'))

def test_alias_update_is_additive_and_idempotent(state):
    before = copy.deepcopy(state)
    assert b.backend(state, 'http://spark-1:8000/v1', 'glm-5.3-flash') is None
    updated = prepare(state)
    assert state == before
    assert updated['backends']['spark-pair'] == before['backends']['spark-pair'] + ['http://spark-1:8000']
    assert updated['facade_models'] == before['facade_models']
    assert prepare(updated) == updated
    for url in ('http://spark-1:8000/v1', 'http://spark-1.hef.run:8000/v1'):
        assert b.backend(updated, url, 'glm-5.3-flash') == 'spark-pair'

def test_active_route_excludes_alias_not_secondary(state, monkeypatch):
    updated = prepare(state)
    updated.update(phase='active', affected=['spark-pair'], provider='openai-codex',
                   model='independent', base_url=guard.CLOUD_BASE)
    for key in ('HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy'):
        monkeypatch.delenv(key, raising=False)
    b.validate(updated)
    with pytest.raises(guard.MaintenanceIsolationError, match='excludes'):
        b.require_route(updated, None, 'glm-5.3-flash', 'http://spark-1:8000/v1', None, resolved=True)
    b.require_route(updated, None, 'secondary', 'http://llm-lab.hef.run:8002/v1', None, resolved=True)

@pytest.mark.parametrize('phase', ['active','isolating','interrupted'])
def test_reject_non_normal(state, phase):
    state['phase'] = phase
    with pytest.raises(ValueError): prepare(state)

def test_reject_wrong_owner_and_shared_alias(state):
    for alias in ('http://spark-3:8000', 'http://llm-lab.hef.run:8002'):
        with pytest.raises(ValueError): prepare(state, alias=alias)

def test_no_migration(state):
    state['version'] = 1
    with pytest.raises(ValueError): prepare(state)
