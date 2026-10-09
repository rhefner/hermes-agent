from unittest.mock import Mock, patch
import pytest
from hermes_cli import kanban_db_dispatch as dispatch
from agent.maintenance_inference import MaintenanceIsolationError

def test_core_spawn_not_treated_as_plugin_under_backend_isolation():
    core = Mock(return_value=123)
    with patch.object(dispatch, '_default_spawn', core), patch('agent.maintenance_inference.legacy_state', return_value=None), patch('agent.maintenance_inference.refuse_unverified_extension', side_effect=MaintenanceIsolationError('blocked')) as guard:
        assert dispatch._call_spawn_fn(core, object(), '/fixture', None) == 123
        guard.assert_not_called()

def test_custom_spawn_still_refused():
    with patch('agent.maintenance_inference.refuse_unverified_extension', side_effect=MaintenanceIsolationError('blocked')):
        with pytest.raises(MaintenanceIsolationError):
            dispatch._call_spawn_fn(lambda *a: 123, object(), '/fixture', None)

def test_legacy_core_spawn_preserves_blanket_gate():
    core = Mock(return_value=123)
    with patch.object(dispatch, '_default_spawn', core), patch('agent.maintenance_inference.legacy_state', return_value={'version':1}), patch('agent.maintenance_inference.refuse_unverified_extension', side_effect=MaintenanceIsolationError('blocked')):
        with pytest.raises(MaintenanceIsolationError): dispatch._call_spawn_fn(core, object(), '/fixture', None)
        core.assert_not_called()
