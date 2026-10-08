"""Pure inventory preparation. No filesystem, migration or implicit live state."""
import copy
from agent.backend_isolation import validate, origin, backends_at


def add_verified_alias(value, *, owner, canonical, alias):
    """Prepare an additive alias update after operator verifies endpoint ownership.

    Application requires a separately drained runtime and compare-and-swap check;
    this pure function intentionally cannot enroll, migrate or write live state.
    """
    updated = copy.deepcopy(value)
    if updated.get('version') != 2 or updated.get('phase') != 'normal':
        raise ValueError('Only an existing v2-normal inventory can be updated')
    validate(updated)
    if backends_at(updated, canonical) != [owner]:
        raise ValueError('Canonical endpoint must have one matching owner')
    owners = backends_at(updated, alias)
    if owners and owners != [owner]:
        raise ValueError('Alias already belongs to another or shared backend')
    origin(alias)
    if not owners:
        updated['backends'][owner].append(alias)
    validate(updated)
    return updated
