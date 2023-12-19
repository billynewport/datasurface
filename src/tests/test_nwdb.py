from typing import Sequence
import tests.nwdb.eco
from datasurface.md import Ecosystem, ValidationProblem

def test_validate_nwdb():
    e : Ecosystem = tests.nwdb.eco.createEcosystem()

    rc : Sequence[ValidationProblem] = e.validateAndHydrateCaches()
    print(rc)
    assert len(rc) == 0


