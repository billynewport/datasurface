from typing import List, Sequence
from datasurface.md.Governance import GitRepository, Repository
import tests.nwdb.eco
from datasurface.md import Ecosystem, ValidationProblem

def test_validate_nwdb():
    e : Ecosystem = tests.nwdb.eco.createEcosystem()

    rc : Sequence[ValidationProblem] = e.validateAndHydrateCaches()
    print(rc)
    assert len(rc) == 0

def test_eq_ecosystem():
    e : Ecosystem = tests.nwdb.eco.createEcosystem()
    e2 : Ecosystem = tests.nwdb.eco.createEcosystem()

    diffR : Repository = GitRepository("ssh://u@local:/v1/source/eco", "main_other")

    assert e == e2

    # No changes
    problems : List[ValidationProblem] = e.checkIfChangesAreAuthorized(e2, e.owningRepo)
    assert len(problems) == 0

    e2.name = "Test2"
    assert e != e2
    problems : List[ValidationProblem] = e.checkIfChangesAreAuthorized(e2, diffR)
    assert len(problems) != 0

    e2 : Ecosystem = tests.nwdb.eco.createEcosystem()

    assert e == e2
    e2.governanceZones.pop("USA")
    assert e != e2

