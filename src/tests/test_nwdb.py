from datasurface.md.Governance import GitRepository, Repository
from datasurface.md.Lint import ValidationTree
import tests.nwdb.eco
from datasurface.md import Ecosystem

def test_validate_nwdb():
    e : Ecosystem = tests.nwdb.eco.createEcosystem()

    rc : ValidationTree = e.lintAndHydrateCaches()
    print(rc)
    assert rc.hasIssues() == False

def test_eq_ecosystem():
    e : Ecosystem = tests.nwdb.eco.createEcosystem()
    e2 : Ecosystem = tests.nwdb.eco.createEcosystem()

    diffR : Repository = GitRepository("ssh://u@local:/v1/source/eco", "main_other")

    assert e == e2

    # No changes
    problems : ValidationTree = ValidationTree(e)
    e.checkIfChangesAreAuthorized(e2, e.owningRepo, problems)
    assert problems.hasIssues() == False

    e2.name = "Test2"
    # Test name cannot be changed from another repo
    # Verify they are not equal, the name was changed
    assert e != e2

    # Verify that the change is not authorized
    problems = ValidationTree(e)
    e.checkIfChangesAreAuthorized(e2, diffR, problems)
    assert problems.hasIssues()

    e2 : Ecosystem = tests.nwdb.eco.createEcosystem()

    assert e == e2
    e2.zones.removeDefinition("USA")
    assert e != e2

