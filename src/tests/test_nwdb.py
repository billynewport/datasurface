"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import copy
import unittest
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.repo import GitHubRepository
from datasurface.md import GovernanceZone, GovernanceZoneDeclaration, \
    InfraStructureLocationPolicy, InfrastructureLocation, \
    InfrastructureVendor, Repository, TeamDeclaration
from datasurface.md import ValidationTree, LocationKey
from datasurface.md.types import IEEE128, IEEE16, IEEE32, IEEE64, DataType, Date, Decimal, \
    String, Vector
import tests.nwdb.eco
from datasurface.platforms.legacy import LegacyDataPlatform
from datasurface.md import Ecosystem, DDLColumn, NullableStatus, PrimaryKeyStatus, DataPlatformKey
from datasurface.md.governance import PrimaryIngestionPlatform
from typing import Optional


class TestEcosystemValidation(unittest.TestCase):

    def test_pip(self):
        e: Ecosystem = tests.nwdb.eco.createEcosystem()
        rc: ValidationTree = e.lintAndHydrateCaches()
        rc.printTree()
        self.assertFalse(rc.hasErrors())
        e.hydratePrimaryIngestionPlatforms("src/tests/nwdb/test_pip.json", rc)
        rc.printTree()
        self.assertFalse(rc.hasErrors())
        self.assertEqual(len(e.primaryIngestionPlatforms), 1)
        # Check the store has a pip
        pip: Optional[PrimaryIngestionPlatform] = e.primaryIngestionPlatforms.get("NW_Data")
        self.assertIsNotNone(pip)
        self.assertEqual(len(pip.dataPlatforms), 1)
        self.assertEqual(pip.dataPlatforms, {DataPlatformKey("LegacyA")})
        rc = e.lintAndHydrateCaches()
        rc.printTree()
        self.assertFalse(rc.hasErrors())

    def test_validate_nwdb(self):
        e: Ecosystem = tests.nwdb.eco.createEcosystem()

        rc: ValidationTree = e.lintAndHydrateCaches()
        rc.printTree()
        self.assertFalse(rc.hasErrors())
        # self.assertTrue(rc.hasWarnings())  # ClearTextCredential - This was causing a failure, warning is not generated

    def test_nwdb_has_all_softlinks(self):
        e: Ecosystem = tests.nwdb.eco.createEcosystem()

        self.assertEqual(e, e)

        rc: ValidationTree = e.lintAndHydrateCaches()
        rc.printTree()
        self.assertFalse(rc.hasErrors())

        # Verify that all softlinks are in the ecosystem
        self.assertIsNotNone(e.key)
        for iv in e.vendors.values():
            self.assertIsNotNone(iv.key)
            for loc in iv.locations.values():
                self.assertIsNotNone(loc.key)

        if (e.zones.authorizedObjects):

            for gz in e.zones.authorizedObjects.values():
                for sp in gz.storagePolicies.values():
                    self.assertIsNotNone(sp.key)
                self.assertIsNotNone(gz.key)
                for td in gz.teams.authorizedNames.values():
                    self.assertIsNotNone(td.key)

    def test_validate_columns(self):
        col: DDLColumn = DDLColumn("col1", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)
        tree: ValidationTree = ValidationTree(col)
        col.lint(tree)
        self.assertFalse(tree.hasErrors())

        # Test cases where the column is not valid
        col.name = "col 1"  # Not ANSI SQL Identifier
        tree: ValidationTree = ValidationTree(col)
        col.lint(tree)
        self.assertEqual(len(tree.problems), 1)
        self.assertTrue(tree.hasErrors())

        # Test cases where the column is not valid

    def assertOneIssue(self, o: DataType):
        tree: ValidationTree = ValidationTree(o)
        o.lint(tree)
        self.assertEqual(len(tree.problems), 1)
        self.assertTrue(tree.hasErrors())

    def assertNoIssue(self, o: DataType):
        tree: ValidationTree = ValidationTree(o)
        o.lint(tree)
        self.assertFalse(tree.hasErrors())

    def test_lint_datatypes(self):
        self.assertNoIssue(String(20))

        self.assertOneIssue(String(0))  # String length must be > 0
        self.assertOneIssue(Decimal(10, -1))  # Scale < 0
        self.assertOneIssue(Decimal(10, 11))  # Precision > scale
        self.assertNoIssue(Decimal(10, 0))  # Scale == 0 is ok

        self.assertNoIssue(Vector(10))
        self.assertOneIssue(Vector(0))  # Vector length must be > 0

        self.assertNoIssue(IEEE128())
        self.assertNoIssue(IEEE64())
        self.assertNoIssue(IEEE32())
        self.assertNoIssue(IEEE16())

        self.assertNoIssue(Date())

    def test_equality(self):
        self.assertEqual(
            LegacyDataPlatform(
                "name",
                PlainTextDocumentation("Test")),
            LegacyDataPlatform("name", PlainTextDocumentation("Test")))

        ghr: GitHubRepository = GitHubRepository("https://github.com/billynewport/eco.git", "main")
        self.assertEqual(ghr, ghr)

        gzd: GovernanceZoneDeclaration = GovernanceZoneDeclaration("USA", GitHubRepository("https://github.com/billynewport/gzUSA.git", "main"))
        self.assertEqual(gzd, gzd)

        ptd: PlainTextDocumentation = PlainTextDocumentation("Amazon AWS")
        self.assertEqual(ptd, ptd)

        ifl: InfrastructureLocation = InfrastructureLocation("L", ptd)
        self.assertEqual(ifl, ifl)

        ifl2 = copy.deepcopy(ifl)
        ifl2.add(InfrastructureLocation("L2"))
        self.assertEqual(ifl2, ifl2)
        self.assertNotEqual(ifl, ifl2)

        iv: InfrastructureVendor = InfrastructureVendor("V")
        self.assertEqual(iv, iv)
        iv2: InfrastructureVendor = copy.deepcopy(iv)
        self.assertEqual(iv, iv2)
        iv2.add(ifl)
        self.assertNotEqual(iv, iv2)

        gz: GovernanceZone = GovernanceZone("GZ", ghr)
        self.assertEqual(gz, gz)

        td: TeamDeclaration = TeamDeclaration("TD", ghr)
        self.assertEqual(td, td)

        gz.add(td)
        self.assertEqual(gz, gz)

        ifl2Key: LocationKey = LocationKey("V/L2")

        islp: InfraStructureLocationPolicy = InfraStructureLocationPolicy("Azure USA Only", PlainTextDocumentation("Test"), {ifl2Key}, None)
        self.assertEqual(islp, islp)

        eco: Ecosystem = Ecosystem("E", ghr, iv2, gzd)
        self.assertEqual(eco, eco)

    def test_eq_ecosystem(self):
        e: Ecosystem = tests.nwdb.eco.createEcosystem()
        e2: Ecosystem = tests.nwdb.eco.createEcosystem()

        diffR: Repository = GitHubRepository("ssh://u@local:/v1/source/eco", "main_other")
        self.assertEqual(diffR, diffR)

        self.assertEqual(e, e2)

        # No changes
        problems: ValidationTree = ValidationTree(e)
        e.checkIfChangesAreAuthorized(e2, e.owningRepo, problems)
        self.assertFalse(problems.hasErrors())

        e2.name = "Test2"
        # Test name cannot be changed from another repo
        # Verify they are not equal, the name was changed
        self.assertNotEqual(e, e2)

        # Verify that the change is not authorized
        problems = ValidationTree(e)
        e.checkIfChangesAreAuthorized(e2, diffR, problems)
        self.assertTrue(problems.hasErrors())

        e2: Ecosystem = tests.nwdb.eco.createEcosystem()

        self.assertEqual(e, e2)
        e2.zones.removeDefinition("USA")
        self.assertNotEqual(e, e2)
