"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest
from datasurface.md import PlainTextDocumentation

from datasurface.md import Ecosystem, GovernanceZone, InfraStructureLocationPolicy, InfraStructureVendorPolicy, \
    InfrastructureVendor
from datasurface.md import ValidationTree, LocationKey
from datasurface.md import DataClassificationPolicy, SimpleDC, SimpleDCTypes
from tests.nwdb.eco import createEcosystem


class TestPolicy(unittest.TestCase):
    def test_InfrastructureVendorPolicy(self):
        eco: Ecosystem = createEcosystem()

        myCorp: InfrastructureVendor = eco.getVendorOrThrow("MyCorp")
        outsource: InfrastructureVendor = eco.getVendorOrThrow("Outsource")

        p: InfraStructureVendorPolicy = InfraStructureVendorPolicy("Only MyCorp", PlainTextDocumentation("Test"), {myCorp})

        self.assertEqual(p.name, "Only MyCorp")

        self.assertTrue(p.isCompatible(myCorp))
        self.assertFalse(p.isCompatible(outsource))

    def test_InfrastructureLocationPolicy(self):
        eco: Ecosystem = createEcosystem()

        myCorpNY1Location: LocationKey = LocationKey("MyCorp:USA/NY_1")
        self.assertIsNotNone(myCorpNY1Location.getAsInfraLocation(eco))
        outsourceNJ1Location: LocationKey = LocationKey("Outsource:USA/NJ_1")
        self.assertIsNotNone(outsourceNJ1Location.getAsInfraLocation(eco))

        p: InfraStructureLocationPolicy = InfraStructureLocationPolicy("Outsource USA Only", PlainTextDocumentation("Test"), {myCorpNY1Location})
        self.assertEqual(p.name, "Outsource USA Only")

        self.assertTrue(p.isCompatible(myCorpNY1Location))
        self.assertFalse(p.isCompatible(outsourceNJ1Location))

        s: set[InfraStructureLocationPolicy] = set()
        s.add(p)
        self.assertEqual(s, s)

    def test_DataClassicationPolicy(self):
        p: DataClassificationPolicy = DataClassificationPolicy("IP Only", PlainTextDocumentation("Test"), {SimpleDC(SimpleDCTypes.IP)})
        self.assertEqual(p.name, "IP Only")
        self.assertTrue(p.isCompatible(SimpleDC(SimpleDCTypes.IP)))
        self.assertFalse(p.isCompatible(SimpleDC(SimpleDCTypes.PUB)))


class TestPlatformPolicy(unittest.TestCase):

    def test_InfraVendorPolicy(self):
        aws: InfrastructureVendor = InfrastructureVendor("AWS")
        azure: InfrastructureVendor = InfrastructureVendor("Azure")

        p: InfraStructureVendorPolicy = InfraStructureVendorPolicy("Only AWS", PlainTextDocumentation("Test"), {aws})

        self.assertTrue(p.isCompatible(aws))
        self.assertFalse(p.isCompatible(azure))

    def test_InfraLocationPolicy(self):
        eco: Ecosystem = createEcosystem()

        ny1Location: LocationKey = LocationKey("MyCorp:USA/NY_1")
        self.assertIsNotNone(ny1Location.getAsInfraLocation(eco))
        nj1Location: LocationKey = LocationKey("MyCorp:USA/NJ_1")
        self.assertIsNotNone(nj1Location.getAsInfraLocation(eco))

        p: InfraStructureLocationPolicy = InfraStructureLocationPolicy("MyCorp USA NY_1 Only", PlainTextDocumentation("Test"), {ny1Location})

        self.assertTrue(p.isCompatible(ny1Location))
        self.assertFalse(p.isCompatible(nj1Location))

        s: set[InfraStructureLocationPolicy] = set()
        s.add(p)
        self.assertEqual(s, s)

    def test_VendorRestriction(self):
        # No policies allow all
        eco: Ecosystem = createEcosystem()
        tree: ValidationTree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Add Outsource to GZ USA as ONLY allowed Vendor
        gzUSA: GovernanceZone = eco.getZoneOrThrow("USA")
        p: InfraStructureVendorPolicy = InfraStructureVendorPolicy("Only Outsource", PlainTextDocumentation("Test"), {eco.getVendorOrThrow("Outsource")})
        gzUSA.add(p)

        # The NW Store uses MyCorp, this should fail lint
        tree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.hasErrors())

        # Now reset and allow MyCorp, passes
        eco = createEcosystem()
        p = InfraStructureVendorPolicy("Only MyCorp", PlainTextDocumentation("Test"), {eco.getVendorOrThrow("MyCorp")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Now, disallow Outsource, it only uses MyCorp so should be fine
        eco = createEcosystem()
        p = InfraStructureVendorPolicy("Outsource Not Allowed", PlainTextDocumentation("Test"), None, {eco.getVendorOrThrow("Outsource")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Now, disallow Azure, it only uses Azure so should fail
        eco = createEcosystem()
        p = InfraStructureVendorPolicy("MyCorp not allowed", PlainTextDocumentation("Test"), None, {eco.getVendorOrThrow("MyCorp")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.hasErrors())
