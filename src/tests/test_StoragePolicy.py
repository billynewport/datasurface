

import unittest
from datasurface.md.Documentation import PlainTextDocumentation

from datasurface.md.Governance import Ecosystem, GovernanceZone, InfraStructureLocationPolicy, InfraStructureVendorPolicy, InfrastructureLocation, \
    InfrastructureVendor
from datasurface.md.Lint import ValidationTree
from datasurface.md.Policy import DataClassificationPolicy, SimpleDC, SimpleDCTypes
from tests.nwdb.eco import createEcosystem


class TestPolicy(unittest.TestCase):
    def test_InfrastructureVendorPolicy(self):
        eco: Ecosystem = createEcosystem()

        aws: InfrastructureVendor = eco.getVendorOrThrow("AWS")
        azure: InfrastructureVendor = eco.getVendorOrThrow("Azure")

        p: InfraStructureVendorPolicy = InfraStructureVendorPolicy("Only AWS", PlainTextDocumentation("Test"), {aws})

        self.assertEqual(p.name, "Only AWS")

        self.assertTrue(p.isCompatible(aws))
        self.assertFalse(p.isCompatible(azure))

    def test_InfrastructureLocationPolicy(self):
        eco: Ecosystem = createEcosystem()

        awsLocation: InfrastructureLocation = eco.getLocationOrThrow("AWS", ["USA", "us-east-1"])
        azureLocation: InfrastructureLocation = eco.getLocationOrThrow("Azure", ["USA", "Central US"])

        p: InfraStructureLocationPolicy = InfraStructureLocationPolicy("Azure USA Only", PlainTextDocumentation("Test"), {awsLocation})
        self.assertEqual(p.name, "Azure USA Only")

        self.assertTrue(p.isCompatible(awsLocation))
        self.assertFalse(p.isCompatible(azureLocation))

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

        eco: Ecosystem = createEcosystem()

        aws: InfrastructureVendor = eco.getVendorOrThrow("AWS")
        azure: InfrastructureVendor = eco.getVendorOrThrow("Azure")

        p: InfraStructureVendorPolicy = InfraStructureVendorPolicy("Only AWS", PlainTextDocumentation("Test"), {aws})

        self.assertTrue(p.isCompatible(aws))
        self.assertFalse(p.isCompatible(azure))

    def test_InfraLocationPolicy(self):
        eco: Ecosystem = createEcosystem()

        awsLocation: InfrastructureLocation = eco.getLocationOrThrow("AWS", ["USA", "us-east-1"])
        azureLocation: InfrastructureLocation = eco.getLocationOrThrow("Azure", ["USA", "Central US"])

        p: InfraStructureLocationPolicy = InfraStructureLocationPolicy("Azure USA Only", PlainTextDocumentation("Test"), {awsLocation})

        self.assertTrue(p.isCompatible(awsLocation))
        self.assertFalse(p.isCompatible(azureLocation))

        s: set[InfraStructureLocationPolicy] = set()
        s.add(p)
        self.assertEqual(s, s)

    def test_VendorRestriction(self):
        # No policies allow all
        eco: Ecosystem = createEcosystem()
        tree: ValidationTree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Add AWS to GZ USA as ONLY allowed Vendor
        gzUSA: GovernanceZone = eco.getZoneOrThrow("USA")
        p: InfraStructureVendorPolicy = InfraStructureVendorPolicy("Only AWS", PlainTextDocumentation("Test"), {eco.getVendorOrThrow("AWS")})
        gzUSA.add(p)

        # The NW Store uses Azure, this should fail lint
        tree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.hasErrors())

        # Now reset and allow Azure, passes
        eco = createEcosystem()
        p = InfraStructureVendorPolicy("Only Azure", PlainTextDocumentation("Test"), {eco.getVendorOrThrow("Azure")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Now, disallow AWS, it only uses Azure so should be fine
        eco = createEcosystem()
        p = InfraStructureVendorPolicy("AWS Not Allowed", PlainTextDocumentation("Test"), None, {eco.getVendorOrThrow("AWS")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Now, disallow Azure, it only uses Azure so should fail
        eco = createEcosystem()
        p = InfraStructureVendorPolicy("Azure not allowed", PlainTextDocumentation("Test"), None, {eco.getVendorOrThrow("Azure")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.hasErrors())
