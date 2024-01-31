

import unittest

from datasurface.md.Governance import Ecosystem, GovernanceZone, InfraStructureLocationPolicy, InfraStructureVendorPolicy, InfrastructureLocation, InfrastructureVendor
from datasurface.md.Lint import ValidationTree
from tests.nwdb.eco import createEcosystem


class TestPlatformPolicy(unittest.TestCase):

    def test_InfraVendorPolicy(self):

        eco : Ecosystem = createEcosystem()

        aws : InfrastructureVendor = eco.getVendorOrThrow("AWS")
        azure : InfrastructureVendor = eco.getVendorOrThrow("Azure")

        p : InfraStructureVendorPolicy = InfraStructureVendorPolicy("Only AWS", {aws})

        self.assertTrue(p.isCompatible(aws))
        self.assertFalse(p.isCompatible(azure))

    def test_InfraLocationPolicy(self):
        eco : Ecosystem = createEcosystem()

        awsLocation : InfrastructureLocation = eco.getLocationOrThrow("AWS", ["USA", "us-east-1"])
        azureLocation : InfrastructureLocation = eco.getLocationOrThrow("Azure", ["USA", "Central US"])

        p : InfraStructureLocationPolicy = InfraStructureLocationPolicy("Azure USA Only", {awsLocation})

        self.assertTrue(p.isCompatible(awsLocation))
        self.assertFalse(p.isCompatible(azureLocation))

        s : set[InfraStructureLocationPolicy] = set()
        s.add(p)
        self.assertEqual(s, s)


    def test_VendorRestriction(self):
        # No policies allow all
        eco : Ecosystem = createEcosystem()
        tree : ValidationTree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Add AWS to GZ USA as ONLY allowed Vendor
        gzUSA : GovernanceZone = eco.getZoneOrThrow("USA")
        p : InfraStructureVendorPolicy = InfraStructureVendorPolicy("Only AWS", {eco.getVendorOrThrow("AWS")})
        gzUSA.add(p)

        # The NW Store uses Azure, this should fail lint
        tree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.hasErrors())

        # Now reset and allow Azure, passes
        eco = createEcosystem()
        p = InfraStructureVendorPolicy("Only Azure", {eco.getVendorOrThrow("Azure")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Now, disallow AWS, it only uses Azure so should be fine
        eco = createEcosystem()
        p = InfraStructureVendorPolicy("AWS Not Allowed", None, {eco.getVendorOrThrow("AWS")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Now, disallow Azure, it only uses Azure so should fail
        eco = createEcosystem()
        p = InfraStructureVendorPolicy("Azure not allowed", None, {eco.getVendorOrThrow("Azure")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.hasErrors())





        

        
