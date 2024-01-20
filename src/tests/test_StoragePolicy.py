

import unittest

from datasurface.md.Governance import DataContainer, Ecosystem, GovernanceZone, GovernanceZoneDeclaration, InfraLocation, InfrastructureVendor, LocalGovernanceManagedOnly, PolicyMandatedRule, StoragePolicy, StoragePolicyAllowAnyContainer
from tests.nwdb.eco import createEcosystem


class Test_StoragePolicies(unittest.TestCase):
    def test_AllowAny(self):

        eco : Ecosystem = createEcosystem()

        gzUSA : GovernanceZone = eco.getZoneOrThrow("USA")

        awsVendor : InfrastructureVendor = gzUSA.getVendorOrThrow("AWS")
        usEast1 : InfraLocation = awsVendor.getLocationOrThrow("us-east-1")

        if(usEast1.key):
            allowAnyP : StoragePolicy = StoragePolicyAllowAnyContainer("Allow all", PolicyMandatedRule.INDIVIDUALLY_MANDATED)
            gzUSA.add(allowAnyP)
            self.assertFalse(eco.lintAndHydrateCaches().hasErrors())

            usContainer : DataContainer = DataContainer(usEast1.key)
            self.assertTrue(allowAnyP.isCompatible(usContainer))

            eco.add(GovernanceZoneDeclaration("RestrictedGZ", eco.owningRepo))

            gzRestricted : GovernanceZone = eco.getZoneOrThrow("RestrictedGZ")
            sameZoneOnlyP : StoragePolicy = LocalGovernanceManagedOnly("Same zone only", PolicyMandatedRule.INDIVIDUALLY_MANDATED)
            gzRestricted.add(
                sameZoneOnlyP,
                InfrastructureVendor("AWS-CN",
                    InfraLocation("Beijing"),
                    InfraLocation("Ningxia"))
                )
            awsChina : InfrastructureVendor = gzRestricted.getVendorOrThrow("AWS-CN")
            beijing : InfraLocation = awsChina.getLocationOrThrow("Beijing")
            if(beijing.key):
                beijingContainer : DataContainer = DataContainer(beijing.key)

                self.assertFalse(eco.lintAndHydrateCaches().hasErrors())

                # US container not allowed in restricted zone
                self.assertFalse(sameZoneOnlyP.isCompatible(usContainer))
                # Local container is allowed
                self.assertTrue(sameZoneOnlyP.isCompatible(beijingContainer))
            else:
                self.fail("Beijing location has no key")
                


        
