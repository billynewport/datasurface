

from typing import Sequence
import unittest

from datasurface.md.Governance import CDCCaptureIngestion, DataContainer, Dataset, Datastore, Ecosystem, GovernanceZone, GovernanceZoneDeclaration, InfraLocation, InfrastructureVendor, LocalGovernanceManagedOnly, PolicyMandatedRule, StoragePolicy, StoragePolicyAllowAnyContainer, Team, TeamDeclaration
from datasurface.md.Lint import ValidationTree
from datasurface.md.Schema import DDLColumn, DDLTable, NullableStatus, PrimaryKeyStatus, String
from tests.nwdb.eco import createEcosystem


class Test_StoragePolicies(unittest.TestCase):
    def test_AllowAnyAndGzRestricted(self):

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
                
    def test_MandatoryPolicies(self):
        # Here we add a manadory in zone policy and then check its present
        # on a Dataset defined within that zone
        eco : Ecosystem = createEcosystem()

        eco.add(GovernanceZoneDeclaration("RestrictedGZ", eco.owningRepo))

        gzRestricted : GovernanceZone = eco.getZoneOrThrow("RestrictedGZ")
        gzRestricted.add(
            TeamDeclaration("RTeam", eco.owningRepo),
            LocalGovernanceManagedOnly("Same zone only", PolicyMandatedRule.MANDATED_WITHIN_ZONE),
            InfrastructureVendor("AWS-CN",
                InfraLocation("Beijing"),
                InfraLocation("Ningxia"))
            )
        
        t : Team = gzRestricted.getTeamOrThrow("RTeam")
        t.add(
            Datastore("rStore1",
                CDCCaptureIngestion(),
                Dataset("RDataset1",
                    DDLTable(
                        DDLColumn("key", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                        DDLColumn("firstName", String(20))
                    )))
            )
        eTree : ValidationTree = eco.lintAndHydrateCaches()
        self.assertFalse(eTree.hasErrors())
        
        rDataset1 : Dataset = t.dataStores["rStore1"].datasets["RDataset1"]
        datasetPolicies : Sequence[StoragePolicy] = gzRestricted.getDatasetStoragePolicies(rDataset1)
        self.assertEqual(1, len(datasetPolicies))
        self.assertTrue(isinstance(datasetPolicies[0], LocalGovernanceManagedOnly))
        
        

        
