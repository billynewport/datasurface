

from typing import Sequence
import unittest
from datasurface.md.Documentation import PlainTextDocumentation

from datasurface.md.Governance import CDCCaptureIngestion, DataContainer, Dataset, Datastore, Ecosystem, GovernanceZone, GovernanceZoneDeclaration, InfraStructureLocationPolicy, InfraStructureVendorPolicy, InfrastructureLocation, InfrastructureVendor, LocalGovernanceManagedOnly, PolicyMandatedRule, StoragePolicy, StoragePolicyAllowAnyContainer, Team, TeamDeclaration
from datasurface.md.Lint import ValidationTree
from datasurface.md.Schema import DDLColumn, DDLTable, NullableStatus, PrimaryKeyStatus, String
from tests.nwdb.eco import createEcosystem


class Test_StoragePolicies(unittest.TestCase):
    def test_AllowAnyAndGzRestricted(self):

        eco : Ecosystem = createEcosystem()

        gzUSA : GovernanceZone = eco.getZoneOrThrow("USA")

        awsVendor : InfrastructureVendor = gzUSA.getVendorOrThrow("AWS")
        usEast1 : InfrastructureLocation = awsVendor.getLocationOrThrow("us-east-1")

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
                    PlainTextDocumentation("Amazon AWS China"),
                    InfrastructureLocation("Beijing"),
                    InfrastructureLocation("Ningxia"))
                )
            awsChina : InfrastructureVendor = gzRestricted.getVendorOrThrow("AWS-CN")
            beijing : InfrastructureLocation = awsChina.getLocationOrThrow("Beijing")
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
                PlainTextDocumentation("Amazon AWS China"),
                InfrastructureLocation("Beijing"),
                InfrastructureLocation("Ningxia"))
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
        
class TestPlatformPolicy(unittest.TestCase):

    def test_InfraVendorPolicy(self):

        eco : Ecosystem = createEcosystem()

        gzUSA : GovernanceZone = eco.getZoneOrThrow("USA")
        aws : InfrastructureVendor = gzUSA.getVendorOrThrow("AWS")
        azure : InfrastructureVendor = gzUSA.getVendorOrThrow("Azure")

        p : InfraStructureVendorPolicy = InfraStructureVendorPolicy({aws})

        self.assertTrue(p.isCompatible(aws))
        self.assertFalse(p.isCompatible(azure))

    def test_InfraLocationPolicy(self):
        eco : Ecosystem = createEcosystem()

        gzUSA : GovernanceZone = eco.getZoneOrThrow("USA")
        aws : InfrastructureVendor = gzUSA.getVendorOrThrow("AWS")
        awsLocation : InfrastructureLocation = aws.getLocationOrThrow("us-east-1")
        azure : InfrastructureVendor = gzUSA.getVendorOrThrow("Azure")
        azureLocation : InfrastructureLocation = azure.getLocationOrThrow("USA").getLocationOrThrow("Central US")

        p : InfraStructureLocationPolicy = InfraStructureLocationPolicy({awsLocation})

        self.assertTrue(p.isCompatible(awsLocation))
        self.assertFalse(p.isCompatible(azureLocation))

    def test_VendorRestriction(self):
        eco : Ecosystem = createEcosystem()

        # Add AWS as ONLY allowed Vendor
        gzUSA : GovernanceZone = eco.getZoneOrThrow("USA")
        v : InfrastructureVendor = gzUSA.getVendorOrThrow("AWS")
        p : InfraStructureVendorPolicy = InfraStructureVendorPolicy({v})
        gzUSA.add(p)

        # The NW Store uses Azure, this should fail lint
        tree : ValidationTree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.hasErrors())

        # Now, allow Azure, passes
        eco = createEcosystem()
        p = InfraStructureVendorPolicy({gzUSA.getVendorOrThrow("Azure")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Now, disallow AWS, it only uses Azure so should be fine
        eco = createEcosystem()
        p = InfraStructureVendorPolicy(None, {gzUSA.getVendorOrThrow("AWS")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        # Now, disallow Azure, it only uses Azure so should fail
        eco = createEcosystem()
        p = InfraStructureVendorPolicy(None, {gzUSA.getVendorOrThrow("Azure")})
        gzUSA = eco.getZoneOrThrow("USA")
        gzUSA.add(p)
        tree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.hasErrors())





        

        
