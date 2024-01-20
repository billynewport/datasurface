from typing import Optional
import unittest

from datasurface.md import InfrastructureVendor, InfraLocation, TeamDeclaration, GitHubRepository, Ecosystem
from datasurface.md import GovernanceZone, GovernanceZoneDeclaration

class TestZones(unittest.TestCase):

    def checkChildLocation(self, parent : InfraLocation, childName : str, vendor : InfrastructureVendor):
        child : Optional[InfraLocation] = parent.locations.get(childName)
        if(child is None):
            raise Exception("Child location {} not found in parent {}".format(childName, parent.name))
        self.assertIsNotNone(child)
        self.assertEqual(child.name, childName)
        self.assertIsNotNone(child)

    def test_CreateUSAEco(self):
        eco = Ecosystem("BigCorp", GitHubRepository("a", "b"))
        self.assertEqual(eco.name, "BigCorp")
        self.assertEqual(eco.zones.getNumObjects(), 0)
        usZoneName : str = "US"
        eco.add(
            GovernanceZoneDeclaration(usZoneName, GitHubRepository("aa", "bb")),
        )
        gzUSA : GovernanceZone = eco.getZoneOrThrow(usZoneName)
        gzUSA.add(
                InfrastructureVendor("AWS",
                    InfraLocation("USA",
                        InfraLocation("us-east-1"),
                        InfraLocation("us-east-2"),
                        InfraLocation("us-west-1"),
                        InfraLocation("us-west-2")
                        ),
                    InfraLocation("Europe",
                        InfraLocation("eu-west-1"),
                        InfraLocation("eu-west-2")
                        ),
                ))
        
        gzUSA.add(
            InfrastructureVendor("AZURE",
                InfraLocation("USA",
                    InfraLocation("Central US"),
                    InfraLocation("North Central US"),
                    InfraLocation("South Central US"),
                    InfraLocation("West Central US"),
                    InfraLocation("West US"),
                    InfraLocation("West US 2"),
                    InfraLocation("West US 3"),
                    InfraLocation("East US"),
                    InfraLocation("East US 2"),
                    InfraLocation("East US 3")
                    ),
                ))
        gzUSA.add(
            TeamDeclaration("Billys team",
                GitHubRepository("https://github.com/data-fracture/ecomgr.git", "module")
                ),
            TeamDeclaration("Jacks team", 
                GitHubRepository("git repo 2", "module2"))
            )
        self.assertEqual(eco.zones.getNumObjects(), 1)
        self.assertEqual(eco.getZone("US"), gzUSA)

        self.assertEqual(gzUSA.name, usZoneName)
        self.assertEqual(len(gzUSA.vendors), 2)

        aws : Optional[InfrastructureVendor] = gzUSA.vendors.get("AWS")
        if(aws is None):
            raise Exception("Vendor AWS not found")
        self.assertIsNotNone(aws)
        self.assertEqual(len(aws.locations), 2)
        awsUSA : Optional[InfraLocation] = aws.locations.get("USA")
        if(awsUSA is None):
            raise Exception("Location USA not found")
        self.assertIsNotNone(awsUSA)
        self.assertEqual(awsUSA.name, "USA")
         
        for locName in ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']:
            self.checkChildLocation(awsUSA, locName, aws)
        awsEurope : Optional[InfraLocation] = aws.locations.get("Europe")
        if(awsEurope is None):
            self.assertIsNotNone(awsEurope)
            raise Exception("Location Europe not found")
        
        self.assertEqual(awsEurope.name, "Europe")
        for locName in ['eu-west-1', 'eu-west-2']:
            self.checkChildLocation(awsEurope, locName, aws)
                         
        azure : Optional[InfrastructureVendor] = gzUSA.vendors.get("AZURE")
        if(azure is None):
            raise Exception("Vendor AZURE not found") 
        self.assertIsNotNone(azure)
        self.assertEqual(len(azure.locations), 1)
        azureUSA : Optional[InfraLocation] = azure.locations.get("USA")
        if(azureUSA is None):
            raise Exception("Location USA not found")
        self.assertIsNotNone(azureUSA)
        self.assertEqual(azureUSA.name, "USA")
        self.assertEqual(len(azureUSA.locations), 10)
        for locName in ['Central US', 'North Central US', 'South Central US', 'West Central US', 'West US', 'West US 2', 'West US 3', 'East US', 'East US 2', 'East US 3']:
            self.checkChildLocation(azureUSA, locName, azure)



if __name__ == '__main__':
    unittest.main()

