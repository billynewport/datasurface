from typing import Optional
from datasurface.md import Team, GovernanceZoneDeclaration, GitRepository, GovernanceZone, InfrastructureVendor, InfraLocation, TeamDeclaration, DataPlatform
from datasurface.md import Ecosystem
from datasurface.md.Lint import ValidationTree
from tests.nwdb.nwdb import defineTables as defineNWTeamTables
from tests.nwdb.nwdb import defineWorkspaces as defineNWTeamWorkspaces

def createEcosystem() -> Ecosystem:
    ecosys : Ecosystem = Ecosystem(
        "Test", 
        GitRepository("ssh://u@local:/v1/source/eco", "main"),
        GovernanceZoneDeclaration("USA", GitRepository("ssh://u@local:/v1/source/gz_usa", "main")),
        GovernanceZoneDeclaration("EU", GitRepository("ssh://u@local:/v1/source/gz_eu", "main")),
        GovernanceZoneDeclaration("UK", GitRepository("ssh://u@local:/v1/source/gz_uk", "main"))
    )

    gzUSA : Optional[GovernanceZone] = ecosys.getZone("USA")

    if(gzUSA):
        gzUSA.add(InfrastructureVendor("AWS",
                    InfraLocation("us-east-1"), # Virginia
                    InfraLocation("us-west-1")), # California
                InfrastructureVendor("MyCorp",
                    InfraLocation("NJ_1"),
                    InfraLocation("NY_1")),
                
                InfrastructureVendor("Azure",
                    InfraLocation("USA",
                        InfraLocation("Central US"), # Iowa
                        InfraLocation("East US"), # Virginia
                        InfraLocation("East US 2"), # Virginia
        #                InfraLocation("East US 3"), # Georgia
        #                InfraLocation("North Central US"), # Illinois
                        InfraLocation("South Central US"), # Texas
        #                InfraLocation("West Central US"), # Wyoming
        #                InfraLocation("West US"), # California
                        InfraLocation("West US 2"), # Washington
                        InfraLocation("West US 3")), # Arizona
                ),

                TeamDeclaration("FrontOffice", GitRepository("ssh://u@local:/v1/source/fo", "main")),
                TeamDeclaration("MiddleOffice", GitRepository("ssh://u@local:/v1/source/mo", "main")),
                TeamDeclaration("NorthWindTeam", GitRepository("ssh://u@local:/v1/source/nwteam", "main")),
                TeamDeclaration("BackOffice", GitRepository("ssh://u@local:/v1/source/bo", "main")),

                DataPlatform("DataGlide@1.0")
            )
    else:
        raise Exception("USA governance zone not found")

    gzEU : Optional[GovernanceZone] = ecosys.getZone("EU")
    if(gzEU):
        gzEU.add(InfrastructureVendor("AWS",
                    InfraLocation("eu-central-1"), # Frankfurt
                    InfraLocation("eu-west-3")), # Paris

                TeamDeclaration("FrontOffice", GitRepository("ssh://u@local:/v1/source/fo", "main")),
                TeamDeclaration("MiddleOffice", GitRepository("ssh://u@local:/v1/source/mo", "main")),
                TeamDeclaration("BackOffice", GitRepository("ssh://u@local:/v1/source/bo", "main")),

                DataPlatform("DataGlide@1.0")
                )
    else:
        raise Exception("EU governance zone not found")

    gzUK : Optional[GovernanceZone] = ecosys.getZone("UK")
    if(gzUK):
        gzUK.add(
            InfrastructureVendor("AWS",
                InfraLocation("eu-west-1"), # Ireland
                InfraLocation("eu-west-2")), # London
            InfrastructureVendor("MyCorp",
                InfraLocation("London"),
                InfraLocation("Cambridge")),

            TeamDeclaration("FrontOffice", GitRepository("ssh://u@local:/v1/source/fo", "main")),
            TeamDeclaration("MiddleOffice", GitRepository("ssh://u@local:/v1/source/mo", "main")),
            TeamDeclaration("BackOffice", GitRepository("ssh://u@local:/v1/source/bo", "main")),

            DataPlatform("DataGlide@1.0")
        )
    else:
        raise Exception("UK governance zone not found")


    # Fill out the NorthWindTeam managed by the USA governance zone
    nw_team : Optional[Team] = ecosys.getTeam("USA", "NorthWindTeam")
    if(nw_team):
        defineNWTeamTables(nw_team)
        defineNWTeamWorkspaces(nw_team)
    
    return ecosys

def test_Validate():
    ecosys : Ecosystem = createEcosystem()
    vTree : ValidationTree = ecosys.lintAndHydrateCaches()
    if(vTree.hasErrors()):
        print(vTree)
        raise Exception("Ecosystem validation failed")
    else:
        print("Ecosystem validated OK")

if __name__ == "__main__":
    test_Validate()