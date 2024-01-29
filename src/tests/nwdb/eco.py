from datasurface.md import Team, GovernanceZoneDeclaration, GitHubRepository, GovernanceZone, InfrastructureVendor, InfrastructureLocation, TeamDeclaration, DataPlatform
from datasurface.md import Ecosystem
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.Lint import ValidationTree
from tests.nwdb.nwdb import defineTables as defineNWTeamTables
from tests.nwdb.nwdb import defineWorkspaces as defineNWTeamWorkspaces

def createEcosystem() -> Ecosystem:
    ecosys : Ecosystem = Ecosystem(
        "Test", 
        GitHubRepository("https://github.com/billynewport/eco.git", "main"),
        GovernanceZoneDeclaration("USA", GitHubRepository("https://github.com/billynewport/gzUSA.git", "main")),
        GovernanceZoneDeclaration("EU", GitHubRepository("https://github.com/billynewport/gzEU.git", "main")),
        GovernanceZoneDeclaration("UK", GitHubRepository("https://github.com/billynewport/gzUK.git", "main"))
    )

    gzUSA : GovernanceZone = ecosys.getZoneOrThrow("USA")

    gzUSA.add(InfrastructureVendor("AWS",
                PlainTextDocumentation("Amazon AWS"),
                InfrastructureLocation("us-east-1"), # Virginia
                InfrastructureLocation("us-west-1")), # California
            InfrastructureVendor("MyCorp",
                PlainTextDocumentation("Private USA company data centers"),
                InfrastructureLocation("NJ_1"),
                InfrastructureLocation("NY_1")),
            
            InfrastructureVendor("Azure",
                PlainTextDocumentation("Microsoft Azure"),
                InfrastructureLocation("USA",
                    InfrastructureLocation("Central US"), # Iowa
                    InfrastructureLocation("East US"), # Virginia
                    InfrastructureLocation("East US 2"), # Virginia
    #                InfraLocation("East US 3"), # Georgia
    #                InfraLocation("North Central US"), # Illinois
                    InfrastructureLocation("South Central US"), # Texas
    #                InfraLocation("West Central US"), # Wyoming
    #                InfraLocation("West US"), # California
                    InfrastructureLocation("West US 2"), # Washington
                    InfrastructureLocation("West US 3")), # Arizona
            ),

            TeamDeclaration("FrontOffice", GitHubRepository("https://github.com/billynewport/fo.git", "main")),
            TeamDeclaration("MiddleOffice", GitHubRepository("https://github.com/billynewport/mo.git", "main")),
            TeamDeclaration("NorthWindTeam", GitHubRepository("https://github.com/billynewport/nwTeam.git", "main")),
            TeamDeclaration("BackOffice", GitHubRepository("https://github.com/billynewport/bo.git", "main")),

            DataPlatform("DataGlide@1.0")
        )

    gzEU : GovernanceZone = ecosys.getZoneOrThrow("EU")
    gzEU.add(InfrastructureVendor("AWS",
                PlainTextDocumentation("Amazon AWS"),
                InfrastructureLocation("eu-central-1"), # Frankfurt
                InfrastructureLocation("eu-west-3")), # Paris

            TeamDeclaration("FrontOffice", GitHubRepository("https://github.com/billynewport/fo.git", "main")),
            TeamDeclaration("MiddleOffice", GitHubRepository("https://github.com/billynewport/mo.git", "main")),
            TeamDeclaration("BackOffice", GitHubRepository("https://github.com/billynewport/bo.git", "main")),

            DataPlatform("DataGlide@1.0")
            )

    gzUK : GovernanceZone = ecosys.getZoneOrThrow("UK")
    gzUK.add(
        InfrastructureVendor("AWS",
            PlainTextDocumentation("Amazon AWS UK"),
            InfrastructureLocation("eu-west-1"), # Ireland
            InfrastructureLocation("eu-west-2")), # London
        InfrastructureVendor("MyCorp",
            PlainTextDocumentation("Private UK Data centers"),
            InfrastructureLocation("London"),
            InfrastructureLocation("Cambridge")),

        TeamDeclaration("FrontOffice", GitHubRepository("https://github.com/billynewport/fo.git", "main")),
        TeamDeclaration("MiddleOffice", GitHubRepository("https://github.com/billynewport/mo.git", "main")),
        TeamDeclaration("BackOffice", GitHubRepository("https://github.com/billynewport/bo.git", "main")),

        DataPlatform("DataGlide@1.0")
    )


    # Fill out the NorthWindTeam managed by the USA governance zone
    nw_team : Team = ecosys.getTeamOrThrow("USA", "NorthWindTeam")
    defineNWTeamTables(ecosys, gzUSA, nw_team)
    defineNWTeamWorkspaces(nw_team)
    
    tree : ValidationTree = ecosys.lintAndHydrateCaches()
    if(tree.hasErrors()):
        tree.printTree()
        raise Exception("Ecosystem validation failed")
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