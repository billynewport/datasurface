from typing import Optional
from datasurface.md.Governance import Ecosystem, GitHubRepository, GovernanceZone, GovernanceZoneDeclaration, TeamDeclaration


def createEcosystem() -> Ecosystem:
    ecosys : Ecosystem = Ecosystem(
        "Test Ecosystem", 
        GitHubRepository("https://github.com/billynewport/eco.git", "main"),
        GovernanceZoneDeclaration("USA", GitHubRepository("https://github.com/billynewport/gzUSA.git", "main"))
    )

    gz : Optional[GovernanceZone] = ecosys.getZone("USA")
    if(gz == None):
        raise Exception("USA governance zone not found")
    
    gz.add(
        TeamDeclaration("Customer", GitHubRepository("https://github.com/billynewport/teamUSA_Customer.git", "main"))
        )
    
    return ecosys

