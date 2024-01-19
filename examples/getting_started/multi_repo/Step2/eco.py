from typing import Optional
from datasurface.md.Governance import Ecosystem, GitRepository, GovernanceZone, GovernanceZoneDeclaration, TeamDeclaration


def createEcosystem() -> Ecosystem:
    ecosys : Ecosystem = Ecosystem(
        "Test Ecosystem", 
        GitRepository("https://github.com/billynewport/eco.git", "main"),
        GovernanceZoneDeclaration("USA", GitRepository("https://github.com/billynewport/gzUSA.git", "main"))
    )

    gz : Optional[GovernanceZone] = ecosys.getZone("USA")
    if(gz == None):
        raise Exception("USA governance zone not found")
    
    gz.add(
        TeamDeclaration("Customer", GitRepository("https://github.com/billynewport/teamUSA_Customer.git", "main"))
        )
    
    return ecosys

