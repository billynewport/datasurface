from datasurface.md.Governance import Ecosystem, GitRepository, GovernanceZoneDeclaration

def createEcosystem() -> Ecosystem:
    ecosys : Ecosystem = Ecosystem(
        "Test Ecosystem", 
        GitRepository("https://github.com/billynewport/eco.git", "main"),
        GovernanceZoneDeclaration("USA", GitRepository("https://github.com/billynewport/gzUSA.git", "main"))
    )
    return ecosys

