from datasurface.md.Governance import Ecosystem, GitHubRepository, GovernanceZoneDeclaration

def createEcosystem() -> Ecosystem:
    ecosys : Ecosystem = Ecosystem(
        "Test Ecosystem", 
        GitHubRepository("https://github.com/billynewport/eco.git", "main"),
        GovernanceZoneDeclaration("USA", GitHubRepository("https://github.com/billynewport/gzUSA.git", "main"))
    )
    return ecosys

