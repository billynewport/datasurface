from datasurface.md import Ecosystem
from datasurface.md.Governance import GitHubRepository

def ecoFactory() -> Ecosystem:
    e : Ecosystem = Ecosystem("Test", GitHubRepository("a", "b"))
    return e
