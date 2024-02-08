from datasurface.md import Ecosystem
from datasurface.md.GitOps import GitHubRepository

def ecoFactory() -> Ecosystem:
    e : Ecosystem = Ecosystem("Test", GitHubRepository("a", "b"))
    return e
