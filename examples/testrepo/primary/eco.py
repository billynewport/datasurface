from datasurface.md import Ecosystem
from datasurface.md.Governance import GitRepository

def ecoFactory() -> Ecosystem:
    e : Ecosystem = Ecosystem("Test", GitRepository("a", "b"))
    return e
