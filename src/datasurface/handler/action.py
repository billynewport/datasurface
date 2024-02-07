
import importlib
import os
import sys
from typing import Optional

from datasurface.md.Governance import Ecosystem
from datasurface.md.GitOps import GitHubRepository, Repository
from datasurface.md.Lint import ValidationTree


def verifyPullRequest():
    mainFolder : str = sys.argv[1]
    prFolder : str = sys.argv[2]

    head_Repository : Optional[str] = os.environ.get('HEAD_REPOSITORY')
    head_Branch : Optional[str] = os.environ.get('HEAD_BRANCH')
    if(head_Repository == None or head_Branch == None):
        raise Exception("Git arguments not found")

    prRepo : Repository = GitHubRepository(head_Repository, head_Branch)

    origSystemPath : list[str] = sys.path
    # Add mainFolder to system path and then load the eco.py which has a main and returns an Ecosystem
    sys.path.append(mainFolder)

    module = importlib.import_module("eco")
    function = getattr(module, "createEcosystem")

    ecoMain : Ecosystem = function()

    # Reset system path
    sys.path = origSystemPath
    # Add prFolder to system path and then load the eco.py which has a main and returns an Ecosystem
    sys.path.append(prFolder)

    module = importlib.import_module("eco")
    function = getattr(module, "createEcosystem")

    ecoPR : Ecosystem = function()

    sys.path = origSystemPath
    tree : ValidationTree = ecoMain.checkIfChangesCanBeMerged(ecoPR, prRepo)

    if(tree.hasErrors()):
        raise Exception(f"Pull request not allowed: {tree}")


    