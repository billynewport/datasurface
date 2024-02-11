
import copy
import importlib
import os
import sys
from types import ModuleType
from typing import Optional

#import github

from datasurface.md.Governance import Ecosystem
from datasurface.md.GitOps import GitHubRepository, Repository
from datasurface.md.Lint import ValidationTree


def getEcosystem(path : str) -> Ecosystem:
    """This tries to bootstrap an Ecosystem from the file eco.py on a specific path"""
    origSystemPath : list[str] = copy.deepcopy(sys.path)
    try:
        sys.path.append(path)

        # Remove the module from sys.modules to force a reload
        if 'eco' in sys.modules:
            del sys.modules['eco']

        module : ModuleType = importlib.import_module("eco")
        function = getattr(module, "createEcosystem")
        sys.path = origSystemPath

        # This should now point to createEcosystem() -> Ecosystem in the eco.py file on the path specified
        eco : Ecosystem = function() 
        # Flesh out
        eco.lintAndHydrateCaches()

        return eco
    finally:
        sys.path = origSystemPath

def verifyPullRequest() -> ValidationTree:
    """This is the actual github action handler"""
    origSystemPath : list[str] = sys.path
    mainFolder : str = sys.argv[1]
    prFolder : str = sys.argv[2]

    head_Repository : Optional[str] = os.environ.get('HEAD_REPOSITORY')
    head_Branch : Optional[str] = os.environ.get('HEAD_BRANCH')
    if(head_Repository == None or head_Branch == None):
        raise Exception("Git arguments not found")

    # Connect to GitHub
    gitToken : Optional[str] = os.environ.get('GITHUB_TOKEN')
    if(gitToken == None):
        raise Exception("GITHUB_TOKEN not in the environment")
    
#    gitHub : github.Github = github.Github(gitToken)

    prRepo : Repository = GitHubRepository(head_Repository, head_Branch)

    # Backup current sys.path
    origSystemPath : list[str] = sys.path

    # Main branch Ecosystem, we compare pull request against this
    ecoMain : Ecosystem = getEcosystem(mainFolder)

    # Pull request ecosystem
    ecoPR : Ecosystem = getEcosystem(prFolder)

    sys.path = origSystemPath

    # Check if all changes in ecoPR are valid, consistent, authorized from the pull request repo
    tree : ValidationTree = ecoMain.checkIfChangesCanBeMerged(ecoPR, prRepo)

    # Need to create github comments for top 20 problems and indicate if there are more
    if(tree.hasErrors()):
        tree.printTree()
    return tree

    
if __name__ == "__main__":
    tree : ValidationTree = verifyPullRequest()
    if(tree.hasErrors()):
        tree.printTree()
        sys.exit(1)
