
import copy
import importlib
import os
import sys
from types import ModuleType
from typing import Optional

from datasurface.md.Governance import Ecosystem
from datasurface.md.GitOps import GitHubRepository, Repository
from datasurface.md.Lint import ValidationTree


def getEcosystem(path: str) -> Optional[Ecosystem]:
    """This tries to bootstrap an Ecosystem from the file eco.py on a specific path"""
    origSystemPath: list[str] = copy.deepcopy(sys.path)
    try:
        sys.path.append(path)

        # Remove the module from sys.modules to force a reload
        if 'eco' in sys.modules:
            del sys.modules['eco']

        try:
            module: ModuleType = importlib.import_module("eco")
            function = getattr(module, "createEcosystem")
        except ModuleNotFoundError:
            # Should only happen on inital setup of a repository
            return None

        # This should now point to createEcosystem() -> Ecosystem in the eco.py file on the path specified
        eco: Ecosystem = function()
        # Flesh out
        eco.lintAndHydrateCaches()

        return eco
    finally:
        sys.path = origSystemPath


def verifyPullRequest() -> ValidationTree:
    """This is the actual github action handler"""
    mainFolder: str = sys.argv[1]
    prFolder: str = sys.argv[2]

    head_Repository: Optional[str] = os.environ.get('HEAD_REPOSITORY')
    head_Branch: Optional[str] = os.environ.get('HEAD_BRANCH')
    if (head_Repository is None or head_Branch is None):
        raise Exception("Git arguments not found")

    # Connect to GitHub
    gitToken: Optional[str] = os.environ.get('GITHUB_TOKEN')
    if (gitToken is None):
        raise Exception("GITHUB_TOKEN not in the environment")

#    gitHub : github.Github = github.Github(gitToken)

    prRepo: Repository = GitHubRepository(head_Repository, head_Branch)

    # Main branch Ecosystem, we compare pull request against this
    ecoMain: Optional[Ecosystem] = getEcosystem(mainFolder)

    # Pull request ecosystem
    ecoPR: Optional[Ecosystem] = getEcosystem(prFolder)
    if (ecoPR is None):
        raise Exception("eco.py not found in pull request")
    else:
        # If this is an initial checkin, we don't have a main branch to compare against
        if ecoMain is None:
            if prRepo == ecoPR.owningRepo:
                return ValidationTree(ecoPR)
            else:
                tree: ValidationTree = ValidationTree(ecoPR)
                tree.addProblem("Initial checkin must be to the same repository")
                return tree
        else:
            # Check if all changes in ecoPR are valid, consistent, authorized from the pull request repo
            tree: ValidationTree = ecoMain.checkIfChangesCanBeMerged(ecoPR, prRepo)

            # Need to create github comments for top 20 problems and indicate if there are more
            if (tree.hasErrors()):
                tree.printTree()
            return tree


if __name__ == "__main__":
    tree: ValidationTree = verifyPullRequest()
    tree.printTree()
    if (tree.hasErrors()):
        sys.exit(1)
    else:
        sys.exit(0)
