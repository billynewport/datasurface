from abc import ABC, abstractmethod
import copy
import importlib
import sys
from types import ModuleType
from typing import Optional

from datasurface.md.Governance import Ecosystem
from datasurface.md.GitOps import Repository
from datasurface.md.Lint import ValidationTree


class RespositorywithCICD(ABC):
    """This is the base class for CICD systems, it is used to verify pull requests and to create the repository for the pull request"""
    def __init__(self, name: str):
        self.name: str = name

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RespositorywithCICD):
            return False
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    @abstractmethod
    def createRepositoryForPullRequestRepo(self) -> Repository:
        """Creates the repository for the incoming pull request from the environment"""
        pass

    def getEcosystem(self, path: str) -> Optional[Ecosystem]:
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

    def verifyPullRequest(self, mainFolder: str, prFolder: str) -> ValidationTree:
        """This is the actual github action handler"""

        prRepo: Repository = self.createRepositoryForPullRequestRepo()

        # Main branch Ecosystem, we compare pull request against this
        ecoMain: Optional[Ecosystem] = self.getEcosystem(mainFolder)

        # Pull request ecosystem
        ecoPR: Optional[Ecosystem] = self.getEcosystem(prFolder)
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
                return tree
