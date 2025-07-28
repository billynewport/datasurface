"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from abc import ABC, abstractmethod
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md import InternalLintableObject
from datasurface.md import Repository
from datasurface.md import ValidationTree
from datasurface.md import Ecosystem
from typing import Optional


class RepositorywithCICD(ABC):
    """This is the base class for CICD systems, it is used to verify pull requests and to create the repository for the pull request"""
    def __init__(self, name: str):
        self.name: str = name

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RepositorywithCICD):
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

    def verifyPullRequest(self, mainFolder: str, prFolder: str) -> ValidationTree:
        """This is the actual github action handler. This returns a ValidationTree with the issues with both the 'Main' and 'Proposed' models as subtrees."""

        prRepo: Repository = self.createRepositoryForPullRequestRepo()

        # Main branch Ecosystem, we compare pull request against this
        # This can be missing if this is an initial checkin
        rcTree: ValidationTree = ValidationTree(InternalLintableObject())
        ecoMain: Optional[Ecosystem]
        treeMain: Optional[ValidationTree]
        ecoMain, treeMain = loadEcosystemFromEcoModule(mainFolder)
        if treeMain is not None:
            rcTree.addNamedSubTree(treeMain, "Main")
        if treeMain is not None and treeMain.hasErrors():
            rcTree.addProblem("eco.py not found or failed lint in main branch")
            return rcTree

        # Pull request ecosystem, we compare main against this, it must exist
        ecoPR: Optional[Ecosystem]
        treePR: Optional[ValidationTree]
        ecoPR, treePR = loadEcosystemFromEcoModule(prFolder)
        if treePR is not None:
            rcTree.addNamedSubTree(treePR, "Proposed")
        if ecoPR is None:
            rcTree.addProblem("proposed eco.py not found")
            return rcTree
        if treePR is not None and treePR.hasErrors():
            rcTree.addProblem("proposed eco.py not found or failed lint")
            return rcTree
        else:
            # If this is an initial checkin, we don't have a main branch to compare against
            if ecoMain is None:
                if prRepo == ecoPR.owningRepo:
                    return rcTree
                else:
                    rcTree.addProblem("Initial checkin must be to the same repository")
                    return rcTree
            else:
                # Check if all changes in ecoPR are valid, consistent, authorized from the pull request repo
                rcTree.addNamedSubTree(ecoMain.checkIfChangesCanBeMerged(ecoPR, prRepo), "Merged")

                # Need to create github comments for top 20 problems and indicate if there are more
                return rcTree
