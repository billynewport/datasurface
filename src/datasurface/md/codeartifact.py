
"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.governance import CodeArtifact
from datasurface.md.repo import GitHubRepository
from datasurface.md.lint import ValidationTree
from datasurface.md import Ecosystem
from typing import Any


class PythonRepoCodeArtifact(CodeArtifact):
    """This describes a python repo which can be used to transform data in a workspace."""
    def __init__(self, repo: GitHubRepository, version: str) -> None:
        super().__init__()
        self.repo: GitHubRepository = repo
        self.version: str = version

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "repo": self.repo.to_json(),
            "version": self.version
        }

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PythonRepoCodeArtifact):
            return self.repo == other.repo and self.version == other.version
        return False

    def __str__(self) -> str:
        return f"PythonRepoCodeArtifact({self.repo}, {self.version})"

    def __hash__(self) -> int:
        return hash(f"{self.repo}#{self.version}")

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        self.repo.lint(tree.addSubTree(self.repo))
