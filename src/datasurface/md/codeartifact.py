
"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.governance import CodeArtifact
from datasurface.md.repo import GitHubRepository
from typing import Any


class PythonRepoCodeArtifact(CodeArtifact):
    """This describes a python repo which can be used to transform data in a workspace"""
    def __init__(self, repo: GitHubRepository) -> None:
        super().__init__()
        self.repo: GitHubRepository = repo

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "repo": self.repo.to_json()
        }