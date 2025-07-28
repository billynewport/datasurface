"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import os
import sys
from typing import Optional
from datasurface.handler.cicd import RepositorywithCICD

from datasurface.md.repo import GitHubRepository, Repository

from datasurface.md import ValidationTree


class GitHubCICD(RepositorywithCICD):
    """This is the GitHub implementation of the RespositorywithCICD class together
    with a main method which is the entry point for the GitHub action"""
    # This is a GitHub implementation of RespositorywithCICD
    def __init__(self, name: str):
        super().__init__(name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GitHubCICD):
            return False
        return super().__eq__(other)

    def __hash__(self) -> int:
        return super().__hash__()

    def createRepositoryForPullRequestRepo(self) -> Repository:
        """This creates a GitHubRepository instance representing the GitHub pull request in progress.
        It uses the environment variables provided by the GitHubCICD environment"""
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
        return prRepo


if __name__ == "__main__":
    # This is the entry point for the GitHub action
    cicd: RepositorywithCICD = GitHubCICD("GitHub")
    mainFolder: str = sys.argv[1]
    prFolder: str = sys.argv[2]
    tree: ValidationTree = cicd.verifyPullRequest(mainFolder, prFolder)
    tree.printTree()  # Print any errors or warnings

    # If errors then exit with a 1 to fail the check
    if (tree.getErrors()):
        sys.exit(1)
    else:
        sys.exit(0)
