"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from abc import abstractmethod
from typing import Any, Optional

from datasurface.md.documentation import Documentation, Documentable
from datasurface.md.lint import ValidationTree, ValidationProblem, ProblemSeverity
from datasurface.md.json import JSONable
from typing import Mapping, Iterable
import re
from urllib.parse import urlparse, ParseResult
from datasurface.md.credential import Credential, CredentialType


class Repository(Documentable, JSONable):
    """This is a repository which can store an ecosystem model. It is used to check whether changes are authorized when made from a repository"""
    def __init__(self, doc: Optional[Documentation], credential: Optional[Credential] = None):
        Documentable.__init__(self, doc)
        self.credential: Optional[Credential] = credential

    @abstractmethod
    def lint(self, tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        raise NotImplementedError()

    def __eq__(self, other: object) -> bool:
        if (Documentable.__eq__(self, other) and isinstance(other, Repository) and self.credential == other.credential):
            return True
        else:
            return False

    @abstractmethod
    def eqForAuthorization(self, other: 'Repository') -> bool:
        """This checks if the repositories are equal for authorization purposes. It should ignore the credential for example."""
        raise NotImplementedError()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        if (self.credential):
            rc.update({"credential": self.credential.to_json()})
        return rc


class RepositoryNotAuthorizedToMakeChanges(ValidationProblem):
    """This indicates a repository is not authorized to make changes"""
    def __init__(self, owningRepo: Repository, obj: object, changeSource: Repository) -> None:
        super().__init__(f"'{obj}' owned by {owningRepo} cannot be changed by repo {changeSource}", ProblemSeverity.ERROR)


class GitControlledObject(Documentable):
    """This is the base class for all objects which are controlled by a git repository"""
    def __init__(self, repo: 'Repository') -> None:
        Documentable.__init__(self, None)
        self.owningRepo: Repository = repo
        """This is the repository which is authorized to make changes to this object"""

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, GitControlledObject)):
            return self.owningRepo == other.owningRepo and Documentable.__eq__(self, other)
        else:
            return False

    def dictsAreDifferent(self, current_dict: dict[str, Any], proposed_dict: dict[str, Any], validation_tree: ValidationTree, dict_name: str) -> bool:
        """This checks if the current and proposed dictionaries are different and if so adds problems to the validation tree highlighting the differences"""
        if current_dict != proposed_dict:
            self.showDictChangesAsProblems(current_dict, proposed_dict, validation_tree.addSubTree(self))
            return True
        return False

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "owningRepo": self.owningRepo.to_json()})
        return rc

    @abstractmethod
    def areTopLevelChangesAuthorized(self, proposed: 'GitControlledObject', changeSource: Repository, tree: ValidationTree) -> bool:
        """This should compare attributes which are locally authorized only. Any differences should be added to the tree as problems"""
        if (self.owningRepo.eqForAuthorization(changeSource)):
            return True
        rc: bool = self.owningRepo == proposed.owningRepo
        if not rc:
            tree.addRaw(RepositoryNotAuthorizedToMakeChanges(self.owningRepo, self.owningRepo, changeSource))
        if (self.documentation != proposed.documentation):
            tree.addRaw(RepositoryNotAuthorizedToMakeChanges(self.owningRepo, self.documentation, changeSource))
            rc = False
        return rc

    def superLint(self, tree: ValidationTree):
        rTree: ValidationTree = tree.addSubTree(self.owningRepo)
        self.owningRepo.lint(rTree)
        if (self.documentation):
            self.documentation.lint(rTree)

    def checkTopLevelAttributeChangesAreAuthorized(self, proposed: 'GitControlledObject', changeSource: 'Repository', vTree: ValidationTree) -> None:
        """This checks if the local attributes of the object have been modified by the authorized change source"""
        # Check if the ecosystem has been modified at all
        if (self == proposed):
            return
        else:
            # If changer is authorized then changes are allowed
            if (self.owningRepo.eqForAuthorization(changeSource)):
                return
            rc: bool = self.areTopLevelChangesAuthorized(proposed, changeSource, vTree)
            if not rc:
                vTree.addRaw(RepositoryNotAuthorizedToMakeChanges(self.owningRepo, self, changeSource))

    @abstractmethod
    def checkIfChangesAreAuthorized(self, proposed: 'GitControlledObject', changeSource: 'Repository', vTree: ValidationTree) -> None:
        """This checks if the differences between the current and proposed objects are authorized by the specified change source"""
        raise NotImplementedError()

    def checkDictChangesAreAuthorized(self, current: Mapping[str, 'GitControlledObject'], proposed: Mapping[str, 'GitControlledObject'],
                                      changeSource: 'Repository', vTree: ValidationTree) -> None:
        """This checks if the current dict has been modified relative to the specified change source"""
        """This checks if any objects has been added or removed relative to e"""

        # Get the object keys from the current main ecosystem
        current_keys: set[str] = set(current)

        # Get the object keys from the proposed ecosystem
        proposed_keys: set[str] = set(proposed.keys())

        deleted_keys: set[str] = current_keys - proposed_keys
        added_keys: set[str] = proposed_keys - current_keys

        # first check any top level objects have been added or removed by the correct change sources
        for key in deleted_keys:
            # Check if the object was deleted by the authoized change source
            obj: Optional[GitControlledObject] = current[key]
            if (not obj.owningRepo.eqForAuthorization(changeSource)):
                vTree.addRaw(RepositoryNotAuthorizedToMakeChanges(
                    obj.owningRepo,
                    f"Key {key} has been deleted",
                    changeSource))

        for key in added_keys:
            # Check if the object was added by the specified change source
            obj: Optional[GitControlledObject] = proposed[key]
            if (not obj.owningRepo.eqForAuthorization(changeSource)):
                vTree.addRaw(RepositoryNotAuthorizedToMakeChanges(
                    obj.owningRepo,
                    f"Key {key} has been added",
                    changeSource))

        # Now check each common object for changes
        common_keys: set[str] = current_keys.intersection(proposed_keys)
        for key in common_keys:
            prop: Optional[GitControlledObject] = proposed[key]
            curr: Optional[GitControlledObject] = current[key]
            # Check prop against curr for unauthorized changes
            cTree: ValidationTree = vTree.addSubTree(curr)
            curr.checkIfChangesAreAuthorized(prop, changeSource, cTree)

    def showDictChangesAsProblems(self, current: Mapping[str, object], proposed: Mapping[str, object], vTree: ValidationTree) -> None:
        """This converts any changes between the dictionaries into problems for the validation tree"""
        # Get the object keys from the current main ecosystem
        current_keys: set[str] = set(current)

        # Get the object keys from the proposed ecosystem
        proposed_keys: set[str] = set(proposed.keys())

        deleted_keys: set[str] = current_keys - proposed_keys
        added_keys: set[str] = proposed_keys - current_keys

        # first check any top level objects have been added or removed by the correct change sources
        for key in deleted_keys:
            # Check if the object was deleted by the authoized change source
            obj: Optional[object] = current[key]
            vTree.addProblem(f"{str(obj)} has been deleted")

        for key in added_keys:
            # Check if the object was added by the specified change source
            obj: Optional[object] = proposed[key]
            vTree.addProblem(f"{str(obj)} has been added")

        # Now check each common object for changes
        common_keys: set[str] = current_keys.intersection(proposed_keys)
        for key in common_keys:
            prop: Optional[object] = proposed[key]
            curr: Optional[object] = current[key]
            if (prop != curr):
                vTree.addProblem(f"{str(prop)} has been modified")

    def showSetChangesAsProblems(self, current: Iterable[object], proposed: Iterable[object], vTree: ValidationTree) -> None:
        """This converts any changes between the dictionaries into problems for the validation tree"""
        # Get the object keys from the current main ecosystem
        current_keys: set[object] = set(current)

        # Get the object keys from the proposed ecosystem
        proposed_keys: set[object] = set(proposed)

        deleted_keys: set[object] = current_keys - proposed_keys
        added_keys: set[object] = proposed_keys - current_keys

        # first check any top level objects have been added or removed by the correct change sources
        for key in deleted_keys:
            # Check if the object was deleted by the authoized change source
            vTree.addProblem(f"{str(key)} has been deleted")

        for key in added_keys:
            # Check if the object was added by the specified change source
            vTree.addProblem(f"{str(key)} has been added")

        # Now check each common object for changes
        common_keys: set[object] = current_keys.intersection(proposed_keys)
        for key in common_keys:
            prop: Optional[object] = None
            curr: Optional[object] = None
            if key in proposed:
                prop = key
            if key in current:
                curr = key
            if (prop != curr):
                vTree.addProblem(f"{str(key)} has been modified")


class FakeRepository(Repository):
    """Fake implementation for test cases only"""
    def __init__(self, name: str, doc: Optional[Documentation] = None) -> None:
        super().__init__(doc)
        self.name = name

    def lint(self, tree: ValidationTree) -> None:
        pass

    def __str__(self) -> str:
        return f"FakeRepository({self.name})"

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, FakeRepository)):
            return super().__eq__(other) and self.name == other.name
        else:
            return False

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc

    def eqForAuthorization(self, other: 'Repository') -> bool:
        """This checks if the repositories are equal for authorization purposes. It should ignore the credential for example."""
        if (isinstance(other, FakeRepository) and self.name == other.name):
            return True
        else:
            return False


class GitRepository(Repository):
    def __init__(self, doc: Optional[Documentation] = None, credential: Optional[Credential] = None) -> None:
        super().__init__(doc, credential)

    def is_valid_github_repo_name(self, name: str) -> bool:
        if not 1 <= len(name) <= 100:
            return False
        parts: list[str] = name.split('/')
        if len(parts) != 2:
            return False
        owner: str = parts[0]
        repo: str = parts[1]
        if (owner == '' or repo == ''):
            return False

        if name[0] == '-' or name[-1] == '-':
            return False
        if '..' in name:
            return False
        if '/' not in name:
            return False
        owner, repo = name.split('/')
        if not owner or not repo:
            return False
        pattern = r'^[a-zA-Z0-9_.-]+$'
        return re.match(pattern, owner) is not None and re.match(pattern, repo) is not None

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def is_valid_github_branch(self, branch: str) -> bool:
        # Branch names cannot contain the sequence ..
        if '..' in branch:
            return False

        # Branch names cannot have a . at the end
        if branch.endswith('.'):
            return False

        # Branch names cannot contain any of the following characters: ~ ^ : \ * ? [ ] /
        if any(char in branch for char in ['~', '^', ':', '\\', '*', '?', '[', ']', '/']):
            return False

        # Branch names cannot start with -
        if branch.startswith('-'):
            return False

        # Branch names can only contain alphanumeric characters, ., -, and _
        pattern = r'^[a-zA-Z0-9_.-]+$'
        return re.match(pattern, branch) is not None


class GitHubRepository(GitRepository):
    """This represents a GitHub Repository specifically, this branch should have an eco.py files in the root
    folder. The eco.py file should contain an ecosystem object which is used to construct the ecosystem"""
    def __init__(self, repo: str, branchName: str, doc: Optional[Documentation] = None, credential: Optional[Credential] = None) -> None:
        super().__init__(doc, credential)
        self.repositoryName: str = repo
        """The name of the git repository from which changes to objects are authorized. The name is in the form 'owner/repo'. The system will
        add https://github.com/ to the front of the name implicitly, there is no need to include it in the name."""
        self.branchName: str = branchName
        """The name of the branch containing an eco.py to construct an ecosystem"""

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, GitHubRepository)):
            return super().__eq__(other) and self.repositoryName == other.repositoryName and self.branchName == other.branchName
        else:
            return False

    def eqForAuthorization(self, other: 'Repository') -> bool:
        """This checks if the repositories are equal for authorization purposes. It should ignore the credential for example."""
        if (isinstance(other, GitHubRepository) and self.repositoryName == other.repositoryName and self.branchName == other.branchName):
            return True
        else:
            return False

    def __hash__(self) -> int:
        return hash(self.repositoryName) + hash(self.branchName)

    def __str__(self) -> str:
        return f"GitRepository({self.repositoryName}/{self.branchName})"

    def lint(self, tree: ValidationTree):
        """This checks if repository is valid syntaxically"""
        if (not self.is_valid_github_repo_name(self.repositoryName)):
            tree.addProblem(f"Repository name <{self.repositoryName}> is not valid")
        if (not self.is_valid_github_branch(self.branchName)):
            tree.addProblem(f"Branch name <{self.branchName}> is not valid")
        if (self.credential and self.credential.credentialType != CredentialType.API_TOKEN):
            tree.addProblem(f"Credential type <{self.credential.credentialType}> must be an API token")

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "repositoryName": self.repositoryName, "branchName": self.branchName})
        return rc


class GitLabRepository(GitRepository):
    """This provides the metadata for a Gitlab based repository. The service url is provided and the repository name and branch"""
    def __init__(self, repoUrl: str, repo: str, branchName: str, doc: Optional[Documentation] = None) -> None:
        super().__init__(doc)
        self.repoUrl: str = repoUrl
        self.repositoryName: str = repo
        """The name of the git repository from which changes to Team objects are authorized"""
        self.branchName: str = branchName
        """The name of the branch containing an eco.py to construct an ecosystem"""

    def __hash__(self) -> int:
        return hash(self.repoUrl) + hash(self.repositoryName) + hash(self.branchName)

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, GitLabRepository)):
            return super().__eq__(other) and self.repoUrl == other.repoUrl and self.repositoryName == other.repositoryName and \
                self.branchName == other.branchName
        else:
            return False

    def eqForAuthorization(self, other: 'Repository') -> bool:
        """This checks if the repositories are equal for authorization purposes. It should ignore the credential for example."""
        if (isinstance(other, GitLabRepository) and self.repoUrl == other.repoUrl and self.repositoryName == other.repositoryName and
                self.branchName == other.branchName):
            return True
        else:
            return False

    def __str__(self) -> str:
        return f"GitLabRepository({self.repoUrl}/{self.repositoryName}/{self.branchName})"

    def is_valid_url(self, url: str) -> bool:
        try:
            result: ParseResult = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def lint(self, tree: ValidationTree):
        """This checks if repository is valid syntaxically"""
        if (not self.is_valid_url(self.repoUrl)):
            tree.addProblem(f"Repository url <{self.repoUrl}> is not valid")
        if (not self.is_valid_github_repo_name(self.repositoryName)):
            tree.addProblem(f"Repository name <{self.repositoryName}> is not valid")
        if (not self.is_valid_github_branch(self.branchName)):
            tree.addProblem(f"Branch name <{self.branchName}> is not valid")

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "repoUrl": self.repoUrl, "repositoryName": self.repositoryName, "branchName": self.branchName})
        return rc
