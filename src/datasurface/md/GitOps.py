from abc import ABC, abstractmethod
import re
from typing import Iterable, Mapping, Optional
from datasurface.md import Documentation
from datasurface.md.Documentation import Documentable

from datasurface.md.Lint import ValidationTree


class Repository(ABC, Documentable):
    """This is a repository which can store an ecosystem model. It is used to check whether changes are authorized when made from a repository"""
    def __init__(self, doc: Optional[Documentation]):
        ABC.__init__(self)
        Documentable.__init__(self, doc)

    @abstractmethod
    def lint(self, tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        raise NotImplementedError()

    def __eq__(self, __value: object) -> bool:
        if (ABC.__eq__(self, __value) and Documentable.__eq__(self, __value) and isinstance(__value, Repository)):
            return True
        else:
            return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class GitControlledObject(ABC, Documentable):
    """This is the base class for all objects which are controlled by a git repository"""
    def __init__(self, repo: 'Repository') -> None:
        ABC.__init__(self)
        Documentable.__init__(self, None)
        self.owningRepo: Repository = repo
        """This is the repository which is authorized to make changes to this object"""

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, GitControlledObject)):
            return self.owningRepo == __value.owningRepo and ABC.__eq__(self, __value) and Documentable.__eq__(self, __value)
        else:
            return False

    @abstractmethod
    def areTopLevelChangesAuthorized(self, proposed: 'GitControlledObject', changeSource: Repository, tree: ValidationTree) -> bool:
        """This should compare attributes which are locally authorized only"""
        if (self.owningRepo == changeSource):
            return True
        rc: bool = self.owningRepo == proposed.owningRepo
        if not rc:
            tree.addProblem(f"{self} changed owning repo")
        if (self.documentation != proposed.documentation):
            tree.addProblem(f"{self} changed documentation")
            rc = False
        return rc

    def superLint(self, tree: ValidationTree):
        rTree: ValidationTree = tree.createChild(self.owningRepo)
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
            if (self.owningRepo == changeSource):
                return
            rc: bool = self.areTopLevelChangesAuthorized(proposed, changeSource, vTree)
            if not rc:
                vTree.addProblem(f"{self} top level owned by {self.owningRepo} has been modified by an unauthorized source {changeSource}")

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
            if (obj.owningRepo != changeSource):
                vTree.addProblem(f"Key {key} has been deleted by an unauthorized source")

        for key in added_keys:
            # Check if the object was added by the specified change source
            obj: Optional[GitControlledObject] = proposed[key]
            if (obj.owningRepo != changeSource):
                vTree.addProblem(f"Key {key} has been added by an unauthorized source")

        # Now check each common object for changes
        common_keys: set[str] = current_keys.intersection(proposed_keys)
        for key in common_keys:
            prop: Optional[GitControlledObject] = proposed[key]
            curr: Optional[GitControlledObject] = current[key]
            # Check prop against curr for unauthorized changes
            cTree: ValidationTree = vTree.createChild(curr)
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

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, FakeRepository)):
            return super().__eq__(__value) and self.name == __value.name
        else:
            return False


class GitHubRepository(Repository):
    """This represents a GitHub Repository specifically, this branch should have an eco.py files in the root
    folder. The eco.py file should contain an ecosystem object which is used to construct the ecosystem"""
    def __init__(self, repo: str, branchName: str, doc: Optional[Documentation] = None) -> None:
        super().__init__(doc)
        self.repositoryName: str = repo
        """The name of the git repository from which changes to Team objects are authorized"""
        self.branchName: str = branchName
        """The name of the branch containing an eco.py to construct an ecosystem"""

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, GitHubRepository)):
            return super().__eq__(__value) and self.repositoryName == __value.repositoryName and self.branchName == __value.branchName
        else:
            return False

    def __str__(self) -> str:
        return f"GitRepository({self.repositoryName}/{self.branchName})"

    def is_valid_github_repo_name(self, name: str) -> bool:
        if not 1 <= len(name) <= 100:
            return False
        owner, repo = name.split('/')
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

    def is_valid_github_branch(self, branch: str) -> bool:
        # Branch names cannot contain the sequence ..
        if '..' in branch:
            return False

        # Branch names cannot have a . at the end
        if branch.endswith('.'):
            return False

        # Branch names cannot have multiple consecutive . characters
        if '..' in branch:
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

    def lint(self, tree: ValidationTree):
        """This checks if repository is valid syntaxically"""
        if (not self.is_valid_github_repo_name(self.repositoryName)):
            tree.addProblem("Repository name <{self.repositoryName}> is not valid")
        if (not self.is_valid_github_branch(self.branchName)):
            tree.addProblem("Branch name <{self.branchName}> is not valid")
