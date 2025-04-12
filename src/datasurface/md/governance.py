"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from dataclasses import dataclass
from collections import OrderedDict
import os
import tempfile
from typing import Any, Callable, Optional, Sequence, Type, TypeVar, Union, cast, List
from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from typing import Generic

import re

from datasurface.md.exceptions import NameMustBeANSISQLIdentifierException
from datasurface.md.exceptions import AttributeAlreadySetException, ObjectAlreadyExistsException, ObjectDoesntExistException
from datasurface.md.exceptions import UnknownArgumentException
from datasurface.md.lint import AttributeNotSet, ConstraintViolation, DataTransformerMissing, DuplicateObject, NameHasBadSynthax, NameMustBeSQLIdentifier, \
        ObjectIsDeprecated, ObjectMissing, ObjectNotCompatibleWithPolicy, ObjectWrongType, ProductionDatastoreMustHaveClassifications, \
        UnauthorizedAttributeChange, ProblemSeverity, UnknownChangeSource, UnknownObjectReference, ValidationProblem, ValidationTree, UserDSLObject, \
        InternalLintableObject

import hashlib
from typing import Tuple, Dict, Mapping, Iterable
from urllib.parse import urlparse, ParseResult


"""This file contains the bulk of the objects used in the DSL model used by DataSurface"""


sql_reserved_words: list[str] = [
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "INSERT", "UPDATE", "DELETE",
    "CREATE", "ALTER", "DROP", "TABLE", "DATABASE", "INDEX", "VIEW", "TRIGGER",
    "PROCEDURE", "FUNCTION", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "ON",
    "GROUP", "BY", "ORDER", "HAVING", "UNION", "EXCEPT", "INTERSECT", "CASE",
    "WHEN", "THEN", "ELSE", "END", "AS", "DISTINCT", "NULL", "IS", "BETWEEN",
    "LIKE", "IN", "EXISTS", "ALL", "ANY", "SOME", "CAST", "CONVERT", "COALESCE",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "TOP", "LIMIT", "FETCH", "OFFSET",
    "ROW", "ROWS", "ONLY", "FIRST", "NEXT", "VALUE", "VALUES", "INTO", "SET",
    "OUTPUT", "DECLARE", "CURSOR", "FOR", "WHILE", "LOOP", "REPEAT", "IF",
    "ELSEIF", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "TRANSACTION", "TRY",
    "CATCH", "THROW", "USE", "USING", "COLLATE", "PLAN", "EXECUTE", "PREPARE",
    "DEALLOCATE", "ASC", "DESC"]

sql_reserved_words_as_set: set[str] = set(sql_reserved_words)


def is_valid_sql_identifier(identifier: str) -> bool:
    """This checks if the string is a valid SQL identifier"""
    # Check for reserved words
    if (identifier.upper() in sql_reserved_words_as_set):
        return False
    # Regular expression for a valid SQL identifier
    pattern = r'^[a-zA-Z][a-zA-Z0-9_]{0,127}$'
    return (re.match(pattern, identifier)) is not None


def is_valid_azure_key_vault_name(name: str) -> bool:
    # Regular expression for a valid Azure Key Vault name
    pattern = r'^[a-z0-9]{3,24}$'
    return (re.match(pattern, name)) is not None


def is_valid_hostname_or_ip(s: str) -> bool:
    """This checks if the string is a valid hostname or IP address"""
    # Check if it's a valid IPv4 address
    pattern_ipv4 = r"^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"
    if re.fullmatch(pattern_ipv4, s) is not None:
        return True

    # Check if it's a valid IPv6 address
    pattern_ipv6 = (
        r"^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|"
        r"([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}"
        r"(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|"
        r"([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|"
        r"fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|"
        r"1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|"
        r"1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$"
    )

    if re.fullmatch(pattern_ipv6, s) is not None:
        return True

    # Check hostname
    pattern_hostname = r"^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$"
    if re.fullmatch(pattern_hostname, s) is not None and len(s) <= 253:
        return True

    return False


class JSONable(ABC):
    """This is a base class for all objects which can be converted to a JSON object"""
    def __init__(self) -> None:
        ABC.__init__(self)

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {"_type": self.__class__.__name__}
        return rc


class ANSI_SQL_NamedObject(UserDSLObject):
    name: str
    """This is the base class for objects in the model which must have an SQL identifier compatible name. These
    objects may have names which are using in creating database artifacts such as Tables, views, columns"""
    def __init__(self, name: str) -> None:
        self.name: str = name
        """The name of the object"""
        if not is_valid_sql_identifier(self.name):
            raise NameMustBeANSISQLIdentifierException(self.name)
        UserDSLObject.__init__(self)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ANSI_SQL_NamedObject) and self.name == other.name

    def checkForBackwardsCompatibility(self, other: object, vTree: ValidationTree) -> bool:
        if (not isinstance(other, ANSI_SQL_NamedObject)):
            vTree.addProblem(f"Object {other} is not an ANSI_SQL_NamedObject")
            return False

        """Returns true if this column is backwards compatible with the other column"""
        # TODO Add support to changing the column data type to a compatible type
        if (self.name != other.name):
            vTree.addProblem(f"Column name changed from {self.name} to {other.name}")
        return True

    def nameLint(self, tree: ValidationTree) -> None:
        if not is_valid_sql_identifier(self.name):
            tree.addRaw(NameHasBadSynthax(f"Name {self.name} is not a valid ANSI SQL identifier"))

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"


def validate_cron_string(cron_string: str):
    # Split the cron string into fields
    fields: list[str] = cron_string.split()

    # Check that there are exactly 5 fields
    if len(fields) != 5:
        return False

    # Define the valid ranges for each field
    ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]

    # Check each field
    for field, (min_value, max_value) in zip(fields, ranges):
        # If the field is a '*', it's valid
        if field == '*':
            continue

        # If the field contains a ',', it's a list of values
        if ',' in field:
            values: list[str] = field.split(',')
        else:
            values: list[str] = [field]

        # Check each value
        dashList: list[str] = []
        start: str = ''
        end: str = ''
        for value in values:
            if '/' in value:
                dashList = value.split('/')
                if (len(dashList) != 2):
                    return False
                start = dashList[0]
                if (start != '*'):
                    return False
                end = dashList[1]
                if not end.isdigit() or not end.isdigit() or not (min_value <= int(end) <= max_value):
                    return False
            # If the value contains a '-', it's a range
            elif '-' in value:
                dashList = value.split('-')
                if (len(dashList) != 2):
                    return False
                start = dashList[0]
                end = dashList[1]
                if not start.isdigit() or not end.isdigit() or not (min_value <= int(start) <= int(end) <= max_value):
                    return False
            else:
                # The value should be a single number
                if not value.isdigit() or not (min_value <= int(value) <= max_value):
                    return False

    # If we've made it this far, the cron string is valid
    return True


R = TypeVar('R')
A = TypeVar('A')


class Memoize(Generic[A, R]):
    """Decorator to cache previous calls to a method"""
    def __init__(self, func: Callable[[A], R]) -> None:
        self.func = func
        self.cache: Dict[Tuple[A, ...], R] = {}

    def __call__(self, *args: A) -> R:
        if args in self.cache:
            return self.cache[args]

        result = self.func(*args)
        self.cache[args] = result
        return result


def memoize(func: Callable[[A], R]) -> Memoize[A, R]:
    return Memoize(func)


class Documentation(UserDSLObject):
    """This is the base class for all documentation objects. There are subclasses for different ways to express documentation such as plain text \
    or markdown and so on."""
    def __init__(self, description: str, tags: Optional[OrderedDict[str, str]] = None) -> None:
        UserDSLObject.__init__(self)
        if not description:
            raise ValueError("Description cannot be empty")
        if tags is not None:
            for key, value in tags.items():
                if not key or not value:
                    raise ValueError("Tag keys and values cannot be empty")
        self.description: str = description
        self.tags: Optional[OrderedDict[str, str]] = tags

    def __eq__(self, other: object) -> bool:
        if other is None or not isinstance(other, Documentation):
            return False
        return (self.description == other.description and
                (self.tags == other.tags if self.tags and other.tags else self.tags is other.tags))

    def __str__(self) -> str:
        tags_str = f", tags={self.tags}" if self.tags else ""
        return f"Documentation(description='{self.description}'{tags_str})"

    @abstractmethod
    def lint(self, tree: ValidationTree):
        pass

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {"description": self.description}
        if (self.tags is not None):
            rc["tags"] = {key: value for key, value in self.tags.items()}
        return rc


class Documentable(UserDSLObject):
    """This is the base class for all objects which can have documentation."""
    def __init__(self, documentation: Optional[Documentation]) -> None:
        UserDSLObject.__init__(self)
        self.documentation: Optional[Documentation] = documentation

    def __eq__(self, other: object):
        if (not isinstance(other, Documentable)):
            return False
        return self.documentation == other.documentation

    def __str__(self) -> str:
        return f"Documentable({self.documentation})"


class PlainTextDocumentation(Documentation):
    def __init__(self, description: str, tags: Optional[OrderedDict[str, str]] = None) -> None:
        super().__init__(description, tags)

    def __eq__(self, other: object):
        if (not isinstance(other, PlainTextDocumentation)):
            return False
        return super().__eq__(other)

    def lint(self, tree: ValidationTree):
        pass

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "description": self.description, "tags": self.tags}


class MarkdownDocumentation(Documentation):
    def __init__(self, description: str, markdown: str, tags: Optional[OrderedDict[str, str]] = None) -> None:
        super().__init__(description, tags)
        self.markdown: str = markdown

    def __eq__(self, other: object):
        if (not isinstance(other, MarkdownDocumentation)):
            return False
        return super().__eq__(other) and self.markdown == other.markdown

    def lint(self, tree: ValidationTree):
        pass

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "description": self.description, "markdown": self.markdown, "tags": self.tags}


class Repository(Documentable, UserDSLObject, JSONable):
    """This is a repository which can store an ecosystem model. It is used to check whether changes are authorized when made from a repository"""
    def __init__(self, doc: Optional[Documentation]):
        UserDSLObject.__init__(self)
        Documentable.__init__(self, doc)

    @abstractmethod
    def lint(self, tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        raise NotImplementedError()

    def __eq__(self, other: object) -> bool:
        if (UserDSLObject.__eq__(self, other) and Documentable.__eq__(self, other) and isinstance(other, Repository)):
            return True
        else:
            return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__}


class RepositoryNotAuthorizedToMakeChanges(ValidationProblem):
    """This indicates a repository is not authorized to make changes"""
    def __init__(self, owningRepo: Repository, obj: object, changeSource: Repository) -> None:
        super().__init__(f"'{obj}' owned by {owningRepo} cannot be changed by repo {changeSource}", ProblemSeverity.ERROR)


class GitControlledObject(Documentable, UserDSLObject):
    """This is the base class for all objects which are controlled by a git repository"""
    def __init__(self, repo: 'Repository') -> None:
        Documentable.__init__(self, None)
        UserDSLObject.__init__(self)
        self.owningRepo: Repository = repo
        """This is the repository which is authorized to make changes to this object"""

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, GitControlledObject)):
            return self.owningRepo == other.owningRepo and UserDSLObject.__eq__(self, other) and Documentable.__eq__(self, other)
        else:
            return False

    def _check_dict_changes(self, current_dict: dict[str, Any], proposed_dict: dict[str, Any], validation_tree: ValidationTree, dict_name: str) -> bool:
        if current_dict != proposed_dict:
            self.showDictChangesAsProblems(current_dict, proposed_dict, validation_tree.addSubTree(self))
            return True
        return False

    @abstractmethod
    def areTopLevelChangesAuthorized(self, proposed: 'GitControlledObject', changeSource: Repository, tree: ValidationTree) -> bool:
        """This should compare attributes which are locally authorized only"""
        if (self.owningRepo == changeSource):
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
            if (self.owningRepo == changeSource):
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
            if (obj.owningRepo != changeSource):
                vTree.addRaw(RepositoryNotAuthorizedToMakeChanges(
                    obj.owningRepo,
                    f"Key {key} has been deleted",
                    changeSource))

        for key in added_keys:
            # Check if the object was added by the specified change source
            obj: Optional[GitControlledObject] = proposed[key]
            if (obj.owningRepo != changeSource):
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
        return {"_type": self.__class__.__name__, "name": self.name}


class GitRepository(Repository):
    def __init__(self, doc: Optional[Documentation] = None) -> None:
        super().__init__(doc)

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
    def __init__(self, repo: str, branchName: str, doc: Optional[Documentation] = None) -> None:
        super().__init__(doc)
        self.repositoryName: str = repo
        """The name of the git repository from which changes to Team objects are authorized"""
        self.branchName: str = branchName
        """The name of the branch containing an eco.py to construct an ecosystem"""

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, GitHubRepository)):
            return super().__eq__(other) and self.repositoryName == other.repositoryName and self.branchName == other.branchName
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

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "repositoryName": self.repositoryName, "branchName": self.branchName}


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
        return {"_type": self.__class__.__name__, "repoUrl": self.repoUrl, "repositoryName": self.repositoryName, "branchName": self.branchName}


T = TypeVar('T')


class Policy(Documentable, JSONable, Generic[T]):
    """Base class for all policies"""
    def __init__(self, name: str, doc: Optional[Documentation] = None) -> None:
        self.name: str = name
        Documentable.__init__(self, doc)
        JSONable.__init__(self)

    @abstractmethod
    def isCompatible(self, obj: T) -> bool:
        """Check if obj meets the policy"""
        raise NotImplementedError()

    def __eq__(self, other: object) -> bool:
        return Documentable.__eq__(self, other) and isinstance(other, Policy) and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "name": self.name}


P = TypeVar('P', bound=JSONable)


L = TypeVar('L')


class Literal(JSONable, Generic[L]):
    def __init__(self, value: L) -> None:
        JSONable.__init__(self)
        self.value: L = value

    def to_json(self) -> dict[str, Any]:
        return {"value": self.value}

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Literal):
            return False
        return self.value == other.value  # type: ignore

    def __hash__(self) -> int:
        return hash(self.value)


class AllowDisallowPolicy(Policy[P]):
    """This checks whether an object is explicitly allowed or explicitly forbidden"""
    def __init__(self, name: str, doc: Optional[Documentation], allowed: Optional[set[P]] = None, notAllowed: Optional[set[P]] = None) -> None:
        super().__init__(name, doc)
        self.allowed: Optional[set[P]] = allowed
        self.notAllowed: Optional[set[P]] = notAllowed
        if (self.allowed and self.notAllowed):
            commonValues: set[P] = self.allowed.intersection(self.notAllowed)
            if len(commonValues) != 0:
                raise Exception("AllowDisallow groups overlap")

    def isCompatible(self, obj: P) -> bool:
        if self.allowed and obj not in self.allowed:
            return False
        if self.notAllowed and obj in self.notAllowed:
            return False
        return True

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, AllowDisallowPolicy) and self.name == other.name

    def __str__(self):
        return f"{self.__class__.__name__}({self.name}, {self.allowed},{self.notAllowed})"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {"_type": self.__class__.__name__, "name": self.name}
        # Now add the set of allowed and set of not allowed by using the to_json method of the objects in the sets
        if self.allowed:
            rc["allowed"] = [obj.to_json() for obj in self.allowed]
        if self.notAllowed:
            rc["notAllowed"] = [obj.to_json() for obj in self.notAllowed]
        return rc


class DataClassification(JSONable):
    """Base class for defining data classifications"""
    def __init__(self):
        JSONable.__init__(self)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DataClassification)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__}


class SimpleDCTypes(Enum):
    """This is the privacy classification of the data"""
    PUB = 0
    """Publicly available data"""
    IP = 1
    """Internal public information"""
    PC1 = 2
    """Personal confidential information"""
    PC2 = 3
    """Personal confidential information"""
    """Names, addresses, phone numbers, etc."""
    CPI = 4
    MNPI = 5
    """Material non public information"""
    CSI = 6
    """Confidential Sensitive Information"""
    PC3 = 7
    """Personal confidential information, social security numbers, credit card numbers, etc."""


class SimpleDC(DataClassification):
    """Simple compound data classification, encodes PII.name for example"""
    def __init__(self, dcType: SimpleDCTypes, name: Optional[str] = None):
        self.dcType: SimpleDCTypes = dcType
        self.name: Optional[str] = name

    def __hash__(self) -> int:
        return hash(str(self.dcType) + (self.name if self.name else "<NULL>"))

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, SimpleDC) and self.dcType == o.dcType and self.name == o.name


class DataClassificationPolicy(AllowDisallowPolicy[DataClassification]):
    """This checks whether a data classification is explicitly allowed or explicitly forbidden"""
    def __init__(self, name: str, doc: Optional[Documentation], allowed: Optional[set[DataClassification]] = None,
                 notAllowed: Optional[set[DataClassification]] = None) -> None:
        super().__init__(name, doc, allowed, notAllowed)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DataClassificationPolicy) and self.allowed == other.allowed and self.notAllowed == other.notAllowed

    def __hash__(self) -> int:
        return super().__hash__()


class VerifyNoPrivacyDataVerify(DataClassificationPolicy):
    def __init__(self, doc: Optional[Documentation]) -> None:
        super().__init__("No privacy classification allowed", doc, None,
                         {
                            SimpleDC(SimpleDCTypes.PC1, "name"),
                            SimpleDC(SimpleDCTypes.PC2, "address"),
                            SimpleDC(SimpleDCTypes.CPI),
                            SimpleDC(SimpleDCTypes.MNPI),
                            SimpleDC(SimpleDCTypes.CSI),
                            SimpleDC(SimpleDCTypes.PC3, "ssn")}
                         )

    def __hash__(self) -> int:
        return super().__hash__()


class ProductionStatus(Enum):
    """This indicates whether the team is in production or not"""
    PRODUCTION = 0
    NOT_PRODUCTION = 1


class DeprecationStatus(Enum):
    """This indicates whether the team is deprecated or not"""
    NOT_DEPRECATED = 0
    DEPRECATED = 1


class DeprecationInfo(Documentable):
    """This is the deprecation information for an object"""
    def __init__(self, status: DeprecationStatus, reason: Optional[Documentation] = None) -> None:
        super().__init__(reason)
        self.status: DeprecationStatus = status
        """If it deprecated or not"""
        """If deprecated then this explains why and what an existing user should do, alternative dataset for example"""

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and \
            isinstance(other, DeprecationInfo) and self.status == other.status


def cyclic_safe_eq(a: object, b: object, visited: set[object]) -> bool:
    """This is a recursive equality checker which avoids infinite recursion by tracking visited objects. The \
        meta data objects have circular references which cause infinite recursion when using the default"""
    ida: int = id(a)
    idb: int = id(b)

    if (ida == idb):
        return True

    if (type(b) is not type(a)):
        return False

    if (idb > ida):
        ida, idb = idb, ida

    pair = (ida, idb)
    if (pair in visited):
        return True

    visited.add(pair)

    # Handle comparing dict objects
    if isinstance(a, dict) and isinstance(b, dict):
        d_a: dict[Any, Any] = a
        d_b: dict[Any, Any] = b

        if len(d_a) != len(d_b):
            return False
        for key in d_a:
            if key not in b or not cyclic_safe_eq(d_a[key], d_b[key], visited):
                return False
        return True

    # Handle comparing list objects
    if isinstance(a, list) and isinstance(b, list):
        l_a: list[Any] = a
        l_b: list[Any] = b

        if len(l_a) != len(l_b):
            return False
        for item_a, item_b in zip(l_a, l_b):
            if not cyclic_safe_eq(item_a, item_b, visited):
                return False
        return True

    # Now compare objects for equality
    try:
        self_vars: dict[str, Any] = vars(a)
    except TypeError:
        # This is a primitive type
        return a == b

    # Check same named attributes for equality
    for attr, value in vars(b).items():
        if (not attr.startswith("_")):
            if not cyclic_safe_eq(self_vars[attr], value, visited):
                return False

    return True


def handleUnsupportedObjectsToJson(obj: object) -> str:
    if isinstance(obj, Enum):
        return obj.name
    elif isinstance(obj, DataType):
        return str(obj.to_json())
    raise Exception(f"Unsupported object {obj} in to_json")


class DataType(UserDSLObject, JSONable):
    """Base class for all data types. These DataTypes are not nullable. Nullable status is a property of
    columns and is specified in the DDLColumn constructor"""
    def __init__(self) -> None:
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        pass

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other)

    def __str__(self) -> str:
        return str(self.__class__.__name__) + "()"

    @abstractmethod
    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        return True

    @abstractmethod
    def lint(self, vTree: ValidationTree) -> None:
        """This method is called to lint the data type. All validation of parameters should
        be here and not in the constructor"""
        pass

    def to_json(self) -> dict[str, Any]:
        """Converts this object to a JSON string"""
        d: dict[str, Any] = dict()
        d["type"] = self.__class__.__name__
        return d


class BoundedDataType(DataType):
    def __init__(self, maxSize: Optional[int]) -> None:
        super().__init__()
        self.maxSize: Optional[int] = maxSize

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        if self.maxSize is not None:
            rc.update({"maxSize": self.maxSize})
        else:
            rc.update({"maxSize": -1})
        return rc

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)
        if (self.maxSize is not None and self.maxSize <= 0):
            vTree.addProblem("Max size must be > 0")

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, BoundedDataType) and self.maxSize == other.maxSize

    def __str__(self) -> str:
        if (self.maxSize is None):
            return str(self.__class__.__name__) + "()"
        else:
            return str(self.__class__.__name__) + f"({self.maxSize})"

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if not isinstance(other, BoundedDataType):
            vTree.addProblem(f"Cannot compare {self.__class__.__name__} with {other.__class__.__name__}")
            return False

        # If this is unlimited then its compatible
        if (self.maxSize is None):
            return True

        # Must be unlimited to be compatible with unlimited
        if (self.maxSize and other.maxSize is None):
            vTree.addProblem(f"maxSize has been reduced from unlimited to {self.maxSize}")

        if (self.maxSize == other.maxSize):
            return True
        if (other.maxSize and self.maxSize and self.maxSize < other.maxSize):
            vTree.addProblem(f"maxSize has been reduced from {other.maxSize} to {self.maxSize}")
        return not vTree.hasErrors()


class ArrayType(BoundedDataType):
    """An Array of a specific data type with an optional bound on the number of elements"""
    def __init__(self, maxSize: Optional[int], type: DataType) -> None:
        super().__init__(maxSize)
        self.dataType: DataType = type

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"elementType": self.dataType.to_json()})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, ArrayType) and self.dataType == other.dataType

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        rc = super().isBackwardsCompatibleWith(other, vTree)
        if rc and isinstance(other, ArrayType):
            rc = rc and self.dataType.isBackwardsCompatibleWith(other.dataType, vTree)
        else:
            rc = False
        return rc

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.maxSize}, {self.dataType})"


class MapType(DataType):
    """A map of a specific data type"""
    def __init__(self, key_type: DataType, value_type: DataType) -> None:
        super().__init__()
        self.keyType: DataType = key_type
        self.valueType: DataType = value_type

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"keyType": self.keyType.to_json()})
        rc.update({"valueType": self.valueType.to_json()})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, MapType) and \
            self.keyType == other.keyType and self.valueType == other.valueType

    def isBackwardsCompatibleWith(self, other: DataType, vTree: ValidationTree) -> bool:
        rc: bool = super().isBackwardsCompatibleWith(other, vTree)
        if rc and isinstance(other, MapType):
            if not self.keyType.isBackwardsCompatibleWith(other.keyType, vTree):
                vTree.addProblem(f"Key type {self.keyType} is not backwards compatible with {other.keyType}")
                rc = False
            if not self.valueType.isBackwardsCompatibleWith(other.valueType, vTree):
                vTree.addProblem(f"Value type {self.valueType} is not backwards compatible with {other.valueType}")
                rc = False
        else:
            rc = False
        return rc

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.keyType}, {self.valueType})"

    def lint(self, vTree: ValidationTree):
        super().lint(vTree)
        self.keyType.lint(vTree)
        self.valueType.lint(vTree)


class StructType(DataType):
    """A struct is a collection of named fields"""
    def __init__(self, fields: OrderedDict[str, DataType]) -> None:
        super().__init__()
        self.fields: OrderedDict[str, DataType] = fields

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"fields": [{"name": name, "type": type.to_json()} for name, type in self.fields.items()]})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, StructType) and self.fields == other.fields

    def isBackwardsCompatibleWith(self, other: DataType, vTree: ValidationTree) -> bool:
        if not super().isBackwardsCompatibleWith(other, vTree):
            return False
        if not isinstance(other, StructType):
            return False

        # Check for removed or modified fields
        for key, value in other.fields.items():
            if key not in self.fields:
                vTree.addProblem(f"Field {key} has been removed")
                return False

        # Check for added or modified fields
        for key, value in self.fields.items():
            if key not in other.fields:
                vTree.addProblem(f"Field {key} has been added")
                return False
            if not value.isBackwardsCompatibleWith(other.fields[key], vTree):
                vTree.addProblem(f"Field {key} is not backwards compatible")
                return False

        return True

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.fields})"

    def lint(self, vTree: ValidationTree):
        super().lint(vTree)
        for value in self.fields.values():
            value.lint(vTree)


class TextDataType(BoundedDataType):
    def __init__(self, maxSize: Optional[int], collationString: Optional[str]) -> None:
        super().__init__(maxSize)
        self.collationString: Optional[str] = collationString
        """The collation and/or character encoding for this string. Unicode strings
        can have a collation but it is not required. Non unicode strings must have a collation"""

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        if self.collationString is not None:
            rc.update({"collationString": self.collationString})
        return rc

    def __str__(self) -> str:
        if (self.maxSize is None and self.collationString is None):
            return str(self.__class__.__name__) + "()"
        elif (self.maxSize is None and self.collationString is not None):
            return str(self.__class__.__name__) + f"(collationString='{self.collationString}')"
        elif (self.maxSize is not None and self.collationString is None):
            return str(self.__class__.__name__) + f"({self.maxSize})"
        else:
            return str(self.__class__.__name__) + f"({self.maxSize}, '{self.collationString}')"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, TextDataType) and self.collationString == other.collationString

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, TextDataType):
            otherTD: TextDataType = cast(TextDataType, other)
            if (self.collationString != otherTD.collationString):
                vTree.addProblem(f"Collation has changed from {otherTD.collationString} to {self.collationString}")
            super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()


class NumericDataType(DataType):
    """Base class for all numeric data types"""
    def __init__(self) -> None:
        super().__init__()

    def isBackwardsCompatibleWith(self, other: DataType, vTree: ValidationTree) -> bool:
        vTree.checkTypeMatches(other, NumericDataType)
        return not vTree.hasErrors()

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, NumericDataType)


class SignedOrNot(Enum):
    SIGNED = 0
    UNSIGNED = 1


class FixedIntegerDataType(NumericDataType):
    """This is a whole number with a fixed size in bits. This can be signed or unsigned."""
    def __init__(self, sizeInBits: int, isSigned: SignedOrNot) -> None:
        super().__init__()
        self.sizeInBits: int = sizeInBits
        """Number of bits including sign bit if present"""
        self.isSigned: SignedOrNot = isSigned
        """Is this a signed integer"""

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"sizeInBits": self.sizeInBits, "isSigned": self.isSigned.name})
        return rc

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)
        if (self.sizeInBits <= 0):
            vTree.addProblem("Size must be > 0")

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FixedIntegerDataType) and self.sizeInBits == other.sizeInBits and \
                self.isSigned == other.isSigned

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, FixedIntegerDataType):
            otherFSBDT: FixedIntegerDataType = cast(FixedIntegerDataType, other)
            if (not (self.sizeInBits == otherFSBDT.sizeInBits and self.isSigned == otherFSBDT.isSigned)):
                if (self.sizeInBits < otherFSBDT.sizeInBits):
                    vTree.addProblem(f"Size has been reduced from {otherFSBDT.sizeInBits} to {self.sizeInBits}")
                if (self.isSigned != otherFSBDT.isSigned):
                    vTree.addProblem(f"Signedness has changed from {otherFSBDT.isSigned} to {self.isSigned}")
                super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}: {self.sizeInBits} bits"


class TinyInt(FixedIntegerDataType):
    """8 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(8, SignedOrNot.SIGNED)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, TinyInt)

    def __hash__(self) -> int:
        return hash(str(self))


class SmallInt(FixedIntegerDataType):
    """16 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(16, SignedOrNot.SIGNED)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, SmallInt)

    def __hash__(self) -> int:
        return hash(str(self))


class Integer(FixedIntegerDataType):
    """32 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(32, SignedOrNot.SIGNED)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Integer)

    def __hash__(self) -> int:
        return hash(str(self))


class BigInt(FixedIntegerDataType):
    """64 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(64, SignedOrNot.SIGNED)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, BigInt)

    def __hash__(self) -> int:
        return hash(str(self))


class NonFiniteBehavior(Enum):
    """This is the behavior of non finite values in a floating point number"""
    IEEE754 = 0
    """A value is nonfinite if the exponent is all ones. In such cases, a value is Inf if the significand is zero and NaN otherwise."""
    NanOnly = 1
    """There is no representation for Inf. Values that should produce Inf produce NaN instead."""


class FloatNanEncoding(Enum):
    """This is the encoding of NaN values in a floating point number"""
    IEEE = 0
    """"""
    AllOnes = 1
    """All ones"""
    NegativeZero = 2
    """Negative zero"""


class CustomFloat(NumericDataType):
    """A custom floating point number. Inspired by LLVM's APFloat. This is a floating point number with a custom exponent range and precision."""
    def __init__(self, maxExponent: int, minExponent: int, precision: int, sizeInBits: int,
                 nonFiniteBehavior: NonFiniteBehavior = NonFiniteBehavior.IEEE754, nanEncoding: FloatNanEncoding = FloatNanEncoding.IEEE) -> None:
        """Creates a custom floating point number with the given exponent range and precision"""
        super().__init__()
        self.sizeInBits: int = sizeInBits
        self.maxExponent: int = maxExponent
        self.minExponent: int = minExponent
        self.precision: int = precision
        """Number of bits in the significand, this includes the sign bit"""
        self.nonFiniteBehavior: NonFiniteBehavior = nonFiniteBehavior
        self.nanEncoding: FloatNanEncoding = nanEncoding

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"maxExponent": self.maxExponent, "minExponent": self.minExponent, "precision": self.precision, "sizeInBits": self.sizeInBits,
                   "nonFiniteBehavior": self.nonFiniteBehavior.name, "nanEncoding": self.nanEncoding.name})
        return rc

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)
        if (self.sizeInBits <= 0):
            vTree.addProblem("Size must be > 0")
        if (self.maxExponent <= 0):
            vTree.addProblem("Max exponent must be > 0")
        if (self.minExponent >= 0):
            vTree.addProblem("Min exponent must be < 0")
        if (self.precision < 0):
            vTree.addProblem("Precision must be >= 0")
        if (self.maxExponent <= self.minExponent):
            vTree.addProblem("Max exponent must be > min exponent")
        if (self.precision > self.sizeInBits):
            vTree.addProblem("Precision must be <= size in bits")
        if (self.nonFiniteBehavior == NonFiniteBehavior.NanOnly and self.nanEncoding == FloatNanEncoding.NegativeZero):
            vTree.addProblem("Non finite behavior cannot be NanOnly and nan encoding cannot be NegativeZero")

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, CustomFloat) and self.maxExponent == other.maxExponent and \
            self.minExponent == other.minExponent and self.precision == other.precision and \
            self.nonFiniteBehavior == other.nonFiniteBehavior and self.nanEncoding == other.nanEncoding

    def isRepresentableBy(self, other: 'CustomFloat') -> bool:
        """Returns true if this float can be represented by the other float excluding non finite behaviors"""
        return self.maxExponent <= other.maxExponent and self.minExponent >= other.minExponent and self.precision <= other.precision

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, CustomFloat):
            otherCF: CustomFloat = cast(CustomFloat, other)

            # Can this object be stored without precision loss in otherCF
            if otherCF.isRepresentableBy(self):
                return super().isBackwardsCompatibleWith(other, vTree)
            vTree.addProblem("New Type loses precision on current type")
        return not vTree.hasErrors()

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.maxExponent}, {self.minExponent}, "
            f"{self.precision}, {self.sizeInBits}, {self.nonFiniteBehavior}, {self.nanEncoding})")


class SimpleCustomFloat(CustomFloat):
    """This is a CustomFloat but uses a simplified str method which is just the class name rather than the fully
    specified CustomFloat. It prevents all standard types needing to implement their own str method."""
    def __init__(self, maxExponent: int, minExponent: int, precision: int, sizeInBits: int,
                 nonFiniteBehavior: NonFiniteBehavior = NonFiniteBehavior.IEEE754, nanEncoding: FloatNanEncoding = FloatNanEncoding.IEEE) -> None:
        super().__init__(maxExponent, minExponent, precision, sizeInBits, nonFiniteBehavior, nanEncoding)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class IEEE16(SimpleCustomFloat):
    """Half precision IEEE754"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=16, precision=11, maxExponent=15, minExponent=-14)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, IEEE16)


class IEEE32(CustomFloat):
    """IEEE754 32 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=32, precision=24, maxExponent=127, minExponent=-126)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, IEEE32)


class Float(IEEE32):
    """Alias 32 bit IEEE floating point number"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Float)


class IEEE64(SimpleCustomFloat):
    """IEEE754 64 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=64, precision=53, maxExponent=1023, minExponent=-1022)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, IEEE64)


class Double(IEEE64):
    """Alias for IEEE64"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Double)


class IEEE128(SimpleCustomFloat):
    """IEEE754 128 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=128, precision=113, maxExponent=16383, minExponent=-16382)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, IEEE128)


class IEEE256(SimpleCustomFloat):
    """IEEE754 256 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=256, precision=237, maxExponent=262143, minExponent=-262142)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, IEEE256)


class FP8_E4M3(SimpleCustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=8, precision=3, maxExponent=15, minExponent=-14)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FP8_E4M3)


class FP6_E2M3(SimpleCustomFloat):
    """IEEE 6 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=6, precision=3, maxExponent=3, minExponent=-2)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FP6_E2M3)


class FP6_E3M2(SimpleCustomFloat):
    """IEEE 6 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=6, precision=2, maxExponent=7, minExponent=-6)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FP6_E3M2)


class FP4_E2M1(SimpleCustomFloat):
    """IEEE 4 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=4, precision=1, maxExponent=3, minExponent=-2)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FP4_E2M1)


class FP8_E5M2(SimpleCustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=8, precision=2, maxExponent=31, minExponent=-30)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FP8_E5M2)


class FP8_E5M2FNUZ(SimpleCustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=8, precision=3, maxExponent=15, minExponent=-15, nonFiniteBehavior=NonFiniteBehavior.NanOnly,
                         nanEncoding=FloatNanEncoding.NegativeZero)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FP8_E5M2FNUZ)


class FP8_E4M3FNUZ(SimpleCustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=8, precision=4, maxExponent=7, minExponent=-7, nonFiniteBehavior=NonFiniteBehavior.NanOnly,
                         nanEncoding=FloatNanEncoding.NegativeZero)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FP8_E4M3FNUZ)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class FP8_E8M0(SimpleCustomFloat):
    """Open Compute 8 bit exponent only number, this is typically used as a scaling factor in micro scale floating point types"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=8, precision=0, maxExponent=255, minExponent=-254)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FP8_E8M0)


class MicroScaling_CustomFloat(NumericDataType):
    """Represents an array of numbers of size batchSize where each element is scaled by a common factor
    with type scaleType. The element type is elementType. This is intended for machine learning applications which
    can use this type of floating point number specification"""
    def __init__(self, batchSize: int, scaleType: Type[CustomFloat], elementType: Type[CustomFloat]) -> None:
        super().__init__()
        self.batchSize: int = batchSize
        self.scaleType: Type[CustomFloat] = scaleType
        self.elementType: Type[CustomFloat] = elementType

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"batchSize": self.batchSize})
        rc.update({"scaleType": self.scaleType.__class__.__name__})
        rc.update({"elementType": self.elementType.__class__.__name__})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, MicroScaling_CustomFloat) and self.batchSize == other.batchSize and \
            self.scaleType == other.scaleType and self.elementType == other.elementType

    def isBackwardsCompatibleWith(self, other: DataType, vTree: ValidationTree) -> bool:
        """Must be same type and batchSize, scaleType and elementType must be backwards compatible with other"""
        if vTree.checkTypeMatches(other, self.__class__):
            otherMSCF: MicroScaling_CustomFloat = cast(MicroScaling_CustomFloat, other)
            if (self.batchSize < otherMSCF.batchSize):
                vTree.addProblem(f"Batch size has been reduced from {otherMSCF.batchSize} to {self.batchSize}")
            if (self.scaleType != otherMSCF.scaleType):
                vTree.addProblem(f"Scale type has changed from {otherMSCF.scaleType} to {self.scaleType}")
            if (self.elementType != otherMSCF.elementType):
                vTree.addProblem(f"Element type has changed from {otherMSCF.elementType} to {self.elementType}")
            return super().isBackwardsCompatibleWith(other, vTree)
        return super().isBackwardsCompatibleWith(other, vTree)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(batchSize={self.batchSize}, scaleType={self.scaleType}, elementType={self.elementType})"


class MXFP8_E4M3(MicroScaling_CustomFloat):
    """MicroScaling 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(batchSize=32, scaleType=FP8_E8M0, elementType=FP8_E4M3)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, MXFP8_E4M3)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class MXFP8_E5M2(MicroScaling_CustomFloat):
    """MicroScaling 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(batchSize=32, scaleType=FP8_E8M0, elementType=FP8_E5M2)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, MXFP8_E5M2)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class MXFP6_E2M3(MicroScaling_CustomFloat):
    """MicroScaling 6 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(batchSize=32, scaleType=FP8_E8M0, elementType=FP6_E2M3)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, MXFP6_E2M3)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class MXFP6_E3M2(MicroScaling_CustomFloat):
    """MicroScaling 6 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(batchSize=32, scaleType=FP8_E8M0, elementType=FP6_E3M2)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, MXFP6_E3M2)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class MXFP4_E2M1(MicroScaling_CustomFloat):
    """MicroScaling 4 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(batchSize=32, scaleType=FP8_E8M0, elementType=FP4_E2M1)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, MXFP4_E2M1)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class Decimal(BoundedDataType):
    """Signed Fixed decimal number with fixed size fraction"""
    def __init__(self, maxSize: int, precision: int) -> None:
        super().__init__(maxSize)
        self.precision: int = precision

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"precision": self.precision})
        return rc

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)
        if (self.precision < 0):
            vTree.addProblem("Precision must be >= 0")
        if (self.maxSize is None):
            vTree.addProblem("Decimal must have a maximum size")
        else:
            if (self.precision > self.maxSize):
                vTree.addProblem("Precision must be <= maxSize")

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Decimal) and self.precision == other.precision

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Decimal):
            otherD: Decimal = cast(Decimal, other)
            if (self.precision < otherD.precision):
                vTree.addProblem(f"Precision has been reduced from {otherD.precision} to {self.precision}")
            return super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()

    def __str__(self) -> str:
        if (self.maxSize is None):
            raise Exception("Decimal must have a maximum size")
        return str(self.__class__.__name__) + f"({self.maxSize},{self.precision})"


class TimeZone:
    """This specifies a timezone. The string can be either a supported timezone string or a custom timezone based around
    GMT. The string is in the format GMT+/-HH:MM or GMT+/-HH"""
    def __init__(self, timeZone: str) -> None:
        self.timeZone: str = timeZone

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "timeZone": self.timeZone}

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TimeZone) and self.timeZone == other.timeZone

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.timeZone})"


class TemporalDataType(DataType):
    """Base class for all temporal data types"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, TemporalDataType)

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)


class Timestamp(TemporalDataType):
    """Timestamp with microsecond precision, this includes a Date and a timestamp with microsecond precision"""
    def __init__(self, tz: TimeZone = TimeZone("UTC")) -> None:
        super().__init__()
        self.timeZone: TimeZone = tz

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"timeZone": self.timeZone.to_json()})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Timestamp) and self.timeZone == other.timeZone

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        vTree.checkTypeMatches(other, Timestamp, Date)
        if isinstance(other, Timestamp) and self.timeZone != other.timeZone:
            vTree.addProblem(f"Timezone has changed from {other.timeZone} to {self.timeZone}")
        return not vTree.hasErrors()


class Date(TemporalDataType):
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Date)

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        vTree.checkTypeMatches(other, Date)
        return not vTree.hasErrors()


class Interval(TemporalDataType):
    """This is a time interval defined as number of months, number of days and number of milliseconds. Each number is a 32 bit signed integer"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Interval)

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Interval):
            super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()


class UniCodeType(TextDataType):
    """Base class for unicode datatypes"""
    def __init__(self, maxSize: Optional[int], collationString: Optional[str]) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, UniCodeType)

    def __str__(self) -> str:
        if (self.maxSize is None and self.collationString is None):
            return str(self.__class__.__name__) + "()"
        elif (self.maxSize is None):
            return str(self.__class__.__name__) + f"(collationString='{self.collationString}')"
        elif (self.collationString is None):
            return str(self.__class__.__name__) + f"({self.maxSize})"
        else:
            return str(self.__class__.__name__) + f"({self.maxSize}, '{self.collationString}')"

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, UniCodeType):
            super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()


class NonUnicodeString(TextDataType):
    """Base class for non unicode datatypes with collation"""
    def __init__(self, maxSize: Optional[int], collationString: Optional[str]) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, NonUnicodeString)

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, NonUnicodeString):
            super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()


class VarChar(NonUnicodeString):
    """Variable length non unicode string with maximum size"""
    def __init__(self, maxSize: Optional[int] = None, collationString: Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, VarChar)


class NVarChar(UniCodeType):
    """Variable length unicode string with maximum size"""
    def __init__(self, maxSize: Optional[int] = None, collationString: Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, NVarChar)


class String(NVarChar):
    """Alias for NVarChar"""
    def __init__(self, maxSize: Optional[int] = None, collationString: Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, String)


def strForFixedSizeString(clsName: str, maxSize: int, collationString: Optional[str]) -> str:
    if (maxSize == 1 and collationString is None):
        return str(clsName) + "()"
    elif (collationString is None):
        return str(clsName) + f"({maxSize})"
    else:
        return str(clsName) + f"({maxSize}, '{collationString}')"


class Char(NonUnicodeString):
    """Non unicode fixed length character string"""
    def __init__(self, maxSize: int = 1, collationString: Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Char)

    def __str__(self) -> str:
        sz: int = 1 if self.maxSize is None else self.maxSize
        return strForFixedSizeString(self.__class__.__name__, sz, self.collationString)


class NChar(UniCodeType):
    """Unicode fixed length character string"""
    def __init__(self, maxSize: int = 1, collationString: Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, NChar)

    def __str__(self) -> str:
        sz: int = 1 if self.maxSize is None else self.maxSize
        return strForFixedSizeString(self.__class__.__name__, sz, self.collationString)


class Boolean(DataType):
    """Boolean value"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Boolean)

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Boolean):
            super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)


class Variant(BoundedDataType):
    """JSON type datatype"""
    def __init__(self, maxSize: Optional[int] = None) -> None:
        super().__init__(maxSize)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Variant)

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Variant):
            super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()


class Binary(BoundedDataType):
    """Binary blob"""
    def __init__(self, maxSize: Optional[int] = None) -> None:
        super().__init__(maxSize)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Binary)

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Binary):
            super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()


class Vector(ArrayType):
    """Fixed length vector of IEEE32's for machine learning"""
    def __init__(self, dimensions: int) -> None:
        super().__init__(dimensions, IEEE32())

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.maxSize})"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Vector)


class NullableStatus(Enum):
    """Specifies whether a column is nullable"""
    NOT_NULLABLE = 0
    """This column cannot store null values"""
    NULLABLE = 1
    """This column can store null values"""


class PrimaryKeyStatus(Enum):
    """Specifies whether a column is part of the primary key"""
    NOT_PK = 0
    """Not part of a primary key"""
    PK = 1
    """Part of the primary key"""


DEFAULT_primaryKey: PrimaryKeyStatus = PrimaryKeyStatus.NOT_PK

DEFAULT_nullable: NullableStatus = NullableStatus.NULLABLE


class DDLColumn(ANSI_SQL_NamedObject, Documentable, JSONable):
    """This is an individual attribute within a DDLTable schema"""
    def __init__(self, name: str, dataType: DataType, *args: Union[NullableStatus, DataClassification, PrimaryKeyStatus, Documentation]) -> None:
        super().__init__(name)
        Documentable.__init__(self, None)
        JSONable.__init__(self)
        self.type: DataType = dataType
        self.primaryKey: PrimaryKeyStatus = DEFAULT_primaryKey
        self.classification: Optional[list[DataClassification]] = None
        self.nullable: NullableStatus = DEFAULT_nullable
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "name": self.name,
            "type": self.type.to_json(),
            "nullable": self.nullable.value,
            "primaryKey": self.primaryKey.value,
            "classification": [c.to_json() for c in self.classification] if self.classification else None,
            "doc": self.documentation.to_json() if self.documentation else None,
        }

    def add(self, *args: Union[NullableStatus, DataClassification, PrimaryKeyStatus, Documentation]) -> None:
        for arg in args:
            if (isinstance(arg, NullableStatus)):
                self.nullable = arg
            elif (isinstance(arg, DataClassification)):
                if (self.classification is None):
                    self.classification = list()
                self.classification.append(arg)
            elif (isinstance(arg, PrimaryKeyStatus)):
                self.primaryKey = arg
            else:
                self.documentation = arg

    def __eq__(self, o: object) -> bool:
        if (type(o) is not DDLColumn):
            return False
        return super().__eq__(o) and self.type == o.type and self.primaryKey == o.primaryKey and self.nullable == o.nullable and \
            self.classification == o.classification

    def checkForBackwardsCompatibility(self, other: object, vTree: ValidationTree) -> bool:
        """Returns true if this column is backwards compatible with the other column"""
        # TODO Add support to changing the column data type to a compatible type
        super().checkForBackwardsCompatibility(other, vTree)
        if not isinstance(other, DDLColumn):
            vTree.addProblem(f"Cannot compare {self.__class__.__name__} with {other.__class__.__name__}")
            return False

        self.type.isBackwardsCompatibleWith(other.type, vTree)
        if (self.nullable != other.nullable):
            vTree.addProblem(f"Nullable status for {self.name} changed from {self.nullable} to {other.nullable}")
        if (self.classification != other.classification):
            vTree.addProblem(f"Data classification for {self.name} changed from {self.classification} to {other.classification}")
        return not vTree.hasErrors()

    def lint(self, tree: ValidationTree) -> None:
        super().nameLint(tree)
        self.type.lint(tree)
        if (self.documentation):
            self.documentation.lint(tree.addSubTree(self.documentation))
        if (self.primaryKey == PrimaryKeyStatus.PK and self.nullable == NullableStatus.NULLABLE):
            tree.addProblem(f"Primary key column {self.name} cannot be nullable")

    def __str__(self) -> str:
        return f"DDLColumn({self.name})"


class AttributeList(UserDSLObject, JSONable):
    """A list of column names."""
    def __init__(self, colNames: list[str]) -> None:
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.colNames: List[str] = []
        for col in colNames:
            self.colNames.append(col)

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "colNames": self.colNames}

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AttributeList) and self.colNames == other.colNames

    def lint(self, tree: ValidationTree) -> None:
        for col in self.colNames:
            if not is_valid_sql_identifier(col):
                tree.addProblem(f"Column name {col} is not a valid ANSI SQL identifier")

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.colNames})"


class PrimaryKeyList(AttributeList):
    """A list of columns to be used as the primary key"""
    def __init__(self, colNames: list[str]) -> None:
        super().__init__(colNames)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, PrimaryKeyList)


class PartitionKeyList(AttributeList):
    """A list of column names used for partitioning ingested data"""
    def __init__(self, colNames: list[str]) -> None:
        super().__init__(colNames)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, PartitionKeyList)


class NotBackwardsCompatible(ValidationProblem):
    """This is a validation problem that indicates that the schema is not backwards compatible"""
    def __init__(self, problem: str) -> None:
        super().__init__(problem, ProblemSeverity.ERROR)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, NotBackwardsCompatible)

    def __hash__(self) -> int:
        return hash(str(self))


class Schema(Documentable, UserDSLObject, JSONable):
    """This is a basic schema in the system. It has base meta attributes common for all schemas and core methods for all schemas"""
    def __init__(self) -> None:
        Documentable.__init__(self, None)
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.primaryKeyColumns: Optional[PrimaryKeyList] = None
        self.ingestionPartitionColumns: Optional[PartitionKeyList] = None
        """How should this dataset be partitioned for ingestion and storage"""

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"primaryKeyColumns": self.primaryKeyColumns.to_json() if self.primaryKeyColumns else None})
        rc.update({"ingestionPartitionColumns": self.ingestionPartitionColumns.to_json() if self.ingestionPartitionColumns else None})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, Schema)):
            return self.primaryKeyColumns == other.primaryKeyColumns and self.ingestionPartitionColumns == other.ingestionPartitionColumns and \
                self.documentation == other.documentation
        else:
            return False

    @abstractmethod
    def getHubSchema(self) -> 'Schema':
        """Returns the hub schema for this schema"""
        pass

    @abstractmethod
    def checkForBackwardsCompatibility(self, other: 'Schema', vTree: ValidationTree) -> bool:
        """Returns true if this schema is backward compatible with the other schema"""
        # Primary keys cannot change
        if (self.primaryKeyColumns != other.primaryKeyColumns):
            vTree.addRaw(NotBackwardsCompatible(f"Primary key columns cannot change from {self.primaryKeyColumns} to {other.primaryKeyColumns}"))
        # Partitioning cannot change
        if (self.ingestionPartitionColumns != other.ingestionPartitionColumns):
            vTree.addRaw(NotBackwardsCompatible(f"Partitioning cannot change from {self.ingestionPartitionColumns} to {other.ingestionPartitionColumns}"))
        if self.documentation:
            self.documentation.lint(vTree.addSubTree(self.documentation))
        return not vTree.hasErrors()

    @abstractmethod
    def checkClassificationsAreOnly(self, verifier: DataClassificationPolicy) -> bool:
        """Returns true if all columns in this schema have the specified classification"""
        pass

    @abstractmethod
    def hasDataClassifications(self) -> bool:
        """Returns True if the schema has any data classification specified"""
        pass

    @abstractmethod
    def lint(self, tree: ValidationTree) -> None:
        """This method performs linting on this schema"""
        if (self.primaryKeyColumns):
            self.primaryKeyColumns.lint(tree.addSubTree(self.primaryKeyColumns))

        if (self.ingestionPartitionColumns):
            self.ingestionPartitionColumns.lint(tree.addSubTree(self.ingestionPartitionColumns))


class DDLTable(Schema):
    """Table definition"""

    def __init__(self, *args: Union[DDLColumn, PrimaryKeyList, PartitionKeyList, Documentation]) -> None:
        super().__init__()
        self.columns: dict[str, DDLColumn] = OrderedDict[str, DDLColumn]()
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"columns": {k: v.to_json() for k, v in self.columns.items()}})
        return rc

    def hasDataClassifications(self) -> bool:
        for col in self.columns.values():
            if (col.classification):
                return True
        return False

    def checkClassificationsAreOnly(self, verifier: DataClassificationPolicy) -> bool:
        """Check all columns comply with the verifier"""
        for col in self.columns.values():
            if col.classification:
                for dc in col.classification:
                    if not verifier.isCompatible(dc):
                        return False
        return True

    def add(self, *args: Union[DDLColumn, PrimaryKeyList, PartitionKeyList, Documentation]):
        """Add a column or primary key list to the table"""
        for c in args:
            if (isinstance(c, DDLColumn)):
                if (self.columns.get(c.name) is not None):
                    raise Exception(f"Duplicate column {c.name}")
                self.columns[c.name] = c
            elif (isinstance(c, PrimaryKeyList)):
                self.primaryKeyColumns = c
            elif (isinstance(c, PartitionKeyList)):
                self.ingestionPartitionColumns = c
            else:
                self.documentation = c
        self.calculateKeys()

    def calculateKeys(self):
        """If a primarykey list is specified then set each column pk correspondingly otherwise construct a primary key list from the pk flag"""
        if (self.primaryKeyColumns):
            for col in self.columns.values():
                col.primaryKey = PrimaryKeyStatus.NOT_PK
            for col in self.primaryKeyColumns.colNames:
                self.columns[col].primaryKey = PrimaryKeyStatus.PK
        else:
            keyNameList: List[str] = []
            for col in self.columns.values():
                if (col.primaryKey == PrimaryKeyStatus.PK):
                    keyNameList.append(col.name)
            self.primaryKeyColumns = PrimaryKeyList(keyNameList)

    def getHubSchema(self) -> 'Schema':
        """Returns the hub schema for this schema"""
        return self

    def getColumnByName(self, name: str) -> Optional[DDLColumn]:
        """Returns a column by name"""
        col: DDLColumn = self.columns[name]
        return col

    def __eq__(self, o: object) -> bool:
        if (not super().__eq__(o)):
            return False
        if (type(o) is not DDLTable):
            return False
        return self.columns == o.columns and self.columns == o.columns

    def checkForBackwardsCompatibility(self, other: 'Schema', vTree: ValidationTree) -> bool:
        """Returns true if this schema is backward compatible with the other schema"""
        super().checkForBackwardsCompatibility(other, vTree)
        if vTree.checkTypeMatches(other, DDLTable):
            currentDDL: DDLTable = cast(DDLTable, other)
            # New tables must contain all old columns
            for col in currentDDL.columns.values():
                if (col.name not in self.columns):
                    vTree.addRaw(NotBackwardsCompatible(f"Column {col.name} is missing from the new schema"))
            # Existing columns must be compatible
            for col in self.columns.values():
                cTree: ValidationTree = vTree.addSubTree(col)
                newCol: Optional[DDLColumn] = currentDDL.columns.get(col.name)
                if (newCol):
                    newCol.checkForBackwardsCompatibility(col, cTree)

            # Now check additional columns
            newColumnNames: set[str] = set(self.columns.keys())
            currColNames: set[str] = set(currentDDL.columns.keys())
            additionalColumns: set[str] = newColumnNames.difference(currColNames)
            for colName in additionalColumns:
                col: DDLColumn = self.columns[colName]
                # Additional columns cannot be primary keys
                if (col.primaryKey == PrimaryKeyStatus.PK):
                    vTree.addRaw(NotBackwardsCompatible(f"Column {col.name} cannot be a new primary key column"))
                # Additional columns must be nullable
                if col.nullable == NullableStatus.NOT_NULLABLE:
                    vTree.addRaw(NotBackwardsCompatible(f"Column {col.name} must be nullable"))
        return not vTree.hasErrors()

    def lint(self, tree: ValidationTree) -> None:
        """This method performs linting on this schema"""
        super().lint(tree)
        if self.primaryKeyColumns:
            pkTree: ValidationTree = tree.addSubTree(self.primaryKeyColumns)
            self.primaryKeyColumns.lint(pkTree)
            for colName in self.primaryKeyColumns.colNames:
                if (colName not in self.columns):
                    pkTree.addRaw(NotBackwardsCompatible(f"Primary key column {colName} is not in the column list"))
                else:
                    col: DDLColumn = self.columns[colName]
                    if (col.primaryKey != PrimaryKeyStatus.PK):
                        tree.addRaw(NotBackwardsCompatible(f"Column {colName} should be marked primary key column"))
                    if (col.nullable == NullableStatus.NULLABLE):
                        tree.addRaw(NotBackwardsCompatible(f"Primary key column {colName} cannot be nullable"))
            for col in self.columns.values():
                colTree: ValidationTree = tree.addSubTree(col)
                col.lint(colTree)
                if col.primaryKey == PrimaryKeyStatus.PK and col.name not in self.primaryKeyColumns.colNames:
                    colTree.addRaw(NotBackwardsCompatible(f"Column {col.name} is marked as primary key but is not in the primary key list"))
        else:
            tree.addProblem("Table must have a primary key list")

        # If partitioning columns are specified then they must exist and be non nullable
        if self.ingestionPartitionColumns:
            for colName in self.ingestionPartitionColumns.colNames:
                if (colName not in self.columns):
                    tree.addRaw(NotBackwardsCompatible(f"Partitioning column {colName} is not in the column list"))
                else:
                    col: DDLColumn = self.columns[colName]
                    if (col.nullable == NullableStatus.NULLABLE):
                        tree.addRaw(NotBackwardsCompatible(f"Partitioning column {colName} cannot be nullable"))

    def __str__(self) -> str:
        return "DDLTable()"


class GenericKey(ABC):

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return "GenericKey()"


class EcosystemKey(GenericKey):
    """Soft link to an ecosystem"""
    def __init__(self, ecoName: str) -> None:
        self.ecoName: str = ecoName

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, EcosystemKey) and self.ecoName == __value.ecoName

    def __str__(self) -> str:
        return f"Ecosystem({self.ecoName})"

    def __hash__(self) -> int:
        return hash(str(self))


class GovernanceZoneKey(EcosystemKey):
    """Soft link to a governance zone"""
    def __init__(self, e: EcosystemKey, gz: str) -> None:
        super().__init__(e.ecoName)
        self.gzName: str = gz

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, GovernanceZoneKey) and self.gzName == __value.gzName

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return super().__str__() + f".GovernanceZone({self.gzName})"


class StoragePolicyKey(GovernanceZoneKey):
    """Soft link to a storage policy"""
    def __init__(self, gz: GovernanceZoneKey, policyName: str):
        super().__init__(gz, gz.gzName)
        self.policyName: str = policyName

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, StoragePolicyKey) and self.policyName == __value.policyName

    def __str__(self) -> str:
        return super().__str__() + f".StoragePolicy({self.policyName})"

    def __hash__(self) -> int:
        return hash(str(self))


class InfrastructureVendorKey(EcosystemKey):
    """Soft link to an infrastructure vendor"""
    def __init__(self, eco: EcosystemKey, iv: str) -> None:
        super().__init__(eco.ecoName)
        self.ivName: str = iv

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, InfrastructureVendorKey) and self.ivName == __value.ivName

    def __str__(self) -> str:
        return super().__str__() + f".InfrastructureVendor({self.ivName})"

    def __hash__(self) -> int:
        return hash(str(self))


class InfraLocationKey(InfrastructureVendorKey):
    """Soft link to an infrastructure location"""
    def __init__(self, iv: InfrastructureVendorKey, loc: list[str]) -> None:
        super().__init__(iv, iv.ivName)
        self.locationPath: list[str] = loc

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, InfraLocationKey) and self.locationPath == __value.locationPath

    def __str__(self) -> str:
        return super().__str__() + f".InfraLocation({self.locationPath})"

    def __hash__(self) -> int:
        return hash(str(self))


class TeamDeclarationKey(GovernanceZoneKey):
    """Soft link to a team declaration"""
    def __init__(self, gz: GovernanceZoneKey, td: str) -> None:
        super().__init__(gz, gz.gzName)
        self.tdName: str = td

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TeamDeclarationKey) and self.tdName == __value.tdName

    def __str__(self) -> str:
        return super().__str__() + f".TeamDeclaration({self.tdName})"


class WorkspaceKey(TeamDeclarationKey):
    def __init__(self, tdKey: TeamDeclarationKey, name: str) -> None:
        super().__init__(tdKey, tdKey.tdName)
        self.name: str = name

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, WorkspaceKey) and \
            self.name == __value.name

    def __str__(self) -> str:
        return super().__str__() + f".WorkspaceKey({self.name})"


class DatastoreKey(TeamDeclarationKey):
    """Soft link to a datastore"""
    def __init__(self, td: TeamDeclarationKey, ds: str) -> None:
        super().__init__(td, td.tdName)
        self.dsName: str = ds

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, DatastoreKey) and self.dsName == __value.dsName

    def __str__(self) -> str:
        return super().__str__() + f".Datastore({self.dsName})"


class PolicyMandatedRule(Enum):
    MANDATED_WITHIN_ZONE = 0
    """Policies with this are forcibly added to every dataset in the zone"""
    INDIVIDUALLY_MANDATED = 1
    """Policies with this are not added to datasets by default. They must be added individually to each dataset"""


class StoragePolicy(Policy['DataContainer']):
    '''This is the base class for storage policies. These are owned by a governance zone and are used to determine whether a container is
    compatible with the policy.'''

    def __init__(self, name: str, isMandatory: PolicyMandatedRule, doc: Optional[Documentation], deprecationStatus: DeprecationInfo) -> None:
        super().__init__(name, doc)
        self.mandatory: PolicyMandatedRule = isMandatory
        self.key: Optional[StoragePolicyKey] = None
        self.deprecationStatus: DeprecationInfo = deprecationStatus
        """If true then all data containers MUST comply with this policy regardless of whether a dataset specifies this policy or not"""

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, StoragePolicy) and self.name == other.name and self.mandatory == other.mandatory and \
            self.key == other.key and self.deprecationStatus == other.deprecationStatus

    def setGovernanceZone(self, gz: 'GovernanceZone') -> None:
        if gz.key is None:
            raise Exception("GovernanceZone key not set")
        self.key = StoragePolicyKey(gz.key, self.name)

    def isCompatible(self, obj: 'DataContainer') -> bool:
        '''This returns true if the container is compatible with the policy. This is used to determine whether data tagged with a policy can be
        stored in a specific container.'''
        return False

class StoragePolicyAllowAnyContainer(StoragePolicy):
    '''This is a storage policy that allows any container to be used.'''
    def __init__(self, name: str, isMandatory: PolicyMandatedRule, doc: Optional[Documentation] = None,
                 deprecationStatus: DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)) -> None:
        super().__init__(name, isMandatory, doc, deprecationStatus)

    def isCompatible(self, obj: 'DataContainer') -> bool:
        return True

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is StoragePolicyAllowAnyContainer and \
            self.name == other.name and self.mandatory == other.mandatory


class InfrastructureLocation(Documentable, UserDSLObject, JSONable):
    """This is a location within a vendors physical location hierarchy. This object
    is only fully initialized after construction when either the setParentLocation or
    setVendor methods are called. This is because the vendor is required to set the parent"""

    def __init__(self, name: str, *args: Union[Documentation, 'InfrastructureLocation']) -> None:
        Documentable.__init__(self, None)
        UserDSLObject.__init__(self)
        JSONable.__init__(self)

        self.name: str = name
        self.key: Optional[InfraLocationKey] = None

        self.locations: dict[str, 'InfrastructureLocation'] = OrderedDict()
        """These are the 'child' locations under this location. A state location would have city children for example"""
        """This specifies the parent location of this location. State is parent on city and so on"""
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "locations": {k: k.to_json() for k in self.locations.values()},
        }

    def __str__(self) -> str:
        return f"InfrastructureLocation({self.name})"

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, tree: ValidationTree):
        """This checks if the vendor is valid for the specified ecosystem, governance zone and team"""
        if (self.key is None):
            tree.addRaw(AttributeNotSet("Location"))
        if (self.documentation):
            dTree: ValidationTree = tree.addSubTree(self.documentation)
            self.documentation.lint(dTree)

        for loc in self.locations.values():
            loc.lint(tree)

    def setParentLocation(self, parent: InfraLocationKey) -> None:
        locList: list[str] = list(parent.locationPath)
        locList.append(self.name)
        self.key = InfraLocationKey(parent, locList)
        self.add()

    def add(self, *args: Union[Documentation, 'InfrastructureLocation']) -> None:
        for loc in args:
            if (isinstance(loc, InfrastructureLocation)):
                self.addLocation(loc)
            else:
                self.documentation = loc
        if (self.key):
            for loc in self.locations.values():
                loc.setParentLocation(self.key)

    def addLocation(self, loc: 'InfrastructureLocation'):
        if self.locations.get(loc.name) is not None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc

    def __eq__(self, other: object) -> bool:
        if super().__eq__(other) and isinstance(other, InfrastructureLocation):
            return self.name == other.name and self.key == other.key and self.locations == other.locations
        return False

    def getEveryChildLocation(self) -> set['InfrastructureLocation']:
        """This returns every child location of this location"""
        rc: set[InfrastructureLocation] = set()
        for loc in self.locations.values():
            rc.add(loc)
            rc = rc.union(loc.getEveryChildLocation())
        return rc

    def containsLocation(self, child: 'InfrastructureLocation') -> bool:
        """This true if this or a child matches the passed location"""
        if (self == child):
            return True
        for loc in self.locations.values():
            if loc.containsLocation(child):
                return True
        return False

    def getLocationOrThrow(self, locationName: str) -> 'InfrastructureLocation':
        """Returns the location with the specified name or throws an exception"""
        loc: Optional[InfrastructureLocation] = self.locations.get(locationName)
        assert loc is not None
        return loc

    def getLocation(self, locationName: str) -> Optional['InfrastructureLocation']:
        """Returns the location with the specified name or None"""
        return self.locations.get(locationName)

    def findLocationUsingKey(self, locationPath: list[str]) -> Optional['InfrastructureLocation']:
        """Returns the location using the path"""
        if (len(locationPath) == 0):
            return None
        else:
            locName: str = locationPath[0]
            loc: Optional[InfrastructureLocation] = self.locations.get(locName)
            if (loc):
                if (len(locationPath) == 1):
                    return loc
                else:
                    return loc.findLocationUsingKey(locationPath[1:])
            else:
                return None


class CloudVendor(Enum):
    """Cloud vendor. This is used with InfrastructureVendor types to associate them with a hard cloud vendor"""
    AWS = 0
    """Amazon Web Services"""
    AZURE = 1
    """Microsoft Azure"""
    GCP = 2
    """Google Cloud Platform"""
    IBM = 3
    """IBM Cloud"""
    ORACLE = 4
    """Oracle Cloud"""
    ALIBABA = 5
    """Alibaba Cloud"""
    AWS_CHINA = 6
    """AWS China"""
    TEN_CENT = 7
    HUAWEI = 8
    AZURE_CHINA = 9  # 21Vianet
    PRIVATE = 10  # Onsite or private cloud


class InfrastructureVendor(Documentable, UserDSLObject, JSONable):
    """This is a vendor which supplies infrastructure for storage and compute. It could be an internal supplier within an
    enterprise or an external cloud provider"""
    def __init__(self, name: str, *args: Union[InfrastructureLocation, Documentation, CloudVendor]) -> None:
        Documentable.__init__(self, None)
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.name: str = name
        self.key: Optional[InfrastructureVendorKey] = None
        self.locations: dict[str, 'InfrastructureLocation'] = OrderedDict()
        self.hardCloudVendor: Optional[CloudVendor] = None
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "locations": {k: k.to_json() for k in self.locations.values()},
            "hardCloudVendor": self.hardCloudVendor.name if self.hardCloudVendor else None,
        }

    def __hash__(self) -> int:
        return hash(self.name)

    def setEcosystem(self, eco: 'Ecosystem') -> None:
        self.key = InfrastructureVendorKey(eco.key, self.name)

        self.add()

    def add(self, *args: Union['InfrastructureLocation', Documentation, CloudVendor]) -> None:
        for loc in args:
            if (isinstance(loc, InfrastructureLocation)):
                self.addLocation(loc)
            elif (isinstance(loc, CloudVendor)):
                self.hardCloudVendor = loc
            else:
                self.documentation = loc
        if (self.key):
            topLocationKey: InfraLocationKey = InfraLocationKey(self.key, [])
            for loc in self.locations.values():
                loc.setParentLocation(topLocationKey)

    def addLocation(self, loc: 'InfrastructureLocation'):
        if self.locations.get(loc.name) is not None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc

    def __eq__(self, other: object) -> bool:
        if super().__eq__(other) and isinstance(other, InfrastructureVendor):
            return self.name == other.name and self.key == other.key and self.locations == other.locations and \
                self.hardCloudVendor == other.hardCloudVendor
        else:
            return False

    def getLocationOrThrow(self, locationName: str) -> 'InfrastructureLocation':
        """Returns the location with the specified name or throws an exception"""
        loc: Optional[InfrastructureLocation] = self.locations.get(locationName)
        assert loc is not None
        return loc

    def getLocation(self, locationName: str) -> Optional['InfrastructureLocation']:
        """Returns the location with the specified name or None"""
        return self.locations.get(locationName)

    def findLocationUsingKey(self, locationPath: list[str]) -> Optional[InfrastructureLocation]:
        """Returns the location using the path"""
        if (len(locationPath) == 0):
            return None
        else:
            locName: str = locationPath[0]
            loc: Optional[InfrastructureLocation] = self.locations.get(locName)
            if (loc):
                if (len(locationPath) == 1):
                    return loc
                else:
                    return loc.findLocationUsingKey(locationPath[1:])
            else:
                return None

    def lint(self, tree: ValidationTree):
        """This checks if the vendor is valid for the specified ecosystem, governance zone and team"""
        if (self.key is None):
            tree.addRaw(AttributeNotSet("Vendor"))
        if (self.documentation is None):
            tree.addRaw(AttributeNotSet("Documentation"))
        else:
            self.documentation.lint(tree)

        for loc in self.locations.values():
            lTree: ValidationTree = tree.addSubTree(loc)
            loc.lint(lTree)

    def __str__(self) -> str:
        return f"InfrastructureVendor({self.name}, {self.hardCloudVendor})"


def convertCloudVendorItems(items: Optional[set[CloudVendor]]) -> Optional[set[Literal[CloudVendor]]]:
    if items is None:
        return None
    else:
        return {Literal(item) for item in items}


class InfraHardVendorPolicy(AllowDisallowPolicy[Literal[CloudVendor]]):
    """Allows a GZ to police which vendors can be used with datastore or workspaces within itself"""
    def __init__(self, name: str, doc: Documentation, allowed: Optional[set[CloudVendor]] = None,
                 notAllowed: Optional[set[CloudVendor]] = None):
        super().__init__(name, doc, convertCloudVendorItems(allowed), convertCloudVendorItems(notAllowed))

    def __str__(self):
        return f"InfraStructureVendorPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        return super().__eq__(v) and isinstance(v, InfraStructureVendorPolicy) and self.allowed == v.allowed and self.notAllowed == v.notAllowed

    def __hash__(self) -> int:
        return super().__hash__()


class DataPlatformPolicy(AllowDisallowPolicy['DataPlatformKey']):
    def __init__(self, name: str, doc: Optional[Documentation], allowed: Optional[set['DataPlatformKey']] = None,
                 notAllowed: Optional[set['DataPlatformKey']] = None):
        super().__init__(name, doc, allowed, notAllowed)

    def __str__(self):
        return f"DataPlatformPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        return super().__eq__(v) and isinstance(v, DataPlatformPolicy) and self.allowed == v.allowed and \
            self.notAllowed == v.notAllowed and self.name == v.name

    def __hash__(self) -> int:
        return super().__hash__()


class EncryptionSystem(JSONable):
    """This describes"""
    def __init__(self) -> None:
        JSONable.__init__(self)
        self.name: Optional[str] = None
        self.keyContainer: Optional['DataContainer'] = None
        """Are keys stored on site or at a third party?"""
        self.hasThirdPartySuperUser: bool = False

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {}
        rc.update({"name": self.name})
        rc.update({"keyContainer": self.keyContainer.to_json() if self.keyContainer else None})
        rc.update({"hasThirdPartySuperUser": self.hasThirdPartySuperUser})
        return rc


class SchemaProjector(ABC):
    """This class takes a Schema and projects it to a Schema compatible with an underlying DataContainer"""
    def __init__(self, dataset: 'Dataset'):
        self.dataset: Dataset = dataset

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, SchemaProjector) and self.dataset == __value.dataset

    @abstractmethod
    def computeSchema(self) -> Optional[Schema]:
        pass


class DefaultSchemaProjector(SchemaProjector):
    """This is a default schema projector which projects the dataset schema to the original schema of the dataset. This is used when
    the data container doesn't have a specific schema projector. However, its likely that most data containers will require a specific
    projector to be written"""
    def __init__(self, dataset: 'Dataset'):
        super().__init__(dataset)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, DefaultSchemaProjector)

    def computeSchema(self) -> Optional[Schema]:
        """This returns the original schema for this implementation"""
        return self.dataset.originalSchema


class CaseSensitiveEnum(Enum):
    CASE_SENSITIVE = 0
    """This is a case sensitive enum"""
    CASE_INSENSITIVE = 1


class DataContainerNamingMapper:
    """This is an interface for mapping dataset names and attributes to the underlying data container. This is used to map
    the name of model elements to concrete data container names which may have different standards for naming. Given, consumers
    should be able to use any producer data, this name mapping must succeed. This may require character substitution or even
    truncation of names possibly with an additional hash on the end of the name to ensure uniqueness"""
    def __init__(self, maxLen: int = 255, caseSensitive: CaseSensitiveEnum = CaseSensitiveEnum.CASE_SENSITIVE, allowQuotes: Optional[str] = None) -> None:
        self.maxLen = maxLen
        self.caseSensitive = caseSensitive
        self.allowQuotes = allowQuotes

    def formatIdentifier(self, s: str) -> str:
        if self.caseSensitive == CaseSensitiveEnum.CASE_INSENSITIVE:
            s = s.upper()
        if self.allowQuotes is not None:
            s = f'{self.allowQuotes}{s}{self.allowQuotes}'
        return s

    def truncateIdentifier(self, s: str, maxLen: int) -> str:
        """This truncates the string to the maximum length. This truncation will add a 3 digit hex hash
        to the end of the string. This, the string may be truncated to maxLen - 4 and then the hash is added
        with an underscore seperator."""

        if len(s) > maxLen:
            truncated = s[:maxLen - 4]
            hash: str = hashlib.sha1(s.encode()).hexdigest()[:3]  # Get the first 3 characters of the hash
            return f"{truncated}_{hash}"
        else:
            return s

    @abstractmethod
    def mapRawDatasetName(self, dp: 'DataPlatform', w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        """This maps the data set name to a physical table which may be then shared by views for each Workspace using
        that dataset for a data platform. This name should not be exposed for use by consumers. They should use the view
        instead."""
        return self.formatIdentifier(f"{dp.name}_{w.name}_{dsg.name}_{store.name}_{ds.name}")

    @abstractmethod
    def mapRawDatasetView(self, dp: 'DataPlatform', w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        """This names the workspace view name for a dataset used in a DSG. This is the actual name used by
        consumers, the view, not the underlying table holding the data"""
        return self.formatIdentifier(f"{dp.name}_{w.name}_{dsg.name}_{store.name}_{ds.name}")

    @abstractmethod
    def mapAttributeName(self, w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset', attributeName: str) -> str:
        """This maps the model attribute name in a schema to the physical attribute/column name allowed by a data container"""
        return self.formatIdentifier(attributeName)


class DefaultDataContainerNamingMapper(DataContainerNamingMapper):
    """This is a default naming adapter which maps the dataset name to the dataset name and the attribute name to the attribute name"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, DefaultDataContainerNamingMapper)

    def mapRawDatasetName(self, dp: 'DataPlatform', w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        """The table which data is materialized in. This is the raw table name containing data"""
        return super().mapRawDatasetName(dp, w, dsg, store, ds)

    def mapRawDatasetView(self, dp: 'DataPlatform', w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        """This is the view name which consumers should use to access the data."""
        return super().mapRawDatasetView(dp, w, dsg, store, ds)

    def mapAttributeName(self, w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset', attributeName: str) -> str:
        return super().mapAttributeName(w, dsg, store, ds, attributeName)


class DataContainer(Documentable, UserDSLObject):
    """This is a container for data. It's a logical container. The data can be physically stored in
    one or more locations through replication or fault tolerance measures. It is owned by a data platform
    and is used to determine whether a dataset is compatible with the container by a governancezone."""
    def __init__(self, name: str, *args: Union[set['LocationKey'], Documentation]) -> None:
        UserDSLObject.__init__(self)
        Documentable.__init__(self, None)
        self.locations: set[LocationKey] = set()
        self.name: str = name
        self.serverSideEncryptionKeys: Optional[EncryptionSystem] = None
        """This is the vendor ecnryption system providing the container. For example, if a cloud vendor
        hosts the container, do they have access to the container data?"""
        self.clientSideEncryptionKeys: Optional[EncryptionSystem] = None
        """This is the encryption system used by the client to encrypt data before sending to the container. This could be used
        to encrypt data before sending to a cloud vendor for example"""
        self.isReadOnly: bool = False
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {}
        rc.update({"_type": self.__class__.__name__, "name": self.name, "locations": [loc.to_json() for loc in self.locations]})
        rc.update({"serverSideEncryptionKeys": self.serverSideEncryptionKeys.to_json() if self.serverSideEncryptionKeys else None})
        rc.update({"clientSideEncryptionKeys": self.clientSideEncryptionKeys.to_json() if self.clientSideEncryptionKeys else None})
        rc.update({"isReadOnly": self.isReadOnly})
        if (self.documentation):
            rc.update({"documentation": self.documentation.to_json()})
        return rc

    def add(self, *args: Union[set['LocationKey'], Documentation]) -> None:
        for arg in args:
            if (isinstance(arg, set)):
                for loc in arg:
                    if (loc in self.locations):
                        raise Exception(f"Duplicate Location {loc}")
                    self.locations.add(loc)
            else:
                self.documentation = arg

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DataContainer):
            return self.name == other.name and self.locations == other.locations and \
                self.serverSideEncryptionKeys == other.serverSideEncryptionKeys and \
                self.clientSideEncryptionKeys == other.clientSideEncryptionKeys and \
                self.isReadOnly == other.isReadOnly
        else:
            return False

    def getName(self) -> str:
        """Returns the name of the container"""
        return self.name

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if (self.documentation):
            dTree: ValidationTree = tree.addSubTree(self.documentation)
            self.documentation.lint(dTree)

        for loc in self.locations:
            loc.lint(eco, tree.addSubTree(loc))

    def __hash__(self) -> int:
        return hash(self.name)

    def areLocationsOwnedByTheseVendors(self, eco: 'Ecosystem', vendors: set[CloudVendor]) -> bool:
        """Returns true if the container only uses locations managed by the provided set of cloud vendors"""
        for lkey in self.locations:
            loc: Optional[InfrastructureLocation] = lkey.getAsInfraLocation(eco)
            if (loc is None or loc.key is None):
                return False
            v: InfrastructureVendor = eco.getVendorOrThrow(loc.key.ivName)
            if v.hardCloudVendor not in vendors:
                return False
        return True

    def areAllLocationsInLocations(self, locations: set['LocationKey']) -> bool:
        """Returns true if all locations are in the provided set of locations"""
        for lkey in self.locations:
            if lkey not in locations:
                return False
        return True

    @abstractmethod
    def projectDatasetSchema(self, dataset: 'Dataset') -> Optional[SchemaProjector]:
        """This returns a schema projector which can be used to project the dataset schema to a schema compatible with the container"""
        return DefaultSchemaProjector(dataset)

    @abstractmethod
    def getNamingAdapter(self) -> Optional[DataContainerNamingMapper]:
        """This returns a naming adapter which can be used to map dataset names and attributes to the underlying data container"""
        return None


class SQLDatabase(DataContainer):
    """A generic SQL Database data container"""
    def __init__(self, name: str, locations: set['LocationKey'], databaseName: str) -> None:
        super().__init__(name, locations)
        self.databaseName: str = databaseName

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, SQLDatabase)):
            return super().__eq__(other) and self.databaseName == other.databaseName
        return False

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)

    def projectDatasetSchema(self, dataset: 'Dataset') -> Optional[SchemaProjector]:
        return super().projectDatasetSchema(dataset)

    def getNamingAdapter(self) -> Optional[DataContainerNamingMapper]:
        return DefaultDataContainerNamingMapper()


class URLSQLDatabase(SQLDatabase):
    """This is a SQL database with a URL"""
    def __init__(self, name: str, locations: set['LocationKey'], url: str, databaseName: str) -> None:
        super().__init__(name, locations, databaseName)
        self.url: str = url

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, URLSQLDatabase)):
            return super().__eq__(other) and self.url == other.url
        return False

    def __hash__(self) -> int:
        return hash(self.name)


class HostPortPair(UserDSLObject):
    """This represents a host and port pair"""
    def __init__(self, hostName: str, port: int) -> None:
        self.hostName: str = hostName
        self.port: int = port

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, HostPortPair)):
            return self.hostName == other.hostName and self.port == other.port
        return False

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return f"{self.hostName}:{self.port}"

    def lint(self, tree: ValidationTree) -> None:
        if not is_valid_hostname_or_ip(self.hostName):
            tree.addRaw(NameHasBadSynthax(f"Host '{self.hostName}' is not a valid hostname or IP address"))
        if self.port < 0 or self.port > 65535:
            tree.addProblem(f"Port {self.port} is not a valid port number")


class HostPortPairList(UserDSLObject):
    """This is a list of host port pairs"""
    def __init__(self, pairs: list[HostPortPair]) -> None:
        self.pairs: list[HostPortPair] = pairs

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, HostPortPairList)):
            return self.pairs == other.pairs
        return False

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return ", ".join([str(p) for p in self.pairs])

    def lint(self, tree: ValidationTree) -> None:
        for pair in self.pairs:
            pair.lint(tree.addSubTree(pair))


class HostPortSQLDatabase(SQLDatabase):
    """This is a SQL database with a host and port"""
    def __init__(self, name: str, locations: set['LocationKey'], hostPort: HostPortPair, databaseName: str) -> None:
        super().__init__(name, locations, databaseName)
        self.hostPortPair: HostPortPair = hostPort

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, HostPortSQLDatabase)):
            return super().__eq__(other) and self.hostPortPair == other.hostPortPair
        return False

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        self.hostPortPair.lint(tree.addSubTree(self.hostPortPair))


class PostgresDatabase(SQLDatabase):
    """This is a Postgres database"""
    def __init__(self, name: str, connection: HostPortPair, locations: set['LocationKey'], databaseName: str) -> None:
        super().__init__(name, locations, databaseName)
        self.connection: HostPortPair = connection

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, PostgresDatabase)):
            return super().__eq__(other) and self.connection == other.connection
        return False

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        self.connection.lint(tree.addSubTree(self.connection))


class ObjectStorage(DataContainer):
    """Generic Object storage service. Flat file storage"""
    def __init__(self, name: str, locs: set['LocationKey'], endPointURI: Optional[str], bucketName: str, prefix: Optional[str]):
        super().__init__(name, locs)
        self.endPointURI: Optional[str] = endPointURI
        self.bucketName: str = bucketName
        self.prefix: Optional[str] = prefix

    def projectDatasetSchema(self, dataset: 'Dataset') -> Optional[SchemaProjector]:
        return super().projectDatasetSchema(dataset)

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, ObjectStorage)):
            return super().__eq__(other) and self.endPointURI == other.endPointURI and self.bucketName == other.bucketName and self.prefix == other.prefix
        return False


class Dataset(ANSI_SQL_NamedObject, Documentable, JSONable):
    """This is a single collection of homogeneous records with a primary key"""
    def __init__(self, name: str, *args: Union[Schema, StoragePolicy, Documentation, DeprecationInfo, DataClassification]) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        JSONable.__init__(self)
        self.originalSchema: Optional[Schema] = None
        # Explicit policies, note these need to be added to mandatory policies for the owning GZ
        self.policies: dict[str, StoragePolicy] = OrderedDict()
        self.dataClassificationOverride: Optional[list[DataClassification]] = None
        """This is the classification of the data in the dataset. The overrides any classifications on the schema"""
        self.deprecationStatus: DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)
        self.add(*args)

    def add(self, *args: Union[Schema, StoragePolicy, Documentation, DeprecationInfo, DataClassification]) -> None:
        for arg in args:
            if (isinstance(arg, Schema)):
                s: Schema = arg
                self.originalSchema = s
            elif (isinstance(arg, StoragePolicy)):
                p: StoragePolicy = arg
                if self.policies.get(p.name) is not None:
                    raise Exception(f"Duplicate policy {p.name}")
                self.policies[p.name] = p
            elif (isinstance(arg, DeprecationInfo)):
                self.deprecationStatus = arg
            elif (isinstance(arg, DataClassification)):
                if (self.dataClassificationOverride is None):
                    self.dataClassificationOverride = list()
                self.dataClassificationOverride.append(arg)
            else:
                d: Documentation = arg
                self.documentation = d

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Dataset):
            return ANSI_SQL_NamedObject.__eq__(self, other) and Documentable.__eq__(self, other) and \
                self.name == other.name and self.originalSchema == other.originalSchema and \
                self.policies == other.policies and \
                self.deprecationStatus == other.deprecationStatus and self.dataClassificationOverride == other.dataClassificationOverride
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', store: 'Datastore', tree: ValidationTree) -> None:
        """Place holder to validate constraints on the dataset"""
        self.nameLint(tree)
        if (self.dataClassificationOverride is not None):
            if (self.originalSchema and self.originalSchema.hasDataClassifications()):
                tree.addProblem("There are data classifications within the schema")
        else:
            if (self.originalSchema and not self.originalSchema.hasDataClassifications()):
                tree.addProblem("There are no data classifications for the dataset", ProblemSeverity.WARNING)
        for policy in self.policies.values():
            if (policy.key is None):
                tree.addRaw(AttributeNotSet(f"Storage policy {policy.name} is not associated with a governance zone"))
            else:
                if (policy.key.gzName != gz.name):
                    tree.addProblem("Datasets must be governed by storage policies from its managing zone")
                if (policy.deprecationStatus.status == DeprecationStatus.DEPRECATED):
                    if (store.isDatasetDeprecated(self)):
                        tree.addRaw(ObjectIsDeprecated(policy, ProblemSeverity.WARNING))
                    else:
                        tree.addRaw(ObjectIsDeprecated(policy, ProblemSeverity.ERROR))
        if (self.originalSchema):
            self.originalSchema.lint(tree)
        else:
            tree.addRaw(AttributeNotSet("originalSchema"))

    def checkClassificationsAreOnly(self, verifier: DataClassificationPolicy) -> bool:
        """This checks if the dataset only has the specified classifications"""

        # Dataset level classification overrides schema level classification
        if (self.dataClassificationOverride):
            for dc in self.dataClassificationOverride:
                if not verifier.isCompatible(dc):
                    return False
            return True
        else:
            if self.originalSchema:
                # check schema attribute classifications are good
                return self.originalSchema.checkClassificationsAreOnly(verifier)
            else:
                return True

    def checkForBackwardsCompatibility(self, other: object, vTree: ValidationTree) -> bool:
        """This checks if the dataset is backwards compatible with the other dataset. This means that the other dataset
        can be used in place of this dataset. This is used to check if a dataset can be replaced by another dataset
        when a new version is released"""
        if (not isinstance(other, Dataset)):
            vTree.addRaw(ObjectWrongType(other, Dataset, ProblemSeverity.ERROR))
            return False
        super().checkForBackwardsCompatibility(other, vTree)
        if (self.originalSchema is None):
            vTree.addRaw(AttributeNotSet(f"Original schema not set for {self.name}"))
        elif (other.originalSchema is None):
            vTree.addRaw(AttributeNotSet(f"Original schema not set for {other.name}"))
        else:
            self.originalSchema.checkForBackwardsCompatibility(other.originalSchema, vTree)
        return not vTree.hasErrors()

    def __str__(self) -> str:
        return f"Dataset({self.name})"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"name": self.name})
        rc.update({"originalSchema": self.originalSchema.to_json() if self.originalSchema else None})
        rc.update({"policies": {k: v.to_json() for k, v in self.policies.items()}})
        return rc

    def hasClassifications(self) -> bool:
        """This returns true if the dataset has classifications for everything"""
        if (self.dataClassificationOverride):
            return True
        if (self.originalSchema and self.originalSchema.hasDataClassifications()):
            return True
        return False


class PyOdbcSourceInfo(SQLDatabase):
    """This describes how to connect to a database using pyodbc"""
    def __init__(self, name: str, locs: set['LocationKey'], serverHost: str, databaseName: str, driver: str, connectionStringTemplate: str) -> None:
        super().__init__(name, locs, databaseName)
        self.serverHost: str = serverHost
        self.driver: str = driver
        self.connectionStringTemplate: str = connectionStringTemplate

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is PyOdbcSourceInfo and self.serverHost == other.serverHost and \
            self.databaseName == other.databaseName and self.driver == other.driver and self.connectionStringTemplate == other.connectionStringTemplate

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        super().lint(eco, tree)
# TODO validate the server string, its not just a host name
#        if (not is_valid_hostname_or_ip(self.serverHost)):
#            tree.addProblem(f"Server host {self.serverHost} is not a valid hostname or IP address")

    def __str__(self) -> str:
        return f"PyOdbcSourceInfo({self.serverHost})"

    def projectDatasetSchema(self, dataset: 'Dataset') -> Optional[SchemaProjector]:
        return super().projectDatasetSchema(dataset)


class CaptureType(Enum):
    SNAPSHOT = 0
    INCREMENTAL = 1


class IngestionConsistencyType(Enum):
    """This determines whether data is ingested in consistent groups across multiple datasets or
    whether each dataset is ingested independently"""
    SINGLE_DATASET = 0
    MULTI_DATASET = 1


class StepTrigger(JSONable):
    """A step such as ingestion is driven in pulses triggered by these."""
    def __init__(self, name: str):
        super().__init__()
        self.name: str = name

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "name": self.name}

    def __eq__(self, o: object) -> bool:
        return isinstance(o, StepTrigger) and self.name == o.name

    @abstractmethod
    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', tree: ValidationTree) -> None:
        pass


class CronTrigger(StepTrigger):
    """This allows the ingestion pulses to be specified using a cron string"""
    def __init__(self, name: str, cron: str):
        super().__init__(name)
        self.cron: str = cron

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"cron": self.cron})
        return rc

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, CronTrigger) and self.cron == o.cron

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if not validate_cron_string(self.cron):
            tree.addProblem(f"Invalid cron string <{self.cron}>")


class Credential(UserDSLObject, JSONable):
    """These allow a client to connect to a service/server"""
    def __init__(self) -> None:
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        pass

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
        }

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, Credential)):
            return True
        else:
            return False

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        pass


class FileSecretCredential(Credential):
    """This allows a secret to be read from the local filesystem. Usually the secret is
    placed in the file using an external service such as Docker secrets etc. The secret should be in the
    form of 2 lines, first line is user name, second line is password"""
    def __init__(self, filePath: str) -> None:
        super().__init__()
        self.secretFilePath: str = filePath

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "secretFilePath": self.secretFilePath,
        }

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is FileSecretCredential and self.secretFilePath == other.secretFilePath

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        # TODO This needs to be better
        if (self.secretFilePath == ""):
            tree.addProblem("Secret file path is empty")

    def __str__(self) -> str:
        return f"FileSecretCredential({self.secretFilePath})"


class UserPasswordCredential(Credential):
    """This is a simple user name and password credential"""
    def __init__(self, username: str, password: str) -> None:
        super().__init__()
        self.username: str = username
        self.password: str = password

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "username": self.username,
            "password": self.password,
        }

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is UserPasswordCredential and self.username == other.username and self.password == other.password

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if (self.username == ""):
            tree.addProblem("Username is empty")
        if (self.password == ""):
            tree.addProblem("Password is empty")

    def __str__(self) -> str:
        return f"UserPasswordCredential({self.username})"


class CertificateCredential(Credential):
    """This is a certificate based credential. The key for authentication is provided along with an optional
    certificate for the CA."""
    def __init__(self, certificate: Optional[str], key: str, authIsInSecure: bool) -> None:
        super().__init__()
        self.certificate: Optional[str] = certificate  # File name of the certification agent certificate
        self.key: str = key  # File name of the public key
        self.authIsInSecure: bool = authIsInSecure  # Is the authentication insecure

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "certificate": self.certificate,
            "key": self.key,
            "authIsInSecure": self.authIsInSecure,
        }

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is CertificateCredential and \
            self.certificate == other.certificate and self.key == other.key and self.authIsInSecure == other.authIsInSecure

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.key})"

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        if (self.key == ""):
            tree.addProblem("Key is empty")


class CredentialStore(UserDSLObject, JSONable):
    """This is a credential store which stores credential data in a set of infra locations"""
    def __init__(self, name: str, locs: set['LocationKey']) -> None:
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.name: str = name
        self.locs: set[LocationKey] = locs

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "name": self.name,
            "locs": {k: k.to_json() for k in self.locs},
        }

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and \
            isinstance(other, CredentialStore) and \
            self.name == other.name and \
            self.locs == other.locs

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        if (self.name == ""):
            tree.addProblem("Name is empty")
        for loc in self.locs:
            loc.lint(eco, tree.addSubTree(loc))

    @abstractmethod
    def getCredential(self, credName: str) -> Credential:
        """This returns the credential with the specified name"""
        pass


class LocalFileCredentialStore(CredentialStore):
    """This is a local file credential store. It represents a folder on the local file system where certificates are stored in files. This could be used with
    docker secrets or similar"""
    def __init__(self, name: str, locs: set['LocationKey'], folder: str) -> None:
        super().__init__(name, locs)
        self.credentials: dict[str, Credential] = dict()
        self.folder: str = folder

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"folder": self.folder})
        rc.update({"credentials": {k: v.to_json() for k, v in self.credentials.items()}})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, LocalFileCredentialStore) and self.folder == other.folder

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)

    def getCredential(self, credName: str) -> Credential:
        if credName not in self.credentials:
            c: Credential = FileSecretCredential(os.path.join(self.folder, credName))
            self.credentials[credName] = c
        return self.credentials[credName]


class ClearTextCredential(UserPasswordCredential):
    """This is implemented for testing but should never be used in production. All
    credentials should be stored and retrieved using secrets Credential objects also
    provided."""
    def __init__(self, username: str, password: str) -> None:
        super().__init__(username, password)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is ClearTextCredential

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        tree.addProblem("ClearText credential found", ProblemSeverity.WARNING)

    def __str__(self) -> str:
        return f"ClearTextCredential({self.username})"


class CaptureMetaData(UserDSLObject, JSONable):
    """This describes how a platform can pull data for a Datastore"""

    def __init__(self, *args: Union[StepTrigger, DataContainer, IngestionConsistencyType]):
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.singleOrMultiDatasetIngestion: Optional[IngestionConsistencyType] = None
        self.stepTrigger: Optional[StepTrigger] = None
        self.dataContainer: Optional[DataContainer] = None
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {}
        rc.update({"_type": self.__class__.__name__})
        rc.update({"singleOrMultiDatasetIngestion": self.singleOrMultiDatasetIngestion.value if self.singleOrMultiDatasetIngestion else None})
        rc.update({"stepTrigger": self.stepTrigger.to_json() if self.stepTrigger else None})
        rc.update({"dataContainer": self.dataContainer.to_json() if self.dataContainer else None})
        return rc

    def add(self, *args: Union[StepTrigger, DataContainer, IngestionConsistencyType]) -> None:
        for arg in args:
            if (isinstance(arg, StepTrigger)):
                if (self.stepTrigger is not None):
                    raise AttributeAlreadySetException("CaptureTrigger already set")
                self.stepTrigger = arg
            elif (isinstance(arg, IngestionConsistencyType)):
                if (self.singleOrMultiDatasetIngestion is not None):
                    raise AttributeAlreadySetException("SingleOrMultiDatasetIngestion already set")
                self.singleOrMultiDatasetIngestion = arg
            else:
                if (self.dataContainer is not None):
                    raise AttributeAlreadySetException("Container already set")
                self.dataContainer = arg

    @abstractmethod
    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        if (self.singleOrMultiDatasetIngestion is None):
            tree.addRaw(AttributeNotSet("Single Or Multi ingestion not specified"))

        if (self.dataContainer is None):
            # The container is implicit when its a DataTransformer (same as the Workspace container)
            if (not isinstance(self, DataTransformerOutput)):
                tree.addRaw(AttributeNotSet("Container not specified"))
            tree.addRaw(AttributeNotSet("Container not specified"))
        else:
            cTree: ValidationTree = tree.addSubTree(self.dataContainer)
            self.dataContainer.lint(eco, cTree)

        if (self.stepTrigger):
            self.stepTrigger.lint(eco, gz, t, tree)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, CaptureMetaData) and self.singleOrMultiDatasetIngestion == other.singleOrMultiDatasetIngestion and \
            self.stepTrigger == other.stepTrigger and self.dataContainer == other.dataContainer

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class DataTransformerOutput(CaptureMetaData):
    """Specifies this datastore is ingested whenever a Datatransformer executes"""
    def __init__(self, workSpaceName: str) -> None:
        super().__init__()
        self.workSpaceName = workSpaceName
        self.singleOrMultiDatasetIngestion = IngestionConsistencyType.MULTI_DATASET

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, d, tree)
        w: Optional[Workspace] = t.workspaces.get(self.workSpaceName)

        if (w is None):
            tree.addRaw(UnknownObjectReference(f"workspace {self.workSpaceName}", ProblemSeverity.ERROR))
        else:
            if (w.dataTransformer is None):
                tree.addRaw(DataTransformerMissing(f"Workspace {self.workSpaceName} must have dataTransformer", ProblemSeverity.ERROR))
            else:
                if (w.dataTransformer.outputDatastore.name != d.name):
                    tree.addRaw(ConstraintViolation(
                        f"Specified Workspace {self.workSpaceName} output store name {w.dataTransformer.outputDatastore.name} "
                        f"doesnt match referring datastore {d.name}", ProblemSeverity.ERROR))

    def __str__(self):
        return f"DataTransformerOutput({self.workSpaceName})"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DataTransformerOutput) and self.workSpaceName == other.workSpaceName


class IngestionMetadata(CaptureMetaData):
    """Producers use these to describe HOW to snapshot and pull deltas from a data source in to
    data pipelines. The ingestion service interprets these to allow code free ingestion from
    supported sources and handle operation pipelines."""
    def __init__(self, *args: Union[DataContainer, Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__()
        self.credential: Optional[Credential] = None
        self.add(*args)

    def add(self, *args: Union[Credential, DataContainer, StepTrigger, IngestionConsistencyType]) -> None:
        for arg in args:
            if (isinstance(arg, Credential)):
                c: Credential = arg
                if (self.credential is not None):
                    raise AttributeAlreadySetException("Credential already set")
                self.credential = c
            else:
                super().add(arg)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, IngestionMetadata):
            return super().__eq__(other) and self.credential == other.credential
        return False

    @abstractmethod
    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if (self.dataContainer):
            capTree: ValidationTree = tree.addSubTree(self.dataContainer)
            self.dataContainer.lint(eco, capTree)
        # Credential is needed for a platform connect to a datacontainer and ingest data
        if (self.credential is None):
            tree.addRaw(AttributeNotSet("credential"))
        else:
            self.credential.lint(eco, tree)
        super().lint(eco, gz, t, d, tree)


class CDCCaptureIngestion(IngestionMetadata):
    """This indicates CDC can be used to capture deltas from the source"""
    def __init__(self, dc: DataContainer, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(dc, *args)

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, d, tree)

    def __str__(self) -> str:
        return "CDCCaptureIngestion()"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is CDCCaptureIngestion


class SQLPullIngestion(IngestionMetadata):
    """This IMD describes how to pull a snapshot 'dump' from each dataset and then persist
    state variables which are used to next pull a delta per dataset and then persist the state
    again so that another delta can be pulled on the next pass and so on"""
    def __init__(self, dc: DataContainer, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(dc, *args)
        self.variableNames: list[str] = []
        """The names of state variables produced by snapshot and delta sql strings"""
        self.snapshotSQL: dict[str, str] = OrderedDict()
        """A SQL string per dataset which pulls a per table snapshot"""
        self.deltaSQL: dict[str, str] = OrderedDict()
        """A SQL string per dataset which pulls all rows which changed since last time for a table"""

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SQLPullIngestion):
            return super().__eq__(other) and self.variableNames == other.variableNames and \
                self.snapshotSQL == other.snapshotSQL and self.deltaSQL == other.deltaSQL
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        raise NotImplementedError()

    def __str__(self) -> str:
        return "SQLPullIngestion()"


class StreamingIngestion(IngestionMetadata):
    """This is an abstract class for streaming data sources"""
    def __init__(self, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(*args)


class KafkaServer(DataContainer):
    """This represents a connection to a Kafka Server."""
    def __init__(self, name: str, locs: set['LocationKey'], bootstrapServers: HostPortPairList, caCert: Optional[Credential] = None) -> None:
        super().__init__(name, locs)
        self.bootstrapServers: HostPortPairList = bootstrapServers
        self.caCertificate: Optional[Credential] = caCert

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, KafkaServer) and self.bootstrapServers == other.bootstrapServers and \
            self.caCertificate == other.caCertificate

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        self.bootstrapServers.lint(tree.addSubTree(self.bootstrapServers))
        if (self.caCertificate):
            self.caCertificate.lint(eco, tree.addSubTree(self.caCertificate))

    def __str__(self) -> str:
        return f"KafkaServer({self.bootstrapServers})"

    def projectDatasetSchema(self, dataset: 'Dataset') -> Optional[SchemaProjector]:
        """This takes a Dataset and returns the schema used when encoding it to a Kafka message"""
        return None

    def getNamingAdapter(self) -> Optional[DataContainerNamingMapper]:
        """This returns a naming adapter which can be used to map dataset names and attributes to the underlying data container"""
        return None


class KafkaIngestion(StreamingIngestion):
    """This allows a topic and a schema format to be specified for a source publishing messages to a Kafka topic"""
    def __init__(self, kafkaServer: DataContainer, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(*args)
        self.kafkaServer: DataContainer = kafkaServer

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, KafkaIngestion) and self.kafkaServer == other.kafkaServer

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, d, tree)
        if not isinstance(self.kafkaServer, KafkaServer):
            tree.addRaw(ObjectWrongType(self.kafkaServer, KafkaServer, ProblemSeverity.ERROR))
        else:
            kTree: ValidationTree = tree.addSubTree(self.kafkaServer)
            self.kafkaServer.lint(eco, kTree)


class DatasetPerTopicKafkaIngestion(KafkaIngestion):
    """This is a KafkaIngestion which uses a separate topic for each dataset"""
    def __init__(self, kafkaServer: DataContainer, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(kafkaServer, *args)


class Datastore(ANSI_SQL_NamedObject, Documentable, JSONable):

    """This is a named group of datasets. It describes how to capture the data and make it available for processing"""
    def __init__(self, name: str, *args: Union[Dataset, CaptureMetaData, Documentation, ProductionStatus, DeprecationInfo]) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        JSONable.__init__(self)
        self.datasets: dict[str, Dataset] = OrderedDict()
        self.key: Optional[DatastoreKey] = None
        self.cmd: Optional[CaptureMetaData] = None
        self.productionStatus: ProductionStatus = ProductionStatus.NOT_PRODUCTION
        self.deprecationStatus: DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)
        """Deprecating a store deprecates all datasets in the store regardless of their deprecation status"""
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"name": self.name, "datasets": {k: v.to_json() for k, v in self.datasets.items()}})
        if (self.key is not None):
            rc.update({"team": self.key.tdName, "governance_zone": self.key.gzName})
        rc.update({"cmd": self.cmd.to_json() if self.cmd else None})
        if (self.documentation):
            rc.update({"doc": self.documentation.to_json()})
        return rc

    def setTeam(self, tdKey: TeamDeclarationKey):
        self.key = DatastoreKey(tdKey, self.name)

    def add(self, *args: Union[Dataset, CaptureMetaData, Documentation, ProductionStatus, DeprecationInfo]) -> None:
        for arg in args:
            if (type(arg) is Dataset):
                d: Dataset = arg
                if self.datasets.get(d.name) is not None:
                    raise ObjectAlreadyExistsException(f"Duplicate Dataset {d.name}")
                self.datasets[d.name] = d
            elif (isinstance(arg, CaptureMetaData)):
                i: CaptureMetaData = arg
                if (self.cmd):
                    raise AttributeAlreadySetException("CMD")
                self.cmd = i
            elif (isinstance(arg, ProductionStatus)):
                self.productionStatus = arg
            elif (isinstance(arg, DeprecationInfo)):
                self.deprecationStatus = arg
            elif (isinstance(arg, Documentation)):
                doc: Documentation = arg
                self.documentation = doc

    def isDatasetDeprecated(self, dataset: Dataset) -> bool:
        """Returns true if the datastore is deprecated OR dataset is deprecated"""
        return self.deprecationStatus.status == DeprecationStatus.DEPRECATED or dataset.deprecationStatus.status == DeprecationStatus.DEPRECATED

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Datastore):
            return ANSI_SQL_NamedObject.__eq__(self, other) and Documentable.__eq__(self, other) and \
                self.datasets == other.datasets and self.cmd == other.cmd and \
                self.productionStatus == other.productionStatus and self.deprecationStatus == other.deprecationStatus and \
                self.key == other.key
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', storeTree: ValidationTree) -> None:
        self.nameLint(storeTree)
        if (self.key is None):
            storeTree.addRaw(AttributeNotSet(f"{self} has no key"))
        if (self.documentation):
            self.documentation.lint(storeTree)
        for dataset in self.datasets.values():
            dTree: ValidationTree = storeTree.addSubTree(dataset)
            dataset.lint(eco, gz, t, self, dTree)
            if (self.productionStatus == ProductionStatus.PRODUCTION):
                if (not dataset.hasClassifications()):
                    dTree.addRaw(ProductionDatastoreMustHaveClassifications(self, dataset))

        if (self.cmd):
            cmdTree: ValidationTree = storeTree.addSubTree(self.cmd)
            self.cmd.lint(eco, gz, t, self, cmdTree)
        else:
            storeTree.addRaw(AttributeNotSet("CaptureMetaData not set"))
        if (len(self.datasets) == 0):
            storeTree.addRaw(AttributeNotSet("No datasets in store"))

    def checkForBackwardsCompatibility(self, other: object, vTree: ValidationTree) -> bool:
        """This checks if the other datastore is backwards compatible with this one. This means that the other datastore
        can be used to replace this one without breaking any data pipelines"""

        if (not isinstance(other, Datastore)):
            vTree.addRaw(ObjectWrongType(other, Datastore, ProblemSeverity.ERROR))
            return False
        super().checkForBackwardsCompatibility(other, vTree)
        # Check if the datasets are compatible
        for dataset in self.datasets.values():
            dTree: ValidationTree = vTree.addSubTree(dataset)
            otherDataset: Optional[Dataset] = other.datasets.get(dataset.name)
            if (otherDataset):
                dataset.checkForBackwardsCompatibility(otherDataset, dTree)
            else:
                dTree.addRaw(ObjectMissing(other, dataset, ProblemSeverity.ERROR))
        return not vTree.hasErrors()

    def __str__(self) -> str:
        return f"Datastore({self.name})"


class TeamCacheEntry:
    """This is used by Ecosystem to cache teams"""
    def __init__(self, t: 'Team', td: 'TeamDeclaration') -> None:
        self.team: Team = t
        self.declaration: TeamDeclaration = td

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, TeamCacheEntry)):
            return self.team == other.team and self.declaration == other.declaration
        return False


class WorkspaceCacheEntry:
    """This is used by Ecosystem to cache workspaces"""
    def __init__(self, w: 'Workspace', t: 'Team') -> None:
        self.workspace: Workspace = w
        self.team: Team = t

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, WorkspaceCacheEntry)):
            return self.workspace == other.workspace and self.team == other.team
        return False


class DatastoreCacheEntry:
    """This is used by Ecosystem to cache datastores"""
    def __init__(self, d: 'Datastore', t: 'Team') -> None:
        self.datastore: Datastore = d
        self.team: Team = t

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, DatastoreCacheEntry)):
            return self.datastore == other.datastore and self.team == other.team
        return False


class DependentWorkspaces(JSONable):
    """This tracks a Workspaces dependent on a datastore"""
    def __init__(self, workSpace: 'Workspace'):
        JSONable.__init__(self)
        self.workspace: Workspace = workSpace
        self.dependencies: set[DependentWorkspaces] = set()

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "workspaceName": self.workspace.name, "dependencies": [dep.to_json() for dep in self.dependencies]}

    def addDependency(self, dep: 'DependentWorkspaces') -> None:
        self.dependencies.add(dep)

    def flatten(self) -> set['Workspace']:
        """Returns a flattened list of dependencies"""
        rc: set[Workspace] = {self.workspace}
        for dep in self.dependencies:
            rc.update(dep.flatten())
        return rc

    def __str__(self) -> str:
        return f"Dependency({self.flatten()})"

    def __hash__(self) -> int:
        return hash(self.workspace.name)

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, DependentWorkspaces)):
            return self.workspace.name == __value.workspace.name
        else:
            return False


class DefaultDataPlatform(UserDSLObject):
    def __init__(self, p: 'DataPlatformKey'):
        UserDSLObject.__init__(self)
        self.defaultPlatform: 'DataPlatformKey' = p

    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        if (eco.getDataPlatform(self.defaultPlatform.name) is None):
            tree.addRaw(AttributeNotSet("Default Data Platform not set"))
        else:
            self.defaultPlatform.lint(eco, tree.addSubTree(self))

    def get(self, eco: 'Ecosystem') -> 'DataPlatform':
        """This returns the default DataPlatform or throws an Exception if it has not been specified"""
        return eco.getDataPlatformOrThrow(self.defaultPlatform.name)

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, DefaultDataPlatform)):
            return self.defaultPlatform == other.defaultPlatform
        return False


# Add regulators here with their named retention policies for reference in Workspaces
# Feels like regulators are across GovernanceZones
class Ecosystem(GitControlledObject, JSONable):

    def createGZone(self, name: str, repo: Repository) -> 'GovernanceZone':
        gz: GovernanceZone = GovernanceZone(name, repo)
        gz.setEcosystem(self)
        return gz

    def __init__(self, name: str, repo: Repository,
                 *args: Union['DataPlatform', Documentation, DefaultDataPlatform, InfrastructureVendor, 'GovernanceZoneDeclaration']) -> None:
        GitControlledObject.__init__(self, repo)
        JSONable.__init__(self)
        self.name: str = name
        self.key: EcosystemKey = EcosystemKey(self.name)

        self.zones: AuthorizedObjectManager[GovernanceZone, GovernanceZoneDeclaration] = \
            AuthorizedObjectManager[GovernanceZone, GovernanceZoneDeclaration]("zones", lambda name, repo: self.createGZone(name, repo), repo)
        """This is the authorative list of governance zones within the ecosystem"""

        self.vendors: dict[str, InfrastructureVendor] = OrderedDict[str, InfrastructureVendor]()
        self.dataPlatforms: dict[str, DataPlatform] = OrderedDict[str, DataPlatform]()
        self.defaultDataPlatform: Optional[DefaultDataPlatform] = None
        self.resetCaches()
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "zones": {k: k.name for k in self.zones.defineAllObjects()},
            "vendors": {k: k.to_json() for k in self.vendors.values()},
            "dataPlatforms": {k: k.to_json() for k in self.dataPlatforms.values()},
        }

    def resetCaches(self) -> None:
        """Empties the caches"""
        self.datastoreCache: dict[str, DatastoreCacheEntry] = {}
        """This is a cache of all data stores in the ecosystem"""
        self.workSpaceCache: dict[str, WorkspaceCacheEntry] = {}
        """This is a cache of all workspaces in the ecosystem"""
        self.teamCache: dict[str, TeamCacheEntry] = {}
        """This is a cache of all team declarations in the ecosystem"""

    def add(self, *args: Union['DataPlatform', DefaultDataPlatform, Documentation, InfrastructureVendor, 'GovernanceZoneDeclaration']) -> None:
        for arg in args:
            if isinstance(arg, InfrastructureVendor):
                if self.vendors.get(arg.name) is not None:
                    raise ObjectAlreadyExistsException(f"Duplicate Vendor {arg.name}")
                self.vendors[arg.name] = arg
            elif isinstance(arg, Documentation):
                self.documentation = arg
            elif isinstance(arg, DataPlatform):
                self.dataPlatforms[arg.name] = arg
            elif isinstance(arg, DefaultDataPlatform):
                if (self.defaultDataPlatform is not None):
                    raise AttributeAlreadySetException("Default DataPlatform already specified")
                else:
                    self.defaultDataPlatform = arg
            else:
                self.zones.addAuthorization(arg)
                arg.key = GovernanceZoneKey(self.key, arg.name)

        for vendor in self.vendors.values():
            vendor.setEcosystem(self)

    def getDefaultDataPlatform(self) -> 'DataPlatform':
        """This returns the default DataPlatform or throws an Exception if it has not been specified"""
        if (self.defaultDataPlatform):
            return self.defaultDataPlatform.get(self)
        else:
            raise Exception("No default data platform specified")

    def getVendor(self, name: str) -> Optional[InfrastructureVendor]:
        return self.vendors.get(name)

    def checkDataPlatformExists(self, d: 'DataPlatform') -> bool:
        """This checks if the data platform exists in the ecosystem and is equal to the one in the ecosystem
        with the same name"""
        if (d.name in self.dataPlatforms):
            return self.dataPlatforms[d.name] == d
        return False

    def getVendorOrThrow(self, name: str) -> InfrastructureVendor:
        v: Optional[InfrastructureVendor] = self.getVendor(name)
        if (v):
            if (v.key is None):
                v.setEcosystem(self)
            return v
        else:
            raise ObjectDoesntExistException(f"Unknown vendor {name}")

    def getDataPlatform(self, name: str) -> Optional['DataPlatform']:
        return self.dataPlatforms.get(name)

    def getDataPlatformOrThrow(self, name: str) -> 'DataPlatform':
        p: Optional['DataPlatform'] = self.getDataPlatform(name)
        if (p is None):
            raise ObjectDoesntExistException(f"Unknown data platform {name}")
        return p

    def getLocation(self, vendorName: str, locKey: list[str]) -> Optional[InfrastructureLocation]:
        vendor: Optional[InfrastructureVendor] = self.getVendor(vendorName)
        loc: Optional[InfrastructureLocation] = None
        if vendor:
            loc: Optional[InfrastructureLocation] = vendor.findLocationUsingKey(locKey)
        return loc

    def getLocationOrThrow(self, vendorName: str, locKey: list[str]) -> InfrastructureLocation:
        vendor: InfrastructureVendor = self.getVendorOrThrow(vendorName)
        loc: Optional[InfrastructureLocation] = vendor.findLocationUsingKey(locKey)
        if loc is None:
            raise ObjectDoesntExistException(f"Unknown location {locKey} in vendor {vendorName}")
        return loc

    def getAllChildLocations(self, vendorName: str, locKey: list[str]) -> set[InfrastructureLocation]:
        """Returns all child locations. Typically used to return all locations within a country for example"""
        rc: set[InfrastructureLocation] = set(self.getLocationOrThrow(vendorName, locKey).locations.values())
        return rc

    def cache_addTeam(self, td: 'TeamDeclaration', t: 'Team'):
        if td.key is None:
            raise Exception("{td} key is None")
        globalTeamName: str = td.key.gzName + "/" + t.name
        if (self.teamCache.get(globalTeamName) is not None):
            raise ObjectAlreadyExistsException(f"Duplicate Team {globalTeamName}")
        self.teamCache[globalTeamName] = TeamCacheEntry(t, td)

    def cache_addWorkspace(self, team: 'Team', work: 'Workspace'):
        """This adds a workspace to the eco cache and flags duplicates"""
        if (self.workSpaceCache.get(work.name) is not None):
            raise ObjectAlreadyExistsException(f"Duplicate workspace {work.name}")
        self.workSpaceCache[work.name] = WorkspaceCacheEntry(work, team)

    def cache_addDatastore(self, store: 'Datastore', t: 'Team'):
        """This adds a store to the eco cache and flags duplicates"""
        if (self.datastoreCache.get(store.name) is not None):
            raise ObjectAlreadyExistsException(f"Duplicate data store {store.name}")
        self.datastoreCache[store.name] = DatastoreCacheEntry(store, t)

    def cache_getWorkspaceOrThrow(self, work: str) -> WorkspaceCacheEntry:
        """This returns the named workspace if it exists"""
        w: Optional[WorkspaceCacheEntry] = self.workSpaceCache.get(work)
        if (w is None):
            raise ObjectDoesntExistException(f"Unknown workspace {work}")
        return w

    def cache_getDatastoreOrThrow(self, store: str) -> DatastoreCacheEntry:
        s: Optional[DatastoreCacheEntry] = self.datastoreCache.get(store)
        if s is None:
            raise ObjectDoesntExistException(f"Unknown datastore {store}")
        return s

    def cache_getDataset(self, storeName: str, datasetName: str) -> Optional[Dataset]:
        """This returns the named dataset if it exists"""
        s: Optional[DatastoreCacheEntry] = self.datastoreCache.get(storeName)
        if (s):
            dataset = s.datastore.datasets.get(datasetName)
            return dataset
        return None

    def lintAndHydrateCaches(self) -> ValidationTree:
        """This validates the ecosystem and returns a list of problems which is empty if there are no issues"""
        self.resetCaches()

        # This will lint the ecosystem, zones, teams, datastores and datasets.
        ecoTree: ValidationTree = ValidationTree(self)

        # This will lint the ecosystem, zones, teams, datastores and datasets.
        # Workspaces are linted in a second pass later.
        # It populates the caches for zones, teams, stores and workspaces.
        """No need to dedup zones as the authorative list is already a dict"""
        for gz in self.zones.authorizedObjects.values():
            govTree: ValidationTree = ecoTree.addSubTree(gz)
            gz.lint(self, govTree)

        """All caches should now be populated"""

        for vendor in self.vendors.values():
            vTree: ValidationTree = ecoTree.addSubTree(vendor)
            vendor.lint(vTree)

        for pl in self.dataPlatforms.values():
            platTree: ValidationTree = ecoTree.addSubTree(pl)
            pl.lint(self, platTree)

        # Now lint the workspaces
        for workSpaceCacheEntry in self.workSpaceCache.values():
            workSpace = workSpaceCacheEntry.workspace
            wsTree: ValidationTree = ecoTree.addSubTree(workSpace)
            if (workSpace.key):
                gz: GovernanceZone = self.getZoneOrThrow(workSpace.key.gzName)
                workSpace.lint(self, gz, workSpaceCacheEntry.team, wsTree)
        self.superLint(ecoTree)
        self.zones.lint(ecoTree)
        if (self.documentation):
            self.documentation.lint(ecoTree)

        if (self.defaultDataPlatform is not None):
            self.defaultDataPlatform.lint(self, ecoTree)

        # If there are no errors at this point then
        # Generate pipeline graphs and lint them.
        # This will ask each DataPlatform to verify that it
        # can generate a pipeline graph for its assigned DAG subset. This
        # can fail for a variety of reasons such as the DataPlatform does not
        # support certain DataContainers or even schema mapping issues to underlying
        # infrastructure.

        if not ecoTree.hasErrors():
            try:
                graph: EcosystemPipelineGraph = EcosystemPipelineGraph(self)

                graph.lint(ecoTree.addSubTree(graph))
            except Exception as e:
                ecoTree.addProblem(f"Error generating pipeline graph {e}", ProblemSeverity.ERROR)

        return ecoTree

    def calculateDependenciesForDatastore(self, storeName: str, wsVisitedSet: set[str] = set()) -> Sequence[DependentWorkspaces]:
        # TODO make tests
        rc: list[DependentWorkspaces] = []
        store: Datastore = self.datastoreCache[storeName].datastore

        # If the store is used in any Workspace then thats a dependency
        for w in self.workSpaceCache.values():
            # Do not enter a cyclic loop
            if (w.workspace.name not in wsVisitedSet):
                if (w.workspace.isDatastoreUsed(store)):
                    workspace: Workspace = w.workspace
                    dep: DependentWorkspaces = DependentWorkspaces(workspace)
                    rc.append(dep)
                    # prevent cyclic loops
                    wsVisitedSet.add(workspace.name)
                    # If the workspace has a data transformer then the output store's dependencies are also dependencies
                    if (workspace.dataTransformer is not None):
                        outputStore: Datastore = workspace.dataTransformer.outputDatastore
                        depList: Sequence[DependentWorkspaces] = self.calculateDependenciesForDatastore(outputStore.name, wsVisitedSet)
                        for dep2 in depList:
                            dep.addDependency(dep2)
        return rc

    def checkIfChangesAreAuthorized(self, proposed: GitControlledObject, changeSource: Repository, vTree: ValidationTree) -> None:
        """This checks if the ecosystem top level has changed relative to the specified change source"""
        """This checks if any Governance zones has been added or removed relative to e"""

        prop_eco: Ecosystem = cast(Ecosystem, proposed)

        self.checkTopLevelAttributeChangesAreAuthorized(prop_eco, changeSource, vTree)

        zTree: ValidationTree = vTree.addSubTree(self.zones)
        self.zones.checkIfChangesAreAuthorized(prop_eco.zones, changeSource, zTree)

    def __eq__(self, proposed: object) -> bool:
        if super().__eq__(proposed) and isinstance(proposed, Ecosystem):
            rc = self.name == proposed.name
            rc = rc and self.zones == proposed.zones
            rc = rc and self.key == proposed.key
            rc = rc and self.vendors == proposed.vendors
            rc = rc and self.dataPlatforms == proposed.dataPlatforms
            rc = rc and self.defaultDataPlatform == proposed.defaultDataPlatform
            return rc
        else:
            return False

    def areTopLevelChangesAuthorized(self, proposed: GitControlledObject, changeSource: Repository, tree: ValidationTree) -> bool:
        """This is a shallow equality check for the top level ecosystem object"""
        if (isinstance(proposed, Ecosystem)):
            rc: bool = True
            # If we are being modified by a potentially unauthorized source then check
            if (self.owningRepo != changeSource):
                rc = super().areTopLevelChangesAuthorized(proposed, changeSource, tree)
                if self.name != proposed.name:
                    tree.addRaw(UnauthorizedAttributeChange("name", self.name, proposed.name, ProblemSeverity.ERROR))
                    rc = False
                if self.owningRepo != proposed.owningRepo:
                    tree.addRaw(UnauthorizedAttributeChange("owningRepo", self.owningRepo, proposed.owningRepo, ProblemSeverity.ERROR))
                    rc = False
                zTree: ValidationTree = tree.addSubTree(self.zones)
                if not self.zones.areTopLevelChangesAuthorized(proposed.zones, changeSource, zTree):
                    rc = False
                if self._check_dict_changes(self.vendors, proposed.vendors, tree, "Vendors"):
                    rc = False
                if self._check_dict_changes(self.dataPlatforms, proposed.dataPlatforms, tree, "DataPlatforms"):
                    rc = False
                if self.defaultDataPlatform != proposed.defaultDataPlatform:
                    tree.addRaw(UnauthorizedAttributeChange("defaultDataPlatformn", self.defaultDataPlatform,
                                                            proposed.defaultDataPlatform, ProblemSeverity.ERROR))
                    rc = False
            return rc
        else:
            return False

    def getZone(self, gz: str) -> Optional['GovernanceZone']:
        """Returns the governance zone with the specified name"""
        zone: Optional[GovernanceZone] = self.zones.getObject(gz)
        return zone

    def getZoneOrThrow(self, gz: str) -> 'GovernanceZone':
        z: Optional[GovernanceZone] = self.getZone(gz)
        if (z is None):
            raise ObjectDoesntExistException(f"Unknown zone {gz}")
        return z

    def getTeam(self, gz: str, teamName: str) -> Optional['Team']:
        """Returns the team with the specified name in the specified zone"""
        zone: Optional[GovernanceZone] = self.getZone(gz)
        if (zone):
            t: Optional[Team] = zone.getTeam(teamName)
            return t
        else:
            return None

    def getTeamOrThrow(self, gz: str, teamName: str) -> 'Team':
        t: Optional[Team] = self.getTeam(gz, teamName)
        if (t is None):
            raise ObjectDoesntExistException(f"Unknown team {teamName} in zone {gz}")
        return t

    def __str__(self) -> str:
        return f"Ecosystem({self.name})"

    def checkIfChangesAreBackwardsCompatibleWith(self, originEco: 'Ecosystem', vTree: ValidationTree) -> None:
        """This checks if the proposed ecosystem is backwards compatible with the current ecosystem"""
        # Check if the zones are compatible
        for zone in self.zones.authorizedObjects.values():
            zTree: ValidationTree = vTree.addSubTree(zone)
            originZone: Optional[GovernanceZone] = originEco.getZone(zone.name)
            if originZone:
                zone.checkForBackwardsCompatiblity(originZone, zTree)

    # Check that the changeSource is one of the authorized sources
    def checkIfChangeSourceIsUsed(self, changeSource: Repository, tree: ValidationTree) -> None:
        # First, gather all the repositories used by the parts in a set
        allSources: set[Repository] = set()
        allSources.add(self.owningRepo)
        # All declared zones
        for zone in self.zones.authorizedNames.values():
            allSources.add(zone.owningRepo)
        # Any teams for defined zones.
        for zone in self.zones.authorizedObjects.values():
            for team in zone.teams.authorizedObjects.values():
                allSources.add(team.owningRepo)
        # Now, just check if the changeSource is in the set
        if changeSource not in allSources:
            tree.addRaw(UnknownChangeSource(changeSource, ProblemSeverity.ERROR))

    def checkIfChangesCanBeMerged(self, proposed: 'Ecosystem', source: Repository) -> ValidationTree:
        """This is called to check if the proposed changes can be merged in to the current ecosystem. It returns a ValidationTree with issues if not
        or an empty ValidationTree if allowed."""

        # First, the incoming ecosystem must be consistent and pass lint checks
        eTree: ValidationTree = proposed.lintAndHydrateCaches()

        # Any errors make us fail immediately
        # But we want warnings and infos to accumulate for the caller
        if eTree.hasErrors():
            return eTree

        # Check if the changeSource is one of the authorized sources
        self.checkIfChangeSourceIsUsed(source, eTree)
        if eTree.hasErrors():
            return eTree

        # Check if the proposed changes being made by an authorized repository
        self.checkIfChangesAreAuthorized(proposed, source, eTree)
        if eTree.hasErrors():
            return eTree

        # Check if the proposed changes are backwards compatible this object
        proposed.checkIfChangesAreBackwardsCompatibleWith(self, eTree)
        return eTree


class InvalidLocationStringProblem(ValidationProblem):
    def __init__(self, problem: str, locStr: str, severity: ProblemSeverity) -> None:
        super().__init__(f"{problem}: {locStr}", severity)

    def __hash__(self) -> int:
        return hash(self.description)


class UnknownLocationProblem(ValidationProblem):
    def __init__(self, locStr: str, severity: ProblemSeverity) -> None:
        super().__init__(f"Unknown location {locStr}", severity)

    def __hash__(self) -> int:
        return hash(self.description)


class UnknownVendorProblem(ValidationProblem):
    def __init__(self, vendor: str, severity: ProblemSeverity) -> None:
        super().__init__(f"Unknown vendor {vendor}", severity)

    def __hash__(self) -> int:
        return hash(self.description)


class LocationKey(UserDSLObject, JSONable):
    """This is used to reference a location on a vendor during DSL construction. This string has format vendor:loc1/loc2/loc3/..."""
    def __init__(self, locStr: str) -> None:
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.locStr: str = locStr
        self.loc: Optional[InfrastructureLocation] = None

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "locStr": self.locStr}

    def getAsInfraLocation(self, eco: Ecosystem) -> Optional[InfrastructureLocation]:
        # The string is in the format vendor:location1/location2/location3
        if self.loc is not None:
            return self.loc
        locList: list[str] = self.locStr.split(":")
        if (len(locList) != 2):
            raise Exception(f"Invalid location string {self.locStr}")
        vendor = locList[0]
        locationParts = locList[1].split("/")

        # Skip the first locationParts element here
        loc: Optional[InfrastructureLocation] = eco.getLocation(vendor, locationParts)
        return loc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, LocationKey)):
            return self.locStr == other.locStr
        return False

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        # First check syntax is correct
        locList: list[str] = self.locStr.split(":")
        if (len(locList) != 2):
            tree.addRaw(InvalidLocationStringProblem("Format must be vendor:loc/loc/loc", self.locStr, ProblemSeverity.ERROR))
            return
        vendor = locList[0]
        locationParts: list[str] = locList[1].split("/")
        if (len(locationParts) == 0):
            tree.addRaw(InvalidLocationStringProblem("One location must be specified", self.locStr, ProblemSeverity.ERROR))
            return
        if (len(locationParts[0]) == 0):
            tree.addRaw(InvalidLocationStringProblem("First location should not start with '/'", self.locStr, ProblemSeverity.ERROR))
            return
        for loc in locationParts:
            if (len(loc) == 0):
                tree.addRaw(InvalidLocationStringProblem("Empty locations not allowed", self.locStr, ProblemSeverity.ERROR))
                return
        if (eco.getVendor(vendor) is None):
            tree.addRaw(UnknownVendorProblem(vendor, ProblemSeverity.ERROR))
            return
        self.loc: Optional[InfrastructureLocation] = self.getAsInfraLocation(eco)
        if (self.loc is None):
            tree.addRaw(UnknownLocationProblem(self.locStr, ProblemSeverity.ERROR))

    def __str__(self) -> str:
        return f"LocationKey({self.locStr})"

    def __hash__(self) -> int:
        return hash(self.locStr)


class VendorKey(UserDSLObject, JSONable):
    """This is used to reference a vendor during DSL construction"""
    def __init__(self, vendor: str) -> None:
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.vendorString: str = vendor
        self.vendor: Optional[InfrastructureVendor] = None

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "vendorString": self.vendorString}

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, VendorKey)):
            return self.vendorString == other.vendorString
        return False

    def getAsInfraVendor(self, eco: Ecosystem) -> Optional[InfrastructureVendor]:
        if self.vendor is not None:
            return self.vendor
        vendor: Optional[InfrastructureVendor] = eco.getVendor(self.vendorString)
        return vendor

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        if (self.getAsInfraVendor(eco) is None):
            tree.addRaw(UnknownVendorProblem(self.vendorString, ProblemSeverity.ERROR))

    def __str__(self) -> str:
        return f"VendorKey({self.vendorString})"

    def __hash__(self) -> int:
        return hash(self.vendorString)


class Team(GitControlledObject, JSONable):
    """This is the authoritive definition of a team within a goverance zone. All teams must have
    a corresponding TeamDeclaration in the owning GovernanceZone"""
    def __init__(self, name: str, repo: Repository, *args: Union[Datastore, 'Workspace', Documentation, DataContainer]) -> None:
        GitControlledObject.__init__(self, repo)
        JSONable.__init__(self)
        self.name: str = name
        self.workspaces: dict[str, Workspace] = OrderedDict()
        self.dataStores: dict[str, Datastore] = OrderedDict()
        self.containers: dict[str, DataContainer] = OrderedDict()
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"name": self.name})
        rc.update({"dataStores": {k: v.name for k, v in self.dataStores.items()}})
        rc.update({"workspaces": {k: v.name for k, v in self.workspaces.items()}})
        rc.update({"containers": {k: v.to_json() for k, v in self.containers.items()}})
        return rc

    def add(self, *args: Union[Datastore, 'Workspace', Documentation, DataContainer]) -> None:
        """Adds a workspace, datastore or gitrepository to the team"""
        for arg in args:
            if (isinstance(arg, Datastore)):
                s: Datastore = arg
                self.addStore(s)
            elif (isinstance(arg, Workspace)):
                w: Workspace = arg
                self.addWorkspace(w)
            elif (isinstance(arg, DataContainer)):
                dc: DataContainer = arg
                if self.containers.get(dc.name) is not None:
                    raise ObjectAlreadyExistsException(f"Duplicate DataContainer {dc.name}")
                self.containers[dc.name]
            else:
                d: Documentation = arg
                self.documentation = d

    def addStore(self, store: Datastore):
        """Adds a datastore to the team checking for duplicates"""
        if self.dataStores.get(store.name) is not None:
            raise ObjectAlreadyExistsException(f"Duplicate Datastore {store.name}")
        self.dataStores[store.name] = store

    def addWorkspace(self, w: 'Workspace'):
        if self.workspaces.get(w.name) is not None:
            raise ObjectAlreadyExistsException(f"Duplicate Workspace {w.name}")
        self.workspaces[w.name] = w
        if (w.dataTransformer):
            oStore: Datastore = w.dataTransformer.outputDatastore
            if oStore.cmd:
                raise AttributeAlreadySetException("Transformer {w.dataTransformer} Datastore CMD is set automatically, do not set manually")

            # Set CMD for Refiner output store
            cmd: CaptureMetaData = DataTransformerOutput(w.name)
            # the transformer will capture data from the Workspace container
            # when the transformer finishes running
            if (w.dataContainer):
                cmd.add(w.dataContainer)
            oStore.add(cmd)
            self.addStore(w.dataTransformer.outputDatastore)

    def __eq__(self, other: object) -> bool:
        if super().__eq__(other) and isinstance(other, Team):
            rc: bool = self.name == other.name
            rc = rc and self.workspaces == other.workspaces
            rc = rc and self.dataStores == other.dataStores
            rc = rc and self.containers == other.containers
            return rc
        return False

    def getStoreOrThrow(self, storeName: str) -> Datastore:
        rc: Optional[Datastore] = self.dataStores.get(storeName)
        if rc is None:
            raise ObjectDoesntExistException(f"Unknown datastore {storeName}")
        return rc

    def getDataContainerOrThrow(self, containerName: str) -> DataContainer:
        """Returns the named data container or throws an exception if it does not exist"""
        rc: Optional[DataContainer] = self.containers.get(containerName)
        if rc is None:
            raise ObjectDoesntExistException(f"Unknown data container {containerName}")
        return rc

    def areTopLevelChangesAuthorized(self, proposed: GitControlledObject, changeSource: Repository, tree: ValidationTree) -> bool:
        """This is a shallow equality check for the top level team object"""
        # If we are being changed by an authorized source then it doesnt matter
        if (self.owningRepo == changeSource):
            return True
        if not super().areTopLevelChangesAuthorized(proposed, changeSource, tree):
            return False
        if not isinstance(proposed, Team):
            return False
        if self._check_dict_changes(self.dataStores, proposed.dataStores, tree, "DataStores"):
            return False
        if self._check_dict_changes(self.workspaces, proposed.workspaces, tree, "Workspaces"):
            return False
        if self._check_dict_changes(self.containers, proposed.containers, tree, "Containers"):
            return False
        return True

    def checkIfChangesAreAuthorized(self, proposed: GitControlledObject, changeSource: Repository, vTree: ValidationTree) -> None:
        """This checks if the team has changed relative to the specified change source"""
        prop_Team: Team = cast(Team, proposed)

        self.checkTopLevelAttributeChangesAreAuthorized(prop_Team, changeSource, vTree)

    def lint(self, eco: Ecosystem, gz: 'GovernanceZone', td: 'TeamDeclaration', teamTree: ValidationTree) -> None:
        """This validates a single team declaration and populates the datastore cache with that team's stores"""
        for s in self.dataStores.values():
            if eco.datastoreCache.get(s.name) is not None:
                teamTree.addRaw(DuplicateObject(s, ProblemSeverity.ERROR))
            else:
                storeTree: ValidationTree = teamTree.addSubTree(s)
                eco.cache_addDatastore(s, self)
                if (td.key):
                    s.setTeam(td.key)
                s.lint(eco, gz, self, storeTree)

        # Iterate over the workspaces to populate the cache but dont lint them yet
        for w in self.workspaces.values():
            if eco.workSpaceCache.get(w.name) is not None:
                teamTree.addRaw(DuplicateObject(w, ProblemSeverity.ERROR))
                # Cannot validate Workspace datasets until everything is loaded
            else:
                eco.cache_addWorkspace(self, w)
                if (gz.key):
                    w.setTeam(TeamDeclarationKey(gz.key, self.name))
                else:
                    teamTree.addRaw(AttributeNotSet(f"{gz} has no key"))
                wTree: ValidationTree = teamTree.addSubTree(w)

                # Check all classification allows policies from gz are satisfied on every sink
                for dccPolicy in gz.classificationPolicies.values():
                    for dsg in w.dsgs.values():
                        for sink in dsg.sinks.values():
                            store: Datastore = eco.cache_getDatastoreOrThrow(sink.storeName).datastore
                            dataset: Dataset = store.datasets[sink.datasetName]
                            if (not dataset.checkClassificationsAreOnly(dccPolicy)):
                                wTree.addRaw(ObjectNotCompatibleWithPolicy(sink, dccPolicy, ProblemSeverity.ERROR))

        # Iterate over DataContainers linting as we go
        for c in self.containers.values():
            cTree: ValidationTree = teamTree.addSubTree(c)
            c.lint(eco, cTree)
        self.superLint(teamTree)

    def __str__(self) -> str:
        return f"Team({self.name})"

    def checkForBackwardsCompatibility(self, originTeam: 'Team', vTree: ValidationTree):
        """This checks if the current team is backwards compatible with the origin team"""
        # Check if the datasets are compatible
        for store in self.dataStores.values():
            sTree: ValidationTree = vTree.addSubTree(store)
            originStore: Optional[Datastore] = originTeam.dataStores.get(store.name)
            if (originStore):
                store.checkForBackwardsCompatibility(originStore, sTree)


class NamedObjectAuthorization:
    """This represents a named object under the management of a repository. It is used to authorize the existence
    of the object before the specified repository can be used to edit/specify it."""
    def __init__(self, name: str, owningRepo: Repository) -> None:
        self.name: str = name
        self.owningRepo: Repository = owningRepo

    def lint(self, tree: ValidationTree):
        self.owningRepo.lint(tree)

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, NamedObjectAuthorization)):
            return self.name == __value.name and self.owningRepo == __value.owningRepo
        else:
            return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"


G = TypeVar('G', bound=GitControlledObject)
N = TypeVar('N', bound=NamedObjectAuthorization)


class AuthorizedObjectManager(GitControlledObject, Generic[G, N]):
    """This tracks a list of named authorizations and the named objects themselves in seperate lists. It is used
    to allow one repository to managed the authorization to create named objects using a second object specific repository or branch.
    Each named object can then be managed by a seperate repository. """
    def __init__(self, name: str, factory: Callable[[str, Repository], G], owningRepo: Repository) -> None:
        super().__init__(owningRepo)
        self.name: str = name
        self.authorizedNames: dict[str, N] = OrderedDict[str, N]()
        self.authorizedObjects: dict[str, G] = OrderedDict[str, G]()
        self.factory: Callable[[str, Repository], G] = factory

    def getNumObjects(self) -> int:
        """Returns the number of objects"""
        return len(self.authorizedObjects)

    def addAuthorization(self, t: N):
        """This is used to add a named authorization along with its owning repository to the list of authorizations."""
        if self.authorizedNames.get(t.name) is not None:
            raise ObjectAlreadyExistsException(f"Duplicate authorization {t.name}")
        self.authorizedNames[t.name] = t

    def defineAllObjects(self) -> list[G]:
        """This 'defines' all declared objects"""
        keys: list[G] = list()
        for n in self.authorizedNames.values():
            v: Optional[G] = self.getObject(n.name)
            if (v is not None):
                keys.append(v)
        return keys

    def getObject(self, name: str) -> Optional[G]:
        """This returns a managed object for the specified name. Users can then fill out the attributes
        of the returned object."""
        noa: Optional[N] = self.authorizedNames.get(name)
        if (noa is None):
            return None
        t: Optional[G] = self.authorizedObjects.get(name)
        if (t is None):
            t = self.factory(name, noa.owningRepo)  # Create an instance of the object
            self.authorizedObjects[name] = t
        return t

    def __eq__(self, other: object) -> bool:
        if (super().__eq__(other) and isinstance(other, AuthorizedObjectManager)):
            a: AuthorizedObjectManager[G, N] = cast(AuthorizedObjectManager[G, N], other)
            rc: bool = self.authorizedNames == a.authorizedNames
            rc = rc and self.name == a.name
            rc = rc and self.authorizedObjects == a.authorizedObjects
            # Cannot test factory for equality
            # rc = rc and self.factory == a.factory
            return rc
        else:
            return False

    def areTopLevelChangesAuthorized(self, proposed: GitControlledObject, changeSource: Repository, tree: ValidationTree) -> bool:
        p: AuthorizedObjectManager[G, N] = cast(AuthorizedObjectManager[G, N], proposed)
        # If we are modified by an authorized source then it doesn't matter if its different or not
        if (self.owningRepo == changeSource):
            return True
        else:
            if (self.authorizedNames == p.authorizedNames):
                return True
            else:
                self.showDictChangesAsProblems(self.authorizedNames, p.authorizedNames, tree)
                return False

    def checkIfChangesAreAuthorized(self, proposed: GitControlledObject, changeSource: Repository, vTree: ValidationTree) -> None:
        proposedGZ: AuthorizedObjectManager[G, N] = cast(AuthorizedObjectManager[G, N], proposed)

        """This checks if the governance zone has changed relative to the specified change source"""
        """This checks if any teams have been added or removed relative to e"""

        self.checkTopLevelAttributeChangesAreAuthorized(proposedGZ, changeSource, vTree)

        # Get the current teams from the change source
        self.checkDictChangesAreAuthorized(self.authorizedObjects, proposedGZ.authorizedObjects, changeSource, vTree)

    def removeAuthorization(self, name: str) -> Optional[N]:
        """Removes the authorization from the list of authorized names"""
        r: Optional[N] = self.authorizedNames.pop(name)
        if (r and self.authorizedObjects.get(name) is not None):
            self.removeDefinition(name)
        return r

    def removeDefinition(self, name: str) -> Optional[G]:
        """Removes the object definition . This must be done by the object repo before the parent repo can remove the authorization"""
        r: Optional[G] = self.authorizedObjects.pop(name)
        return r

    def __str__(self) -> str:
        return f"AuthorizedObjectManager({self.name})"

    def lint(self, tree: ValidationTree):
        self.superLint(tree)


class TeamDeclaration(NamedObjectAuthorization):
    """This is a declaration of a team within a governance zone. It is used to authorize
    the team and to provide the official source of changes for that object and its children"""
    def __init__(self, name: str, authRepo: Repository) -> None:
        super().__init__(name, authRepo)
        self.authRepo: Repository = authRepo
        self.key: Optional[TeamDeclarationKey] = None

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TeamDeclaration) and self.authRepo == __value.authRepo and self.key == __value.key

    def setGovernanceZone(self, gz: 'GovernanceZone') -> None:
        """Sets the governance zone for this team and sets the team for all datastores and workspaces"""
        if gz.key:
            self.key = TeamDeclarationKey(gz.key, self.name)


class GovernanceZoneDeclaration(NamedObjectAuthorization):
    """This is a declaration of a governance zone within an ecosystem. It is used to authorize
    the definition of a governance zone and to provide the official source of changes for that object and its children"""
    def __init__(self, name: str, authRepo: Repository) -> None:
        super().__init__(name, authRepo)
        self.key: Optional[GovernanceZoneKey] = None

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, GovernanceZoneDeclaration) and self.key == __value.key


class GovernanceZone(GitControlledObject, JSONable):
    """This declares the existence of a specific GovernanceZone and defines the teams it manages, the storage policies
    and which repos can be used to pull changes for various metadata"""
    def __init__(self, name: str, ownerRepo: Repository, *args: Union['InfraStructureLocationPolicy', 'InfraStructureVendorPolicy',
                                                                      StoragePolicy, DataClassificationPolicy, TeamDeclaration,
                                                                      Documentation, DataPlatformPolicy, InfraHardVendorPolicy]) -> None:
        GitControlledObject.__init__(self, ownerRepo)
        JSONable.__init__(self)
        self.name: str = name
        self.key: Optional[GovernanceZoneKey] = None
        self.teams: AuthorizedObjectManager[Team, TeamDeclaration] = AuthorizedObjectManager[Team, TeamDeclaration](
            "teams", lambda name, repo: Team(name, repo), ownerRepo)

        self.storagePolicies: dict[str, StoragePolicy] = OrderedDict[str, StoragePolicy]()
        # Schemas for datasets defined in this GZ must comply with these classification restrictions
        self.classificationPolicies: dict[str, DataClassificationPolicy] = dict[str, DataClassificationPolicy]()
        # Only these vendors are allowed within this GZ (Datastores and Workspaces)
        self.vendorPolicies: dict[str, InfraStructureVendorPolicy] = dict[str, InfraStructureVendorPolicy]()
        # Only these locations are allowed within this GZ (Datastore and Workspaces)
        self.hardVendorPolicies: dict[str, InfraHardVendorPolicy] = dict[str, InfraHardVendorPolicy]()
        self.locationPolicies: dict[str, InfraStructureLocationPolicy] = dict[str, InfraStructureLocationPolicy]()
        self.dataplatformPolicies: dict[str, DataPlatformPolicy] = dict[str, DataPlatformPolicy]()

        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        teamKeys: list[Team] = self.teams.defineAllObjects()
        return {
            "name": self.name,
            "teams": {k: k.name for k in teamKeys},
            "storagePolicies": {k: v.to_json() for k, v in self.storagePolicies.items()},
            "classificationPolicies": {k: v.to_json() for k, v in self.classificationPolicies.items()},
            "vendorPolicies": {k: v.to_json() for k, v in self.vendorPolicies.items()},
            "hardVendorPolicies": {k: v.to_json() for k, v in self.hardVendorPolicies.items()},
            "locationPolicies": {k: v.to_json() for k, v in self.locationPolicies.items()},
            "dataplatformPolicies": {k: v.to_json() for k, v in self.dataplatformPolicies.items()},
        }

    def setEcosystem(self, eco: Ecosystem) -> None:
        """Sets the ecosystem for this zone and sets the zone for all teams"""
        self.key = GovernanceZoneKey(eco.key, self.name)

        self.add()

    def checkLocationIsAllowed(self, eco: 'Ecosystem', location: LocationKey, tree: ValidationTree):
        """This checks that the provided location is allowed based on the vendor and location policies
        of the GZ, this allows a GZ to constrain where its data can come from or be used"""
        loc: Optional[InfrastructureLocation] = location.getAsInfraLocation(eco)
        if (loc is None):
            tree.addRaw(UnknownLocationProblem(str(location), ProblemSeverity.ERROR))
            return
        for locPolicy in self.locationPolicies.values():
            if not locPolicy.isCompatible(location):
                tree.addRaw(ObjectNotCompatibleWithPolicy(loc, locPolicy, ProblemSeverity.ERROR))
        if (loc.key):
            v: InfrastructureVendor = eco.getVendorOrThrow(loc.key.ivName)
            for vendorPolicy in self.vendorPolicies.values():
                if not vendorPolicy.isCompatible(VendorKey(v.name)):
                    tree.addRaw(ObjectNotCompatibleWithPolicy(v, vendorPolicy, ProblemSeverity.ERROR))
            for hardVendorPolicy in self.hardVendorPolicies.values():
                if (v.hardCloudVendor is None):
                    tree.addRaw(AttributeNotSet(f"{loc} No hard cloud vendor"))
                elif not hardVendorPolicy.isCompatible(Literal(v.hardCloudVendor)):
                    tree.addRaw(ObjectNotCompatibleWithPolicy(v, hardVendorPolicy, ProblemSeverity.ERROR))
        else:
            tree.addRaw(AttributeNotSet("loc.key"))

    def add(self, *args: Union['InfraStructureVendorPolicy', 'InfraStructureLocationPolicy', StoragePolicy, DataClassificationPolicy,
                               TeamDeclaration, DataPlatformPolicy, Documentation, InfraHardVendorPolicy]) -> None:
        for arg in args:
            if (isinstance(arg, DataClassificationPolicy)):
                dcc: DataClassificationPolicy = arg
                self.classificationPolicies[dcc.name] = dcc
            elif (isinstance(arg, InfraStructureLocationPolicy)):
                self.locationPolicies[arg.name] = arg
            elif (isinstance(arg, InfraStructureVendorPolicy)):
                self.vendorPolicies[arg.name] = arg
            elif (isinstance(arg, StoragePolicy)):
                sp: StoragePolicy = arg
                if self.storagePolicies.get(sp.name) is not None:
                    raise Exception(f"Duplicate Storage Policy {sp.name}")
                self.storagePolicies[sp.name] = sp
            elif (type(arg) is TeamDeclaration):
                t: TeamDeclaration = arg
                self.teams.addAuthorization(t)
            elif (isinstance(arg, InfraHardVendorPolicy)):
                self.hardVendorPolicies[arg.name] = arg
            elif (isinstance(arg, DataPlatformPolicy)):
                self.dataplatformPolicies[arg. name] = arg
            elif (isinstance(arg, Documentation)):
                d: Documentation = arg
                self.documentation = d

        # Set softlink keys
        if (self.key):
            for sp in self.storagePolicies.values():
                sp.setGovernanceZone(self)
            for td in self.teams.authorizedNames.values():
                td.setGovernanceZone(self)

    def getTeam(self, name: str) -> Optional[Team]:
        return self.teams.getObject(name)

    def getTeamOrThrow(self, name: str) -> Team:
        t: Optional[Team] = self.getTeam(name)
        if (t is None):
            raise ObjectDoesntExistException(f"Unknown team {name}")
        return t

    def __eq__(self, other: object) -> bool:
        if isinstance(other, GovernanceZone):
            rc: bool = super().__eq__(other)
            rc = rc and self.name == other.name
            rc = rc and self.key == other.key
            rc = rc and self.dataplatformPolicies == other.dataplatformPolicies
            rc = rc and self.teams == other.teams
            rc = rc and self.classificationPolicies == other.classificationPolicies
            rc = rc and self.storagePolicies == other.storagePolicies
            rc = rc and self.vendorPolicies == other.vendorPolicies
            rc = rc and self.hardVendorPolicies == other.hardVendorPolicies
            rc = rc and self.locationPolicies == other.locationPolicies
            return rc
        return False

    def areTopLevelChangesAuthorized(self, proposed: GitControlledObject, changeSource: Repository, tree: ValidationTree) -> bool:
        """Just check the not git controlled attributes"""
        # If we're changed by an authorized source then it doesn't matter
        if (self.owningRepo == changeSource):
            return True
        if not (super().areTopLevelChangesAuthorized(proposed, changeSource, tree) and type(proposed) is GovernanceZone and self.name == proposed.name):
            return False
        if self._check_dict_changes(self.storagePolicies, proposed.storagePolicies, tree, "StoragePolicies"):
            return False
        if self._check_dict_changes(self.dataplatformPolicies, proposed.dataplatformPolicies, tree, "DataPlatformPolicies"):
            return False
        if self._check_dict_changes(self.vendorPolicies, proposed.vendorPolicies, tree, "VendorPolicies"):
            return False
        if self._check_dict_changes(self.locationPolicies, proposed.locationPolicies, tree, "LocationPolicies"):
            return False
        if not self.teams.areTopLevelChangesAuthorized(proposed.teams, changeSource, tree):
            return False
        return True

    def checkIfChangesAreAuthorized(self, proposed: GitControlledObject, changeSource: Repository, vTree: ValidationTree) -> None:
        proposedGZ: GovernanceZone = cast(GovernanceZone, proposed)

        """This checks if the governance zone has changed relative to the specified change source"""
        """This checks if any teams have been added or removed relative to e"""

        self.checkTopLevelAttributeChangesAreAuthorized(proposedGZ, changeSource, vTree)

        # Get the current teams from the change source
        self.teams.checkIfChangesAreAuthorized(proposedGZ.teams, changeSource, vTree)

    def lint(self, eco: Ecosystem, govTree: ValidationTree) -> None:
        """This validates a GovernanceZone and populates the teamcache with the zones teams"""

        # Make sure each Team is defined
        self.teams.defineAllObjects()

        for team in self.teams.authorizedObjects.values():
            td: TeamDeclaration = self.teams.authorizedNames[team.name]
            if (td.key is None):
                govTree.addRaw(AttributeNotSet(f"{td} has no key"))
            else:
                # Add Team to eco level cache, check for dup Teams and lint
                if (eco.teamCache.get(team.name) is not None):
                    govTree.addRaw(DuplicateObject(team, ProblemSeverity.ERROR))
                else:
                    eco.cache_addTeam(td, team)
                    teamTree: ValidationTree = govTree.addSubTree(team)
                    team.lint(eco, self, td, teamTree)
        self.superLint(govTree)
        self.teams.lint(govTree)
        if (self.key is None):
            govTree.addRaw(AttributeNotSet("Key not set"))

    def __str__(self) -> str:
        return f"GovernanceZone({self.name})"

    def checkForBackwardsCompatiblity(self, originZone: 'GovernanceZone', tree: ValidationTree):
        """This checks if this zone is backwards compatible with the original zone. This means that the proposed zone
        can be used to replace this one without breaking any data pipelines"""

        # Check if the teams are compatible
        for team in self.teams.authorizedObjects.values():
            tTree: ValidationTree = tree.addSubTree(team)
            originTeam: Optional[Team] = originZone.getTeam(team.name)
            # if team exists in old zone then check it, otherwise, it's a new team and we don't care
            if originTeam:
                team.checkForBackwardsCompatibility(originTeam, tTree)

    def getDatasetStoragePolicies(self, dataset: Dataset) -> Sequence[StoragePolicy]:
        """Returns the storage policies for the specified dataset including mandatory ones"""
        rc: list[StoragePolicy] = []
        rc.extend(dataset.policies.values())
        for sp in self.storagePolicies.values():
            if (sp.mandatory == PolicyMandatedRule.MANDATED_WITHIN_ZONE):
                rc.append(sp)
        return rc


@dataclass
class WorkspaceEntitlement:
    pass


@dataclass
class EventSink:
    pass


@dataclass
class Deliverable:
    pass


class DockerContainer:
    """This is a docker container which can be used to run some code such as a DataPlatform"""
    def __init__(self, name: str, image: str, version: str, cmd: str) -> None:
        self.name: str = name
        self.image: str = image
        self.version: str = version
        self.cmd: str = cmd

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, DockerContainer) and self.name == __value.name and self.image == __value.image and \
            self.version == __value.version and self.cmd == __value.cmd

    def __str__(self) -> str:
        return f"DockerContainer({self.name})"


class DataPlatformExecutor(UserDSLObject, JSONable):
    """This specifies how a DataPlatform should execute"""
    def __init__(self) -> None:
        UserDSLObject.__init__(self)
        JSONable.__init__(self)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DataPlatformExecutor)

    def __str__(self) -> str:
        return "DataPlatformExecutor()"

    @abstractmethod
    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass


class DataPlatformCICDExecutor(DataPlatformExecutor):
    """This is a DataPlatformExecutor for DataPlatforms which generate an IaC representation of the
    DataSurface intention graph and stores the generated IaC in a git repository. The IaC platform
    such as terraform or AWS cdk then checks for events on that repository and applies then changes
    from the repository when they are detected."""
    def __init__(self, repo: Repository) -> None:
        super().__init__()
        self.iacRepo: Repository = repo

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DataPlatformCICDExecutor) and self.iacRepo == other.iacRepo

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)
        self.iacRepo.lint(tree.addSubTree(self.iacRepo))

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "iacRepo": self.iacRepo.to_json(),
        }


class DataPlatform(Documentable, UserDSLObject, JSONable):
    """This is a system which can interpret data flows in the metadata and realize those flows"""
    def __init__(self, name: str, *args: Union[CredentialStore, DataPlatformExecutor, Documentation]) -> None:
        Documentable.__init__(self, self.findObjectOfSpecificType(Documentation, *args))
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.name: str = name
        de: Optional[DataPlatformExecutor] = self.findObjectOfSpecificType(DataPlatformExecutor, *args)
        if de is None:
            raise ObjectDoesntExistException(f"Could not find object of type {DataPlatformExecutor}")
        self.executor: DataPlatformExecutor = de
        self.credentialStores: dict[str, CredentialStore] = OrderedDict[str, CredentialStore]()
        self.add(*args)

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {
            "_type": self.__class__.__name__,
            "name": self.name,
            "executor": self.executor.to_json(),
            "credentialStores": {k: v.to_json() for k, v in self.credentialStores.items()},
        }
        return rc

    def add(self, *args: Union[CredentialStore, DataPlatformExecutor, Documentation]) -> None:
        for arg in args:
            if isinstance(arg, CredentialStore):
                cs: CredentialStore = arg
                self.credentialStores[cs.name] = cs
            elif isinstance(arg, DataPlatformExecutor):
                if self.executor != arg:
                    raise ObjectAlreadyExistsException("Executor already set")
                self.executor = arg
            else:
                self.documentation = arg

    def findObjectOfSpecificType(self, t: Type[T], *args: Any) -> Optional[T]:
        """Finds an object of a specific type in the provided arguments."""
        for arg in args:
            if isinstance(arg, t):
                return arg
        return None

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DataPlatform) and self.name == other.name and self.executor == other.executor and Documentable.__eq__(self, other)

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    @abstractmethod
    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        pass

    @abstractmethod
    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        pass

    @abstractmethod
    def lint(self, eco: Ecosystem, tree: ValidationTree):
        if (self.documentation):
            self.documentation.lint(tree)
        self.executor.lint(eco, tree.addSubTree(self.executor))
        if (not eco.checkDataPlatformExists(self)):
            tree.addRaw(ValidationProblem(f"DataPlatform {self} not found in ecosystem {eco}", ProblemSeverity.ERROR))
        for cs in self.credentialStores.values():
            cs.lint(eco, tree.addSubTree(cs))

    @abstractmethod
    def createGraphHandler(self, graph: 'PlatformPipelineGraph') -> 'DataPlatformGraphHandler':
        pass


class DataPlatformKey(JSONable):
    """This is a named reference to a DataPlatform. This allows a DataPlatform to be specified and
    resolved later at lint time."""
    def __init__(self, name: str) -> None:
        JSONable.__init__(self)
        self.name: str = name
        self.platform: Optional[DataPlatform] = None

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "name": self.name}

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        if (not self.name):
            tree.addRaw(AttributeNotSet("name"))
        else:
            self.platform = eco.getDataPlatform(self.name)
            if (self.platform is None):
                tree.addRaw(ValidationProblem(f"Unknown DataPlatform {self.name}", ProblemSeverity.ERROR))

    def __eq__(self, value: object) -> bool:
        return isinstance(value, DataPlatformKey) and self.name == value.name

    def __hash__(self) -> int:
        return hash(self.name)


class DataLatency(Enum):
    """Specifies the acceptable latency range from a consumer"""
    SECONDS = 0
    """Up to 59 seconds"""
    MINUTES = 1
    """Up to 59 minutes"""
    HOURS = 3
    """Up to 24 hours"""
    DAYS = 4
    """A day or more"""


class DataRetentionPolicy(Enum):
    """Client indicates whether the data is live or forensic"""
    LIVE_ONLY = 0
    """Only the latest version of each record should be retained"""
    FORENSIC = 1
    """All versions of every record in every table used to produce the datasets should be retained and present in the consumer data tables"""
    LIVE_WITH_FORENSIC_HISTORY = 2
    """All versions of each record should be retained BUT only latest records are needed in the consumer data tables"""


# This needs to be keyed and rolled up to manage definitions centrally, there should
# be a common ESMA definition for example (5 years forensic)
class ConsumerRetentionRequirements:
    """Consumers specify the retention requirements for the data they consume. Platforms use this to backtrack
    retention requirements for data in the full inferred pipeline to manage that consumer"""
    def __init__(self, r: DataRetentionPolicy, latency: DataLatency, regulator: Optional[str],
                 minRetentionDurationIfNeeded: Optional[timedelta] = None) -> None:
        self.policy: DataRetentionPolicy = r
        self.latency: DataLatency = latency
        self.minRetentionTime: Optional[timedelta] = minRetentionDurationIfNeeded
        self.regulator: Optional[str] = regulator

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

    def __hash__(self) -> int:
        return hash((self.policy, self.latency, self.minRetentionTime, self.regulator))

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "policy": self.policy.name, "latency": self.latency.name,
                "minRetentionTime": self.minRetentionTime, "regulator": self.regulator}


class DataPlatformChooser(ABC):
    """Subclasses of this choose a DataPlatform to render the pipeline for moving data from a producer to a Workspace possibly
    through intermediate Workspaces"""
    def __init__(self):
        pass

    @abstractmethod
    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__}


class WorkspacePlatformConfig(DataPlatformChooser):
    """This allows a Workspace to specify per pipeline hints for behavior, i.e.
    allowed latency and so on"""
    def __init__(self, hist: ConsumerRetentionRequirements) -> None:
        self.retention: ConsumerRetentionRequirements = hist

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        """For now, just return default"""
        # TODO This should evaluate the parameters provide and choose the 'best' DataPlatform
        return eco.getDefaultDataPlatform()

    def __str__(self) -> str:
        return f"WorkspacePlatformConfig({self.retention})"

    def __hash__(self) -> int:
        return hash(self.retention)

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "retention": self.retention.to_json()}


class WorkspaceFixedDataPlatform(DataPlatformChooser):
    """This specifies a fixed DataPlatform for a Workspace"""
    def __init__(self, dp: DataPlatformKey):
        self.dataPlatform: DataPlatformKey = dp

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, WorkspaceFixedDataPlatform) and self.dataPlatform == o.dataPlatform

    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        return eco.getDataPlatform(self.dataPlatform.name)

    def __str__(self) -> str:
        return f"WorkspaceFixedDataPlatform({self.dataPlatform})"

    def __hash__(self) -> int:
        return hash(self.dataPlatform)

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "dataPlatform": self.dataPlatform.name}


class DeprecationsAllowed(Enum):
    """This specifies if deprecations are allowed for a specific dataset in a workspace dsg"""
    NEVER = 0
    """Deprecations are never allowed"""
    ALLOWED = 1
    """Deprecations are allowed but not will generate warnings"""


class DatasetSink(UserDSLObject):
    """This is a reference to a dataset in a Workspace"""
    def __init__(self, storeName: str, datasetName: str, deprecationsAllowed: DeprecationsAllowed = DeprecationsAllowed.NEVER) -> None:
        UserDSLObject.__init__(self)
        self.storeName: str = storeName
        self.datasetName: str = datasetName
        self.key = f"{self.storeName}:{self.datasetName}"
        self.deprecationsAllowed: DeprecationsAllowed = deprecationsAllowed

    def to_json(self) -> dict[str, Any]:
        json_dict: dict[str, Any] = {
            "storeName": self.storeName,
            "datasetName": self.datasetName,
            "deprecationsAllowed": self.deprecationsAllowed.name
        }
        return json_dict

    def __eq__(self, other: object) -> bool:
        if (type(other) is DatasetSink):
            return self.key == other.key and self.storeName == other.storeName and self.datasetName == other.datasetName
        else:
            return False

    def __hash__(self) -> int:
        return hash(f"{self.storeName}/{self.datasetName}")

    def lint(self, eco: Ecosystem, team: Team, ws: 'Workspace', tree: ValidationTree):
        """Check the DatasetSink meets all policy checks"""
        if not is_valid_sql_identifier(self.storeName):
            tree.addRaw(NameMustBeSQLIdentifier(f"DatasetSink store name {self.storeName}", ProblemSeverity.ERROR))
        if not is_valid_sql_identifier(self.datasetName):
            tree.addRaw(NameMustBeSQLIdentifier(f"DatasetSink dataset name {self.datasetName}", ProblemSeverity.ERROR))
        dataset: Optional[Dataset] = eco.cache_getDataset(self.storeName, self.datasetName)
        if (dataset is None):
            tree.addRaw(ConstraintViolation(f"Unknown dataset {self.storeName}:{self.datasetName}", ProblemSeverity.ERROR))
        else:
            storeI: Optional[DatastoreCacheEntry] = eco.datastoreCache.get(self.storeName)
            if storeI:
                store: Datastore = storeI.datastore
                # Check Workspace dataContainer locations are compatible with the Datastore gz policies
                if (store.key):
                    gzStore: GovernanceZone = eco.getZoneOrThrow(store.key.gzName)
                    if (ws.dataContainer):
                        ws.dataContainer.lint(eco, tree)
                        for loc in ws.dataContainer.locations:
                            gzStore.checkLocationIsAllowed(eco, loc, tree)
                else:
                    tree.addRaw(AttributeNotSet(f"{store} key is None"))

                # Production data in non production or vice versa should be noted
                if (store.productionStatus != ws.productionStatus):
                    tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} is using a datastore with a different production status",
                                    ProblemSeverity.WARNING)
                if store.isDatasetDeprecated(dataset):
                    if self.deprecationsAllowed == DeprecationsAllowed.NEVER:
                        tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} is deprecated and deprecations are not allowed")
                    elif (self.deprecationsAllowed == DeprecationsAllowed.ALLOWED):
                        tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} is using deprecated dataset", ProblemSeverity.WARNING)
                dataset: Optional[Dataset] = store.datasets.get(self.datasetName)
                if (dataset is None):
                    tree.addRaw(UnknownObjectReference(f"Unknown dataset {self.storeName}:{self.datasetName}", ProblemSeverity.ERROR))
                else:
                    if (ws.classificationVerifier and not dataset.checkClassificationsAreOnly(ws.classificationVerifier)):
                        tree.addRaw(ObjectNotCompatibleWithPolicy(self, ws.classificationVerifier, ProblemSeverity.ERROR))
            else:
                tree.addRaw(UnknownObjectReference(f"Datastore {self.storeName}", ProblemSeverity.ERROR))

    def __str__(self) -> str:
        return f"DatasetSink({self.storeName}:{self.datasetName})"


class DatasetGroup(ANSI_SQL_NamedObject, Documentable):
    """A collection of Datasets which are rendered with a specific pipeline spec in a Workspace. The name should be
    ANSI SQL compliant because it could be used as part of a SQL View/Table name in a Workspace database"""
    def __init__(self, name: str, *args: Union[DatasetSink, DataPlatformChooser, Documentation]) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        self.platformMD: Optional[DataPlatformChooser] = None
        self.sinks: dict[str, DatasetSink] = OrderedDict[str, DatasetSink]()
        for arg in args:
            if (type(arg) is DatasetSink):
                sink: DatasetSink = arg
                if (self.sinks.get(sink.key) is not None):
                    raise ObjectAlreadyExistsException(f"Duplicate DatasetSink {sink.key}")
                self.sinks[sink.key] = sink
            elif (isinstance(arg, Documentation)):
                self.documentation = arg
            elif (isinstance(arg, DataPlatformChooser)):
                if self.platformMD is None:
                    self.platformMD = arg
                else:
                    raise AttributeAlreadySetException("Platform")
            else:
                raise UnknownArgumentException(f"Unknown argument {type(arg)}")

    def to_json(self) -> dict[str, Any]:
        json_dict: dict[str, Any] = {
            "name": self.name,
            "sinks": {sink.key: sink.to_json() for sink in self.sinks.values()},
            "platformMD": self.platformMD.to_json() if self.platformMD else None
        }
        return json_dict

    def __eq__(self, other: object) -> bool:
        return cyclic_safe_eq(self, other, set())

    def __hash__(self) -> int:
        return hash((self.name, tuple(self.sinks.items()), self.platformMD))

    def lint(self, eco: Ecosystem, team: Team, ws: 'Workspace', tree: ValidationTree):
        super().nameLint(tree)
        if (self.documentation):
            self.documentation.lint(tree)
        if not is_valid_sql_identifier(self.name):
            tree.addRaw(NameMustBeSQLIdentifier(f"DatasetGroup name {self.name}", ProblemSeverity.ERROR))
        for sink in self.sinks.values():
            sinkTree: ValidationTree = tree.addSubTree(sink)
            sink.lint(eco, team, ws, sinkTree)
        if (self.platformMD):
            # PlatformChooser needs to choose a platform and that platform must perfectly match the same named platform in the Ecosystem
            platform: Optional[DataPlatform] = self.platformMD.choooseDataPlatform(eco)
            if (platform is None):
                tree.addProblem("DSG doesnt choose a dataplatform")
            else:
                ecoPlat: Optional[DataPlatform] = eco.dataPlatforms.get(platform.name)
                if (ecoPlat is None):
                    tree.addRaw(UnknownObjectReference(f"DataPlatform {platform.name} is not in the Ecosystem", ProblemSeverity.ERROR))
                else:
                    if (ecoPlat != platform):
                        tree.addProblem("DSG chooses a platform which is different from the same named platform in the Ecosystem", ProblemSeverity.ERROR)
                # Now check the platform is happy with the containers for the Workspace
                if (ws.dataContainer):
                    ws.dataContainer.lint(eco, tree)
                    if (not platform.isContainerSupported(eco, ws.dataContainer)):
                        tree.addProblem(f"DataPlatform {platform.name} does not support the Workspace data container {ws.dataContainer.name}",
                                        ProblemSeverity.ERROR)
        else:
            tree.addRaw(AttributeNotSet("DSG has no data platform chooser"))
        if (len(self.sinks) == 0):
            tree.addRaw(AttributeNotSet("No datasetsinks in group"))

    def __str__(self) -> str:
        return f"DatasetGroup({self.name})"


class TransformerTrigger(JSONable):
    def __init__(self, name: str):
        JSONable.__init__(self)
        self.name: str = name

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "name": self.name}

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def __eq__(self, o: object) -> bool:
        return isinstance(o, TransformerTrigger) and self.name == o.name


class TimedTransformerTrigger(TransformerTrigger):
    def __init__(self, name: str, transformerTrigger: StepTrigger):
        super().__init__(name)
        self.trigger: StepTrigger = transformerTrigger

    def __eq__(self, o: object) -> bool:
        return isinstance(o, TimedTransformerTrigger) and self.trigger == o.trigger and super().__eq__(o)


class CodeArtifact(JSONable):
    """This defines a piece of code which can be used to transform data in a workspace"""

    def __init__(self):
        JSONable.__init__(self)

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__}

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        pass

    def __eq__(self, o: object) -> bool:
        return isinstance(o, CodeArtifact)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class PythonCodeArtifact(CodeArtifact):
    """This describes a python job and its dependencies"""
    def __init__(self, requirements: list[str], envVars: dict[str, str], requiredVersion: str) -> None:
        super().__init__()
        self.requirements: list[str] = requirements
        self.envVars: dict[str, str] = envVars
        self.requiredVersion: str = requiredVersion

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "requirements": self.requirements,
                "envVars": self.envVars,
                "requiredVersion": self.requiredVersion
            }
        )
        return rc

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        # TODO more
        pass

    def __eq__(self, o: object) -> bool:
        if isinstance(o, PythonCodeArtifact):
            rc: bool = self.requiredVersion == o.requiredVersion
            rc = rc and self.requirements == o.requirements
            rc = rc and self.envVars == o.envVars
            return rc
        else:
            return False


class CodeExecutionEnvironment(UserDSLObject, JSONable):
    """This is an environment which can execute code, AWS Lambda, Azure Functions, Kubernetes, etc"""
    def __init__(self, loc: set[LocationKey]):
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.location: set[LocationKey] = loc

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "locations": [loc.to_json() for loc in self.location]}

    def __eq__(self, o: object) -> bool:
        return isinstance(o, CodeExecutionEnvironment) and self.location == o.location

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        for loc in self.location:
            loc.lint(eco, tree)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.location})"

    @abstractmethod
    def isCodeArtifactSupported(self, eco: 'Ecosystem', ca: CodeArtifact) -> bool:
        """This checks if the code artifact can be run in this environment"""
        return False


class KubernetesEnvironment(CodeExecutionEnvironment):
    """This is a Kubernetes environment"""
    def __init__(self, hostName: str, cred: Credential, loc: set[LocationKey]) -> None:
        super().__init__(loc)
        self.hostName: str = hostName
        """This is the hostname of the Kubernetes cluster"""
        self.credential: Credential = cred
        """This is the credential used to access the Kubernetes cluster"""

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "hostName": self.hostName,
                "credential": self.credential.to_json()
            }
        )
        return rc

    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        super().lint(eco, tree)
        if not is_valid_hostname_or_ip(self.hostName):
            tree.addRaw(NameHasBadSynthax(f"Invalid host name <{self.hostName}>"))
        cTree: ValidationTree = tree.addSubTree(self.credential)
        self.credential.lint(eco, cTree)

    def isCodeArtifactSupported(self, eco: Ecosystem, ca: CodeArtifact) -> bool:
        return isinstance(ca, PythonCodeArtifact)


class DataTransformer(ANSI_SQL_NamedObject, Documentable, JSONable):
    """This allows new data to be produced from existing data. The inputs to the transformer are the
    datasets in the workspace and the output is a Datastore associated with the transformer. The transformer
    will be triggered using the specified trigger policy"""
    def __init__(self, name: str, store: Datastore, trigger: TransformerTrigger, code: CodeArtifact,
                 codeEnv: CodeExecutionEnvironment, doc: Optional[Documentation] = None) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        JSONable.__init__(self)
        # This Datastore is defined here and has a CaptureMetaData automatically added. Do not specify a CMD in the Datastore
        # This is done in the Team.addWorkspace method
        self.outputDatastore: Datastore = store
        self.trigger: TransformerTrigger = trigger
        self.code: CodeArtifact = code
        self.codeEnv: CodeExecutionEnvironment = codeEnv
        self.documentation = doc

    def to_json(self) -> dict[str, Any]:
        json_dict: dict[str, Any] = {
            "name": self.name,
            "outputDatastore": self.outputDatastore.to_json(),
            "trigger": self.trigger.to_json(),
            "code": self.code.to_json(),
            "codeEnv": self.codeEnv.to_json(),
        }
        if self.documentation:
            json_dict["documentation"] = self.documentation.to_json()
        return json_dict

    def lint(self, eco: Ecosystem, ws: 'Workspace', tree: ValidationTree):
        ANSI_SQL_NamedObject.nameLint(self, tree)
        if (self.documentation):
            self.documentation.lint(tree)
        # Does store exist
        storeI: Optional[DatastoreCacheEntry] = eco.datastoreCache.get(self.outputDatastore.name)
        if (storeI is None):
            tree.addRaw(UnknownObjectReference(f"datastore {self.outputDatastore.name}", ProblemSeverity.ERROR))
        else:
            if (storeI.datastore.productionStatus != ws.productionStatus):
                tree.addRaw(ConstraintViolation(f"DataTransformer {self.name} is using a datastore with a different production status",
                                                ProblemSeverity.WARNING))

            workSpaceI: WorkspaceCacheEntry = eco.cache_getWorkspaceOrThrow(ws.name)
            if (workSpaceI.team != storeI.team):
                tree.addRaw(ConstraintViolation(f"DataTransformer {self.name} is using a datastore from a different team", ProblemSeverity.ERROR))
        codeEnvTree: ValidationTree = tree.addSubTree(self.codeEnv)
        self.codeEnv.lint(eco, codeEnvTree)
        codeTree: ValidationTree = tree.addSubTree(self.codeEnv)
        self.code.lint(eco, codeTree)

        # Check the code artifact can be executed by the code execution environment
        if not self.codeEnv.isCodeArtifactSupported(eco, self.code):
            codeEnvTree.addRaw(ValidationProblem(f"CodeArtifact {self.code} is not supported in the CodeExecutionEnvironment {self.codeEnv}",
                                                 ProblemSeverity.ERROR))

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, DataTransformer) and self.name == o.name and self.outputDatastore == o.outputDatastore and \
            self.trigger == o.trigger and self.code == o.code and self.codeEnv == o.codeEnv


class WorkloadTier(Enum):
    """This is a relative priority of a Workspace against other Workspaces. This priority propogates backwards to producers whose data a Workspace
    uses. Thus, producers don't set the priority of their data, it's determined by the priority of whose is using it."""
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3
    UNKNOWN = 4


class WorkspacePriority(UserDSLObject, ABC):

    """This is a relative priority of a Workspace against other Workspaces. This priority propogates backwards to producers whose data a Workspace
    uses. Thus, producers don't set the priority of their data, it's determined by the priority of whose is using it."""
    def __init__(self):
        super().__init__()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

    @abstractmethod
    def isMoreImportantThan(self, other: 'WorkspacePriority') -> bool:
        """This checks if this priority is more important than the other priority"""
        return False


class PrioritizedWorkloadTier(WorkspacePriority):
    """This uses a simple enum to determine priority"""
    def __init__(self, priority: WorkloadTier):
        super().__init__()
        self.priority: WorkloadTier = priority

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.priority})"

    def isMoreImportantThan(self, other: 'WorkspacePriority') -> bool:
        if isinstance(other, PrioritizedWorkloadTier):
            return self.priority.value < other.priority.value
        else:
            return super().isMoreImportantThan(other)


class Workspace(ANSI_SQL_NamedObject, Documentable, JSONable):
    """A collection of datasets used by a consumer for a specific use case. This consists of one or more groups of datasets with each set using
    the correct pipeline spec.
    Specific datasets can be present in multiple groups. They will be named differently in each group. The name needs to be ANSI SQL because
    it could be used as part of a SQL View/Table name in a Workspace database. Workspaces must have ecosystem unique names"""
    def __init__(self, name: str, *args: Union[DatasetGroup, DataContainer, Documentation, DataClassificationPolicy, ProductionStatus,
                                               DeprecationInfo, DataTransformer, WorkspacePriority]) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        JSONable.__init__(self)
        self.priority: WorkspacePriority = PrioritizedWorkloadTier(WorkloadTier.UNKNOWN)
        self.dsgs: dict[str, DatasetGroup] = OrderedDict[str, DatasetGroup]()
        self.dataContainer: Optional[DataContainer] = None
        self.productionStatus: ProductionStatus = ProductionStatus.NOT_PRODUCTION
        self.deprecationStatus: DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)
        self.dataTransformer: Optional[DataTransformer] = None
        # This is the set of classifications expected in the Workspace. Linting fails
        # if any datsets/attributes found with classifications different than these
        self.classificationVerifier: Optional[DataClassificationPolicy] = None
        self.key: Optional[WorkspaceKey] = None
        """This workspace is the input to a data transformer if set"""
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        json_dict: dict[str, Any] = {
            "name": self.name,
            "datasetGroups": {name: dsg.to_json() for name, dsg in self.dsgs.items()},
            "productionStatus": self.productionStatus.name,

            "deprecationStatus": {
                "status": self.deprecationStatus.status.name,
                "documentation": self.deprecationStatus.documentation.to_json() if self.deprecationStatus.documentation else None
            }
        }
        if self.dataTransformer:
            json_dict["dataTransformer"] = self.dataTransformer.to_json()
        return json_dict

    def setTeam(self, key: TeamDeclarationKey):
        self.key = WorkspaceKey(key, self.name)

    def add(self, *args: Union[DatasetGroup, DataContainer, Documentation, DataClassificationPolicy, ProductionStatus,
                               DeprecationInfo, DataTransformer, WorkspacePriority]):
        for arg in args:
            if (isinstance(arg, WorkspacePriority)):
                self.priority = arg
            elif (isinstance(arg, DatasetGroup)):
                if (self.dsgs.get(arg.name) is not None):
                    raise ObjectAlreadyExistsException(f"Duplicate DatasetGroup {arg.name}")
                self.dsgs[arg.name] = arg
            elif (isinstance(arg, DataClassificationPolicy)):
                self.classificationVerifier = arg
            elif (isinstance(arg, DataContainer)):
                if (self.dataContainer is not None and self.dataContainer != arg):
                    raise AttributeAlreadySetException("dataContainer")
                self.dataContainer = arg
            elif (isinstance(arg, ProductionStatus)):
                self.productionStatus = arg
            elif (isinstance(arg, DeprecationInfo)):
                self.deprecationStatus = arg
            elif (isinstance(arg, Documentation)):
                if (self.documentation is not None and self.documentation != arg):
                    raise AttributeAlreadySetException("Documentation")
                self.documentation = arg
            else:
                if (self.dataTransformer is not None and self.dataTransformer != arg):
                    raise AttributeAlreadySetException("DataTransformer")
                self.dataTransformer = arg

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        return cyclic_safe_eq(self, other, set())

    def isDatastoreUsed(self, store: Datastore) -> bool:
        """Returns true if the specified datastore is used by this workspace"""
        for dsg in self.dsgs.values():
            for sink in dsg.sinks.values():
                if sink.storeName == store.name:
                    return True
        return False

    def lint(self, eco: Ecosystem, gz: GovernanceZone, t: Team, tree: ValidationTree):
        super().nameLint(tree)

        if (self.key is None):
            tree.addRaw(AttributeNotSet("Workspace key is none"))

        # Check Workspaces in this gz are on dataContainers compatible with vendor
        # and location policies for this GZ
        if (self.dataContainer):
            cntTree: ValidationTree = tree.addSubTree(self.dataContainer)
            self.dataContainer.lint(eco, cntTree)

        # Check production status of workspace matches all datasets in use
        # Check deprecation status of workspace generates warnings for all datasets in use
        # Lint the DSGs
        for dsg in self.dsgs.values():
            dsgTree: ValidationTree = tree.addSubTree(dsg)
            dsg.lint(eco, t, self, dsgTree)

        # Link the transformer if present
        if self.dataTransformer:
            dtTree: ValidationTree = tree.addSubTree(self.dataTransformer)
            self.dataTransformer.lint(eco, self, dtTree)

    def __str__(self) -> str:
        return f"Workspace({self.name})"


class PlatformStyle(Enum):
    OLTP = 0
    OLAP = 1
    COLUMNAR = 2
    OBJECT = 3


class PipelineNode(InternalLintableObject):
    """This is a named node in the pipeline graph. It stores node common information and which nodes this node depends on and those that depend on this node"""
    def __init__(self, name: str, platform: DataPlatform):
        InternalLintableObject.__init__(self)
        self.name: str = name
        self.platform: DataPlatform = platform
        # This node depends on this set of nodes
        self.leftHandNodes: dict[str, PipelineNode] = dict()
        # This set of nodes depend on this node
        self.rightHandNodes: dict[str, PipelineNode] = dict()
        self.priority: Optional[WorkspacePriority] = None

    def __str__(self) -> str:
        return f"{self.__class__.__name__}/{self.name}"

    def __eq__(self, o: object) -> bool:
        return isinstance(o, PipelineNode) and self.name == o.name and self.leftHandNodes == o.leftHandNodes and self.rightHandNodes == o.rightHandNodes

    def addRightHandNode(self, rhNode: 'PipelineNode'):
        """This records a node that depends on this node"""
        self.rightHandNodes[str(rhNode)] = rhNode
        rhNode.leftHandNodes[str(self)] = self

    def setPriority(self, proposedPriority: WorkspacePriority):
        """This sets the priority of this node. If the proposed priority is more important than the current priority then it is set."""
        if (self.priority is not None):
            if (not self.priority.isMoreImportantThan(proposedPriority)):
                self.priority = proposedPriority
        else:
            self.priority = proposedPriority


class ExportNode(PipelineNode):
    """This is a node which represents the export of a dataset from a Datastore to a DataContainer. The dataset data is then
    available to the consumer which owns the Workspace associated with the DataContainer."""
    def __init__(self, platform: DataPlatform, dataContainer: DataContainer, storeName: str, datasetName: str):
        super().__init__(f"Export/{platform.name}/{dataContainer.name}/{storeName}/{datasetName}", platform)
        self.dataContainer: DataContainer = dataContainer
        self.storeName: str = storeName
        self.datasetName: str = datasetName

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, ExportNode) and self.dataContainer == o.dataContainer and \
            self.storeName == o.storeName and self.datasetName == o.datasetName


class IngestionNode(PipelineNode):
    """This is a super class node for ingestion nodes. It represents an ingestion stream source for a pipeline."""
    def __init__(self, name: str, platform: DataPlatform, storeName: str, captureTrigger: Optional[StepTrigger]):
        super().__init__(name, platform)
        self.storeName: str = storeName
        self.captureTrigger: Optional[StepTrigger] = captureTrigger

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, IngestionNode) and self.storeName == o.storeName and self.captureTrigger == o.captureTrigger


class IngestionMultiNode(IngestionNode):
    """This is a node which represents the ingestion of multiple datasets from a Datastore. Such as Datastore might have N datasets and
    all N datasets are ingested together, transactionally in to a pipeline graph."""
    def __init__(self, platform: DataPlatform, storeName: str, captureTrigger: Optional[StepTrigger]):
        super().__init__(f"Ingest/{platform.name}/{storeName}", platform, storeName, captureTrigger)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, IngestionMultiNode)


class IngestionSingleNode(IngestionNode):
    """This is a node which represents the ingestion of a single dataset from a Datastore. Such as Datastore might have N datasets
    and each of the datasets is ingested independently in to the pipeline. This node represents the ingestion of a single dataset"""
    def __init__(self, platform: DataPlatform, storeName: str, dataset: str, captureTrigger: Optional[StepTrigger]):
        super().__init__(f"Ingest/{platform.name}/{storeName}/{dataset}", platform, storeName, captureTrigger)
        self.datasetName: str = dataset

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, IngestionSingleNode) and self.datasetName == o.datasetName


class TriggerNode(PipelineNode):
    """This is a node which represents the trigger for a DataTransformer. The trigger is a join on all the exports to a single Workspace."""
    def __init__(self, w: Workspace, platform: DataPlatform):
        super().__init__(f"Trigger{w.name}", platform)
        self.workspace: Workspace = w

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, TriggerNode) and self.workspace == o.workspace


class DataTransformerNode(PipelineNode):
    """This is a node which represents the execution of a DataTransformer in the pipeline graph. The Datatransformer
    should 'execute' and its outputs can be found in the output Datastore for the datatransformer."""
    def __init__(self, ws: Workspace, platform: DataPlatform):
        super().__init__(f"DataTransfomer/{ws.name}", platform)
        self.workspace: Workspace = ws

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, DataTransformerNode) and self.workspace == o.workspace


class DSGRootNode:
    """This represents a target for a DataPlatform. A DataPlatforms purpose is to hydrated and maintain
    the datasets for a DatasetGroup. A Workspace owns one or more DatasetGroups and all datasets used in
    its DatasetGroups must be exported to the DataContainer used by the Workspace."""
    def __init__(self, w: Workspace, dsg: DatasetGroup):
        self.workspace: Workspace = w
        self.dsg: DatasetGroup = dsg

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, o: object) -> bool:
        return isinstance(o, DSGRootNode) and self.workspace == o.workspace and self.dsg == o.dsg

    def __str__(self) -> str:
        return f"{self.workspace.name}/{self.dsg.name}"


class PlatformPipelineGraph(InternalLintableObject):
    """This should be all the information a DataPlatform needs to render the processing pipeline graph. This would include
    provisioning Workspace views, provisioning dataContainer tables. Exporting data to dataContainer tables. Ingesting data from datastores,
    executing data transformers. We always build the graph starting on the right hand side, the consumer side which is typically a
    Workspace. We work left-wards towards data producers using the datasets used in DatasetGroups in the Workspace. Any datasets
    used by a Workspace need to have exports to the Workspace for those datasets. All datasets exported must also have been
    ingested. So we add an ingestion step. If a dataset is produced by a DataTransformer then we need to have the ingestion
    triggered by the execution of the DataTransformer. The DataTransformer is triggered itself by exports to the Workspace which
    owns it.
    Thus the right hand side of the graph should all be Exports to Workspaces. The left hand side should be
    all ingestion steps. Every ingestion should have a right hand side node which are exports to Workspaces.
    Exports will have a trigger for each dataset used by a DataTransformer. The trigger will be a join on all the exports to
    a single Workspace and will always trigger a DataTransformer node. The Datatransformer node will have an ingestion
    node for its output Datastore and then that Datastore should be exported to the Workspaces which use that Datastore.

    The priority of each node should be the highest priority of the Workspaces which depend on it."""

    def __init__(self, eco: Ecosystem, platform: DataPlatform):
        InternalLintableObject.__init__(self)
        self.platform: DataPlatform = platform
        self.eco: Ecosystem = eco
        self.workspaces: dict[str, Workspace] = dict()
        # All DSGs per Platform
        self.roots: set[DSGRootNode] = set()
        # This tracks which DatasetGroups are consumers of a DataContainer. This is necessary because
        # The DataPlatform may need to create view objects for each DatasetSink in the DataContainer
        # pointed at the underlying raw table
        self.dataContainerConsumers: dict[DataContainer, set[tuple[Workspace, DatasetGroup]]] = dict()

        # These are all the datastores used in the pipelinegraph for this platform. Note, this may be
        # a subset of the datastores in total in the ecosystem
        self.storesToIngest: set[str] = set()

        # This is the set of ALL nodes in this platforms pipeline graph
        self.nodes: dict[str, PipelineNode] = dict()

    def __str__(self) -> str:
        return f"PlatformPipelineGraph({self.platform.name})"

    def generateGraph(self):
        """This generates the pipeline graph for the platform. This is a directed graph with nodes representing
        ingestion, export, trigger, and data transformation operations"""
        self.dataContainerConsumers = dict()
        self.storesToIngest = set()

        # Split DSGs by Asset hosting Workspaces
        for dsg in self.roots:
            if dsg.workspace.dataContainer:
                dataContainer: DataContainer = dsg.workspace.dataContainer
                if self.dataContainerConsumers.get(dataContainer) is None:
                    self.dataContainerConsumers[dataContainer] = set()
                self.dataContainerConsumers[dataContainer].add((dsg.workspace, dsg.dsg))

        # Now collect stores to ingest per platform
        for consumers in self.dataContainerConsumers.values():
            for _, dsg in consumers:
                for sink in dsg.sinks.values():
                    self.storesToIngest.add(sink.storeName)

        # Make ingestion steps for every store used by platform
        for store in self.storesToIngest:
            self.createIngestionStep(store)

        # Now build pipeline graph backwards from workspaces used by platform and stores used by platform
        for dataContainer, consumers in self.dataContainerConsumers.items():
            for _, dsg in consumers:
                for sink in dsg.sinks.values():
                    exportStep: PipelineNode = ExportNode(self.platform, dataContainer, sink.storeName, sink.datasetName)
                    # If export doesn't already exist then create and add to ingestion job
                    if (self.nodes.get(str(exportStep)) is None):
                        self.nodes[str(exportStep)] = exportStep
                        self.addExportToPriorIngestion(exportStep)

    def findExistingOrCreateStep(self, step: PipelineNode) -> PipelineNode:
        """This finds an existing step or adds it to the set of steps in the graph"""
        if self.nodes.get(str(step)) is None:
            self.nodes[str(step)] = step
        else:
            step = self.nodes[str(step)]
        return step

    def createIngestionStep(self, storeName: str):
        """This creates a step to ingest data for a datastore. This results in either a single step for a multi-dataset store
        or one step per dataset in the single dataset stores"""
        store: Datastore = self.eco.cache_getDatastoreOrThrow(storeName).datastore

        if store.cmd:
            if (store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET):
                for datasetName in store.datasets.keys():
                    self.findExistingOrCreateStep(IngestionSingleNode(self.platform, storeName, datasetName, store.cmd.stepTrigger))
            else:  # MULTI_DATASET
                self.findExistingOrCreateStep(IngestionMultiNode(self.platform, storeName, store.cmd.stepTrigger))
        else:
            raise Exception(f"Store {storeName} cmd is None")

    def createIngestionStepForDataStore(self, store: Datastore, exportStep: ExportNode) -> PipelineNode:
        # Create a step for a single or multi dataset ingestion
        ingestionStep: Optional[PipelineNode] = None
        if store.cmd is None:
            raise Exception(f"Store {store.name} cmd is None")
        if (store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET):
            ingestionStep = IngestionSingleNode(exportStep.platform, exportStep.storeName, exportStep.datasetName, store.cmd.stepTrigger)
        else:  # MULTI_DATASET
            ingestionStep = IngestionMultiNode(exportStep.platform, exportStep.storeName, store.cmd.stepTrigger)
        ingestionStep = self.findExistingOrCreateStep(ingestionStep)
        return ingestionStep

    def createGraphForDataTransformer(self, dt: DataTransformerOutput, exportStep: ExportNode) -> None:
        """If a store is the output for a DataTransformer then we need to ingest it from the Workspace
        which defines the DataTransformer."""
        w: Workspace = self.eco.cache_getWorkspaceOrThrow(dt.workSpaceName).workspace
        if w.dataContainer:
            # Find/Create Trigger, this is a join on all incoming exports needed for the transformer
            dtStep: DataTransformerNode = cast(DataTransformerNode, self.findExistingOrCreateStep(DataTransformerNode(w, self.platform)))
            triggerStep: TriggerNode = cast(TriggerNode, self.findExistingOrCreateStep(TriggerNode(w, self.platform)))
            # Add ingestion for transfomer
            dtIngestStep: PipelineNode = self.findExistingOrCreateStep(IngestionMultiNode(self.platform, exportStep.storeName, None))
            dtStep.addRightHandNode(dtIngestStep)
            # Ingesting Transformer causes Export
            dtIngestStep.addRightHandNode(exportStep)
            # Trigger calls Transformer Step
            triggerStep.addRightHandNode(dtStep)
            # Add Exports to call trigger
            for dsgR in self.roots:
                if dsgR.workspace == w:
                    for sink in dsgR.dsg.sinks.values():
                        dsrExportStep: ExportNode = cast(ExportNode, self.findExistingOrCreateStep(
                                ExportNode(self.platform, w.dataContainer, sink.storeName, sink.datasetName)))
                        self.addExportToPriorIngestion(dsrExportStep)
                        # Add Trigger for DT after export
                        dsrExportStep.addRightHandNode(triggerStep)

    def addExportToPriorIngestion(self, exportStep: ExportNode):
        """This makes sure the ingestion steps for a the datasets in an export step exist"""
        assert (self.nodes.get(str(exportStep)) is not None)
        """Work backwards from export step. The normal chain is INGEST -> EXPORT. In the case of exporting a store from
        a transformer then it is INGEST -> EXPORT -> TRIGGER -> TRANSFORM -> INGEST -> EXPORT"""
        store: Datastore = self.eco.cache_getDatastoreOrThrow(exportStep.storeName).datastore
        if (store.cmd):
            # Create a step for a single or multi dataset ingestion
            ingestionStep: PipelineNode = self.createIngestionStepForDataStore(store, exportStep)
            ingestionStep.addRightHandNode(exportStep)
            # If this store is a transformer then we need to create the transformer job
            if isinstance(store.cmd, DataTransformerOutput):
                self.createGraphForDataTransformer(store.cmd, exportStep)

    def getLeftSideOfGraph(self) -> set[PipelineNode]:
        """This returns ingestions which don't depend on anything else, the left end of a pipeline"""
        rc: set[PipelineNode] = set()
        for step in self.nodes.values():
            if len(step.leftHandNodes) == 0:
                rc.add(step)
        return rc

    def getRightSideOfGraph(self) -> set[PipelineNode]:
        """This returns steps which does have other steps depending on them, the right end of a pipeline"""
        rc: set[PipelineNode] = set()
        for step in self.nodes.values():
            if len(step.rightHandNodes) == 0:
                rc.add(step)
        return rc

    def checkNextStepsForStepType(self, filterStep: Type[PipelineNode], targetStep: Type[PipelineNode]) -> bool:
        """This finds steps of a certain type and then checks that ALL follow on steps from it are a certain type"""
        for s in self.nodes:
            if isinstance(s, filterStep):
                for nextS in s.rightHandNodes:
                    if not isinstance(nextS, targetStep):
                        return False
        return True

    def graphToText(self) -> str:
        """This returns a string representation of the pipeline graph from left to right"""
        left_side = self.getLeftSideOfGraph()

        # Create a dictionary to keep track of the nodes we've visited
        visited = {node: False for node in self.nodes.values()}

        def dfs(node: PipelineNode, indent: str = '') -> str:
            """Depth-first search to traverse the graph and build the string representation"""
            if visited[node]:
                return str(node)
            visited[node] = True
            result = indent + str(node) + '\n'
            if node.rightHandNodes:
                result += ' -> ('
                result += ', '.join(dfs(n, indent + '  ') for n in node.rightHandNodes.values() if not visited[n])
                result += ')'
            return result

        # Start the traversal from each node on the left side
        graph_strs = [dfs(node) for node in left_side]

        return '\n'.join(graph_strs)

    def lint(self, tree: ValidationTree) -> None:
        """This checks the pipeline graph for errors and warnings"""

        # Get the IaC renderer for the platform
        gHandler: DataPlatformGraphHandler = self.platform.createGraphHandler(self)

        # Lint the graph to check all nodes are valid with this platform
        # This checks for unsupported databases, vendors, transformers and so on
        gHandler.lintGraph(self.eco, tree.addSubTree(gHandler))

    def propagateWorkspacePriorities(self):
        """Propagates workspace priorities through the pipeline graph.
        Each node's priority will be set to the highest priority of any workspace that depends on it,
        either directly or indirectly through the dependency chain.

        Implementation uses topological sort to process nodes in dependency order, ensuring
        each node is processed only once and after all nodes that depend on it."""

        # First set priorities for nodes directly connected to workspaces
        for dsg in self.roots:
            workspace = dsg.workspace
            # Find all export nodes for this workspace's datasets
            for sink in dsg.dsg.sinks.values():
                if workspace.dataContainer:
                    export_node = ExportNode(self.platform, workspace.dataContainer, sink.storeName, sink.datasetName)
                    if str(export_node) in self.nodes:
                        node = self.nodes[str(export_node)]
                        node.setPriority(workspace.priority)

                        # Also set priority for any trigger and transformer nodes for this workspace
                        trigger_node = TriggerNode(workspace, self.platform)
                        if str(trigger_node) in self.nodes:
                            self.nodes[str(trigger_node)].setPriority(workspace.priority)

                        transformer_node = DataTransformerNode(workspace, self.platform)
                        if str(transformer_node) in self.nodes:
                            self.nodes[str(transformer_node)].setPriority(workspace.priority)

        # Get nodes in topological order (from right to left)
        visited: set[str] = set()
        sorted_nodes: list[PipelineNode] = []

        def visit(node: PipelineNode):
            if str(node) in visited:
                return
            visited.add(str(node))
            # Visit all nodes that depend on this node first
            for dep in node.rightHandNodes.values():
                visit(dep)
            sorted_nodes.append(node)

        # Start from all nodes with no left dependencies (ingestion nodes)
        left_side = self.getLeftSideOfGraph()
        for node in left_side:
            visit(node)

        # Process nodes in reverse topological order (from right to left)
        # This ensures we process each node after all nodes that depend on it
        for node in reversed(sorted_nodes):
            # Get highest priority from nodes that depend on this one
            highest_priority = None
            for dep_node in node.rightHandNodes.values():
                if dep_node.priority is not None:
                    if highest_priority is None or not highest_priority.isMoreImportantThan(dep_node.priority):
                        highest_priority = dep_node.priority

            # Set this node's priority if we found a higher one
            if highest_priority is not None:
                node.setPriority(highest_priority)


class EcosystemPipelineGraph(InternalLintableObject):
    """This is the total graph for an Ecosystem. It's a list of graphs keyed by DataPlatforms in use. One graph per DataPlatform"""
    def __init__(self, eco: Ecosystem):
        InternalLintableObject.__init__(self)
        self.eco: Ecosystem = eco

        # Store for each DP, the set of DSGRootNodes
        self.roots: dict[DataPlatform, PlatformPipelineGraph] = dict()

        # Scan workspaces/dsg pairs, split by DataPlatform
        for w in eco.workSpaceCache.values():
            for dsg in w.workspace.dsgs.values():
                if dsg.platformMD:
                    p: Optional[DataPlatform] = dsg.platformMD.choooseDataPlatform(self.eco)
                    if p:
                        root: DSGRootNode = DSGRootNode(w.workspace, dsg)
                        if self.roots.get(p) is None:
                            self.roots[p] = PlatformPipelineGraph(eco, p)
                        self.roots[p].roots.add(root)
                        # Collect Workspaces using the platform
                        if (self.roots[p].workspaces.get(w.workspace.name) is None):
                            self.roots[p].workspaces[w.workspace.name] = w.workspace

        # Now track DSGs per dataContainer
        # For each platform what DSGs need to be exported to a given dataContainer
        for platform in self.roots.keys():
            pinfo = self.roots[platform]
            pinfo.generateGraph()

    def lint(self, tree: ValidationTree) -> None:
        for p in self.roots.values():
            p.lint(tree.addSubTree(p))

    def __str__(self) -> str:
        return f"EcosystemPipelineGraph({self.eco.name})"


class IaCFragmentManager(Documentable):
    """This is a fragment manager for IaC. It is used to store fragments of IaC code which are generated for a pipeline
    graph."""
    def __init__(self, name: str, doc: Documentation):
        Documentable.__init__(self, doc)
        self.name: str = name

    @abstractmethod
    def preRender(self):
        """This is called before the rendering of the fragments. It can be used to set up the fragment manager."""
        pass

    @abstractmethod
    def postRender(self):
        """This is called after the rendering of the fragments. It can be used to clean up the fragment manager."""
        pass

    @abstractmethod
    def addFragment(self, node: PipelineNode, fragment: str):
        """Add a fragment to the fragment manager"""
        pass


class CombineToStringFragmentManager(IaCFragmentManager):
    def __init__(self, name: str, doc: Documentation):
        super().__init__(name, doc)
        # This is a dictionary of dictionaries. The first key is the group, the second key is the name, and the value is the fragment
        self.fragments: dict[Type[PipelineNode], dict[PipelineNode, str]] = {}

    def addFragment(self, node: PipelineNode, fragment: str):
        nodeType: Type[PipelineNode] = node.__class__
        if nodeType not in self.fragments:
            self.fragments[nodeType] = {}
        self.fragments[nodeType][node] = fragment

    def __str__(self) -> str:
        rc: str = ""
        for group in self.fragments:
            rc += f"Group: {group}\n"
            for name in self.fragments[group]:
                rc += f"  {name}: {self.fragments[group][name]}\n"
        return rc


def defaultPipelineNodeFileName(node: PipelineNode) -> str:
    """Calculate simple file names for pipeline nodes. These are used with an file extension for the particular
    IaC provider, e.g. .tf for Terraform or .yaml for Kubernetes. The file name is unique for each node."""
    if (isinstance(node, IngestionMultiNode)):
        return f"{node.__class__.__name__}_{node.storeName}"
    elif (isinstance(node, IngestionSingleNode)):
        return f"{node.__class__.__name__}_{node.storeName}_{node.datasetName}"
    elif (isinstance(node, ExportNode)):
        return f"{node.__class__.__name__}_{node.storeName}_{node.datasetName}"
    elif (isinstance(node, TriggerNode)):
        return f"{node.__class__.__name__}_{node.name}_{node.workspace.name}"
    elif (isinstance(node, DataTransformerNode)):
        return f"{node.__class__.__name__}_{node.workspace.name}_{node.name}"
    raise Exception(f"Unknown node type {node}")


class FileBasedFragmentManager(IaCFragmentManager):
    """This is a file based fragment manager. It writes the fragments to a temporary directory. The fragments are stored in files with the name of the node.
    The name of the node is determined by the fnGetFileNameForNode function. This function should return a unique name for each node.
    The fragments are stored in the rootDir directory."""
    def __init__(self, name: str, doc: Documentation, fnGetFileNameForNode: Callable[[PipelineNode], str]):
        super().__init__(name, doc)
        self.rootDir: str = tempfile.mkdtemp()
        self.fnGetFileNameForNode: Callable[[PipelineNode], str] = fnGetFileNameForNode

    def addFragment(self, node: PipelineNode, fragment: str):
        name: str = self.fnGetFileNameForNode(node)
        with open(f"{self.rootDir}/{name}", "w") as file:
            file.write(fragment)

    def addStaticFile(self, folder: str, name: str, fragment: str):
        """Add a static file to the fragment manager. This is useful for adding files like
        provider.tf or variables.tf which are not associated with a particular node."""
        # Create the folder if it does not exist
        full_folder_path: str = f"{self.rootDir}/{folder}" if len(folder) > 0 else self.rootDir
        if not os.path.exists(full_folder_path):
            os.makedirs(full_folder_path)
        with open(f"{full_folder_path}/{name}", "w") as file:
            file.write(fragment)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, rootDir={self.rootDir})"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FileBasedFragmentManager) and self.rootDir == other.rootDir

    def __hash__(self) -> int:
        return hash(self.name)


class DataPlatformGraphHandler(UserDSLObject):
    """This is a base class for DataPlatform code for handling a specific intention graph"""
    def __init__(self, graph: PlatformPipelineGraph):
        UserDSLObject.__init__(self)
        self.graph: PlatformPipelineGraph = graph

    @abstractmethod
    def getInternalDataContainers(self) -> set[DataContainer]:
        """This returns all the internal DataContainers created by this DataPlatform to
        execute the pipelines for the indicated graph. These is meant for internal containers not
        for containers for Datastores or Workspaces"""
        pass

    @abstractmethod
    def lintGraph(self, eco: Ecosystem, tree: ValidationTree):
        """This checks the pipeline graph for errors and warnings. It should also check that the platform
        can handle every node in the pipeline graph. Nodes may fail because there is no supported"""
        pass


class IaCDataPlatformRenderer(DataPlatformGraphHandler):
    """This is intended to be a base class for IaC style DataPlatforms which render the intention graph
    to an IaC format. The various nodes in the graph are rendered as seperate files in a temporary folder
    which remains after the graph is rendered. The folder can then be committed to a CI/CD repository where
    it can be used by a platform like Terraform to effect the changes in the graph."""
    def __init__(self, executor: DataPlatformExecutor, graph: PlatformPipelineGraph):
        super().__init__(graph)
        self.executor: DataPlatformExecutor = executor

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, IaCDataPlatformRenderer) and self.executor == other.executor and \
            self.graph == other.graph

    def renderIaC(self, fragments: IaCFragmentManager) -> IaCFragmentManager:
        fragments.preRender()
        """This renders the IaC for the given graph"""
        for node in self.graph.nodes.values():
            if isinstance(node, IngestionSingleNode):
                fragments.addFragment(node, self.renderIngestionSingle(node))
            elif isinstance(node, IngestionMultiNode):
                fragments.addFragment(node, self.renderIngestionMulti(node))
            elif isinstance(node, ExportNode):
                fragments.addFragment(node, self.renderExport(node))
            elif isinstance(node, TriggerNode):
                fragments.addFragment(node, self.renderTrigger(node))
            elif isinstance(node, DataTransformerNode):
                fragments.addFragment(node, self.renderDataTransformer(node))
            else:
                raise Exception(f"Unknown node type {node.__class__.__name__}")
        fragments.postRender()
        return fragments

    @abstractmethod
    def renderIngestionSingle(self, ingestNode: IngestionSingleNode) -> str:
        pass

    @abstractmethod
    def renderIngestionMulti(self, ingestNode: IngestionMultiNode) -> str:
        pass

    @abstractmethod
    def renderExport(self, exportNode: ExportNode) -> str:
        pass

    @abstractmethod
    def renderTrigger(self, triggerNode: TriggerNode) -> str:
        pass

    @abstractmethod
    def renderDataTransformer(self, dtNode: DataTransformerNode) -> str:
        pass

    @abstractmethod
    def lintIngestionSingleNode(self, eco: Ecosystem, node: IngestionSingleNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintIngestionMultiNode(self, eco: Ecosystem, node: IngestionMultiNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintExportNode(self, eco: Ecosystem, node: ExportNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintTriggerNode(self, eco: Ecosystem, node: TriggerNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintDataTransformerNode(self, eco: Ecosystem, node: DataTransformerNode, tree: ValidationTree) -> None:
        pass

    def lintGraph(self, eco: Ecosystem, tree: ValidationTree):
        """Check that the platform can handle every node in the pipeline graph. Nodes may fail because there is no supported
        connector for an ingestion or export node or because there are missing parameters or because a certain type of
        trigger or data transformer is not supported. It may also fail because an infrastructure vendor or datacontainer is not supported"""
        for node in self.graph.nodes.values():
            if (isinstance(node, IngestionSingleNode)):
                self.lintIngestionSingleNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, IngestionMultiNode)):
                self.lintIngestionMultiNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, ExportNode)):
                self.lintExportNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, TriggerNode)):
                self.lintTriggerNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, DataTransformerNode)):
                self.lintDataTransformerNode(eco, node, tree.addSubTree(node))

    def getDataContainerForDatastore(self, storeName: str) -> Optional[DataContainer]:
        """Get the data container for a given datastore"""
        storeEntry: DatastoreCacheEntry = self.graph.eco.cache_getDatastoreOrThrow(storeName)
        if (storeEntry.datastore.cmd is not None):
            return storeEntry.datastore.cmd.dataContainer
        else:
            return None

    def isDataContainerSupported(self, dc: DataContainer, allowedContainers: set[Type[DataContainer]]) -> bool:
        """Check if a data container is supported"""
        return dc.__class__ in allowedContainers


class IaCDataPlatformRendererShim(IaCDataPlatformRenderer):
    """This is a shim for during development. It does nothing and is used to test the IaCDataPlatformRenderer interface"""
    def __init__(self, executor: DataPlatformExecutor, graph: PlatformPipelineGraph):
        super().__init__(executor, graph)

    def renderIngestionSingle(self, ingestNode: IngestionSingleNode) -> str:
        return ""

    def renderIngestionMulti(self, ingestNode: IngestionMultiNode) -> str:
        return ""

    def renderExport(self, exportNode: ExportNode) -> str:
        return ""

    def renderTrigger(self, triggerNode: TriggerNode) -> str:
        return ""

    def renderDataTransformer(self, dtNode: DataTransformerNode) -> str:
        return ""

    def lintIngestionSingleNode(self, eco: Ecosystem, node: IngestionSingleNode, tree: ValidationTree) -> None:
        pass

    def lintIngestionMultiNode(self, eco: Ecosystem, node: IngestionMultiNode, tree: ValidationTree) -> None:
        pass

    def lintExportNode(self, eco: Ecosystem, node: ExportNode, tree: ValidationTree) -> None:
        pass

    def lintTriggerNode(self, eco: Ecosystem, node: TriggerNode, tree: ValidationTree) -> None:
        pass

    def lintDataTransformerNode(self, eco: Ecosystem, node: DataTransformerNode, tree: ValidationTree) -> None:
        pass

    def getInternalDataContainers(self) -> set[DataContainer]:
        raise NotImplementedError("This is a shim")


class UnsupportedDataContainer(ValidationProblem):
    def __init__(self, dc: DataContainer):
        super().__init__(f"DataContainer {dc} is not supported", ProblemSeverity.ERROR)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, UnsupportedDataContainer)

    def __hash__(self) -> int:
        return hash(self.description)


class InfraStructureLocationPolicy(AllowDisallowPolicy[LocationKey]):
    """Allows a GZ to police which locations can be used with datastores or workspaces within itself"""
    def __init__(self, name: str, doc: Documentation, allowed: Optional[set[LocationKey]] = None,
                 notAllowed: Optional[set[LocationKey]] = None):
        super().__init__(name, doc, allowed, notAllowed)

    def __str__(self):
        return f"InfrastructureLocationPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        rc: bool = super().__eq__(v)
        rc = rc and isinstance(v, InfraStructureLocationPolicy)
        other: InfraStructureLocationPolicy = cast(InfraStructureLocationPolicy, v)
        rc = rc and self.allowed == other.allowed
        rc = rc and self.notAllowed == other.notAllowed
        rc = rc and self.name == other.name
        return rc

    def __hash__(self) -> int:
        return super().__hash__()


class InfraStructureVendorPolicy(AllowDisallowPolicy[VendorKey]):
    """Allows a GZ to police which vendors can be used with datastore or workspaces within itself"""
    def __init__(self, name: str, doc: Documentation, allowed: Optional[set[VendorKey]] = None,
                 notAllowed: Optional[set[VendorKey]] = None):
        super().__init__(name, doc, allowed, notAllowed)

    def __str__(self):
        return f"InfraStructureVendorPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        if not super().__eq__(v) or not isinstance(v, InfraStructureVendorPolicy):
            return False
        return self.allowed == v.allowed and self.notAllowed == v.notAllowed

    def __hash__(self) -> int:
        return super().__hash__()

