"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from enum import Enum
from typing import Any, Callable, Generator, Optional
from abc import ABC, abstractmethod
from types import FrameType
import inspect
from datasurface.md.utils import is_valid_sql_identifier
from datasurface.md.exceptions import NameMustBeANSISQLIdentifierException
import os
from datasurface.md.json import JSONable
import logging

# Standard Python logger - works everywhere
logger = logging.getLogger(__name__)

# Global performance controls
_enable_source_tracking = os.environ.get('DATASURFACE_DEBUG', 'false').lower() == 'true'


def enable_source_tracking():
    """Enable source tracking for debugging - impacts performance"""
    global _enable_source_tracking
    _enable_source_tracking = True


def disable_source_tracking():
    """Disable source tracking for maximum performance"""
    global _enable_source_tracking
    _enable_source_tracking = False


def is_source_tracking_enabled() -> bool:
    """Check if source tracking is currently enabled"""
    return _enable_source_tracking


class ValidatableObject(ABC):
    def __init__(self) -> None:
        ABC.__init__(self)

    @abstractmethod
    def getSourceReferenceString(self) -> str:
        """This returns a string representing where this object was first constructed"""
        pass


class UserDSLObject(ValidatableObject, JSONable):
    """This walks back the stack to find the file and line number constructing the object. It needs
    to skip all datasurface classes on the call stack until a non datasurface class is found and that is
    the frame we should use. Objects which we validate in the DSL should inherit from this class"""
    def __init__(self, filename: Optional[str] = None, linenumber: Optional[int] = None) -> None:
        ValidatableObject.__init__(self)
        JSONable.__init__(self)
        self._line_number: int = 0  # The _ here causes these variables to be ignore during cyclic reference checks
        self._file_name: str = ""  # We want this because we can define the identical object in two places and still want them equal
        self._source_ref_cache: Optional[str] = None  # Cache the computed string

        if (filename is not None and linenumber is not None):
            self._file_name = filename
            self._line_number = linenumber
        else:
            # Only do expensive stack walking if source tracking is enabled
            # You could make this conditional with an environment variable
            self._capture_source_info()

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {"_type": self.__class__.__name__}
        return rc

    def _capture_source_info(self) -> None:
        """Capture minimal source info immediately (just filename and line number)"""
        if not _enable_source_tracking:
            # Skip expensive stack walking when source tracking is disabled
            self._file_name = "<source_tracking_disabled>"
            self._line_number = 0
        else:
            f: Optional[FrameType] = self.findFirstNonDataSurfaceCaller()
            if f is not None:
                self._line_number: int = f.f_lineno
                self._file_name: str = f.f_code.co_filename
                # Don't hold onto the frame object itself!

    def setSource(self, filename: str, line: int):
        """This is used to set the file/line when the model is populated using non python DSLs. For example,
        someone can build an adapter to create a model from YML or JSON and then this information has nothing
        to do with the code loading the YML into the model."""
        self._file_name = filename
        self._line_number = line

    def setSourceFromObject(self, obj: 'UserDSLObject'):
        """This is used to set the source from another UserDSLObject"""
        self._file_name = obj._file_name
        self._line_number = obj._line_number

    def findFirstNonDataSurfaceCaller(self) -> Optional[FrameType]:
        """This walks back the caller stack until it finds a non datasurface caller. This is
        assumed to be the python module defining the DSL."""
        f: Optional[FrameType] = inspect.currentframe()
        while f is not None:
            bf: Optional[FrameType] = f.f_back  # type: ignore
            if bf is not None:
                module_name = bf.f_globals.get('__name__', '')  # type: ignore
                if not module_name.startswith('datasurface'):  # type: ignore
                    return bf  # type: ignore
            f = bf  # type: ignore
        return None

    def getSourceReferenceString(self) -> str:
        """This returns a string representing where this object was first constructed"""
        if self._source_ref_cache is None:
            # Lazy computation and caching of the string
            self._source_ref_cache = f"{self}@{self._file_name}::{self._line_number}"
        return self._source_ref_cache

    def __eq__(self, other: object) -> bool:
        """Ignore the source reference when comparing objects"""
        return isinstance(other, self.__class__)


class InternalLintableObject(ValidatableObject):
    """This is used for objects that are internal to the model and not part of the DSL. Examples would be
    objects generated after the DSL has been constructed, there is no 'source' file for the object"""
    def __init__(self) -> None:
        super().__init__()

    def getSourceReferenceString(self) -> str:
        return f"Internal::{self}"


class ProblemSeverity(Enum):
    """This is the severity of the problem"""
    ERROR = 0
    WARNING = 1
    INFO = 2


class ValidationProblem:
    def __init__(self, desc: str, sev: ProblemSeverity = ProblemSeverity.ERROR) -> None:
        self.description: str = desc
        """A description of what the issue is"""
        self.sev: ProblemSeverity = sev

    def __str__(self) -> str:
        return f"{self.sev.name}:{self.description}"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and self.description == other.description and self.sev == other.sev


class UnknownObjectReference(ValidationProblem):
    """This indicates an unknown object"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Unknown object {obj}", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, UnknownObjectReference)

    def __hash__(self) -> int:
        return hash(self.description)


class AttributeNotSet(ValidationProblem):
    """This indicates a required attribute has not been specified"""
    def __init__(self, key: str) -> None:
        super().__init__(f"Attribute {key} not set", ProblemSeverity.ERROR)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, AttributeNotSet)

    def __hash__(self) -> int:
        return hash(self.description)


class ObjectIsDeprecated(ValidationProblem):
    """This indicates an object is deprecated"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is deprecated", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, ObjectIsDeprecated)

    def __hash__(self) -> int:
        return hash(self.description)


class DataTransformerMissing(ValidationProblem):
    """This indicates a data transformer is missing"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Data transformer for {obj} is missing", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DataTransformerMissing)

    def __hash__(self) -> int:
        return hash(self.description)


class ObjectWrongType(ValidationProblem):
    """This indicates an object is the wrong type"""
    def __init__(self, obj: object, expectedType: type, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is not of type {expectedType}", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, ObjectWrongType)

    def __hash__(self) -> int:
        return hash(self.description)


class AttributeValueNotSupported(ValidationProblem):
    """This indicates an value isn't supported"""
    def __init__(self, obj: object, supportedValues: list[str], sev: ProblemSeverity) -> None:
        super().__init__(f"Value {obj} is unsupported value, allowed values: {supportedValues}", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, AttributeValueNotSupported)

    def __hash__(self) -> int:
        return hash(self.description)


class ObjectNotSupportedByDataPlatform(ValidationProblem):
    """This indicates an object is not supported by a data platform"""
    def __init__(self, obj: object, supportedTypes: list[type], sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is not supported by data platform {supportedTypes}", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, ObjectNotSupportedByDataPlatform)


class ObjectMissing(ValidationProblem):
    """This indicates an object is missing"""
    def __init__(self, container: object, missingObject: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {missingObject} is missing from {container}", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, ObjectMissing)

    def __hash__(self) -> int:
        return hash(self.description)


class UnauthorizedAttributeChange(ValidationProblem):
    """This indicates an unauthorized change, two objects are different"""
    def __init__(self, attribute: str, obj1: object, obj2: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Unauthorized {attribute} change: {obj1} is different from {obj2}", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, UnauthorizedAttributeChange)

    def __hash__(self) -> int:
        return hash(self.description)


class NameHasBadSynthax(ValidationProblem):
    """This indicates a name has bad syntax"""
    def __init__(self, name: str) -> None:
        super().__init__(f"Name {name} has bad syntax", ProblemSeverity.ERROR)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, NameHasBadSynthax)

    def __hash__(self) -> int:
        return hash(self.description)


class NameMustBeSQLIdentifier(NameHasBadSynthax):
    """This indicates a name is not a valid SQL identifier"""
    def __init__(self, name: str, sev: ProblemSeverity) -> None:
        super().__init__(f"Name {name} is not a valid SQL identifier")

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, NameMustBeSQLIdentifier)

    def __hash__(self) -> int:
        return hash(self.description)


class ObjectNotCompatibleWithPolicy(ValidationProblem):
    """This indicates an object is not compatible with a policy"""
    def __init__(self, obj: object, policy: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is not compatible with policy {policy}", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, ObjectNotCompatibleWithPolicy)

    def __hash__(self) -> int:
        return hash(self.description)


class DuplicateObject(ValidationProblem):
    """This indicates an object is duplicated"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is duplicated", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DuplicateObject)

    def __hash__(self) -> int:
        return hash(self.description)


class ConstraintViolation(ValidationProblem):
    """This indicates a constraint violation"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Constraint violation {obj}", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, ConstraintViolation)

    def __hash__(self) -> int:
        return hash(self.description)


class OwningRepoCannotBeLiveRepo(ValidationProblem):
    """This indicates the owning repo cannot be the live repo"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Owning repo for {obj} cannot be the same as its live repo", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, OwningRepoCannotBeLiveRepo)

    def __hash__(self) -> int:
        return hash(self.description)


class ProductionDatastoreMustHaveClassifications(ValidationProblem):
    """This indicates a production Datastore has a Dataset where the DataClassification of some attributes is unspecified"""
    def __init__(self, store: object, dataset: object) -> None:
        super().__init__(f"Production Datastore {store} has a dataset {dataset} with missing DataClassifications", ProblemSeverity.ERROR)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, ProductionDatastoreMustHaveClassifications)

    def __hash__(self) -> int:
        return hash(self.description)


class UnknownChangeSource(ValidationProblem):
    """This indicates an unknown change source"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Unknown change source {obj}", sev)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, UnknownChangeSource)

    def __hash__(self) -> int:
        return hash(self.description)


class UnexpectedExceptionProblem(ValidationProblem):
    """This indicates an unexpected exception"""
    def __init__(self, obj: Exception) -> None:
        super().__init__(f"Unexpected exception {obj}", ProblemSeverity.ERROR)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, UnexpectedExceptionProblem)

    def __hash__(self) -> int:
        return hash(self.description)


class ValidationTree:
    """This is a tree of issues found while running a set of checks against the model. It is used to collect issues. Each node in the
    tree represents an object in the model. Each node can list a set of issues found with that node"""
    def __init__(self, obj: ValidatableObject) -> None:
        self.object: ValidatableObject = obj
        self.numErrors: int = 0
        self.numWarnings: int = 0

        """The original object that is in use"""
        self.children: list[ValidationTree] = []
        """The list of children of this object"""
        self.problems: list[ValidationProblem] = []
        """The list of problems with this object"""

    def addSubTree(self, obj: ValidatableObject) -> 'ValidationTree':
        """This creates a subtree of this object"""
        child: ValidationTree = ValidationTree(obj)
        self.children.append(child)
        return child

    def addNamedSubTree(self, obj: 'ValidationTree', name: str) -> None:
        """This adds an existing tree under a string name in the tree"""
        self.children.append(obj)

    def prune(self) -> None:
        """This prunes the tree of objects that have no problems"""
        self.children = [child for child in self.children if child.hasErrors() or child.hasWarnings()]
        for child in self.children:
            child.prune()

    def getProblems(self) -> list[ValidationProblem]:
        """This returns the list of problems"""
        return self.problems

    def addProblem(self, problem: str, sev: ProblemSeverity = ProblemSeverity.ERROR) -> None:
        """This adds a problem to this object"""
        self.problems.append(ValidationProblem(problem, sev))

    def addRaw(self, problem: ValidationProblem) -> None:
        """This adds a problem to this object"""
        self.problems.append(problem)

    def findMatchingProblems(self, filterFunc: Callable[[ValidationProblem], bool]) -> Generator[ValidationProblem, None, None]:
        """This returns true if this object or any of its children match the filter"""
        for problem in self.problems:
            if (filterFunc(problem)):
                yield problem
        for child in self.children:
            yield from child.findMatchingProblems(filterFunc)

    def getErrors(self) -> Generator[ValidationProblem, None, None]:
        """This returns all errors if this object or any of its children have ERROR severity problems"""
        return self.findMatchingProblems(lambda p: p.sev == ProblemSeverity.ERROR)

    def hasErrors(self) -> bool:
        """This returns true if this object or any of its children have ERROR severity problems"""
        return next((True for _ in self.getErrors()), False)

    def hasWarnings(self) -> bool:
        """This returns true if this object or any of its children have Warning severity problems"""
        return next((True for _ in self.getWarnings()), False)

    def getWarnings(self) -> Generator[ValidationProblem, None, None]:
        """This returns all warnings if this object or any of its children have non ERROR severity problems"""
        return self.findMatchingProblems(lambda p: p.sev == ProblemSeverity.WARNING)

    def checkTypeMatches(self, obj: object, *expectedType: type) -> bool:
        """Returns true if any type matches, false if not and adds a problem"""

        for type in expectedType:
            if (isinstance(obj, type)):
                return True
        self.addProblem("Unexpected type " + str(obj.__class__.__name__))
        return False

    def printTree(self, indent: int = 0) -> None:
        """This prints the tree of objects"""
        self.numErrors = 0
        self.numWarnings = 0
        if (self.hasErrors() or self.hasWarnings()):  # If something to see here or in the children then
            logger.info("%s%s", " " * indent, self.object.getSourceReferenceString())
            for problem in self.problems:
                if problem.sev == ProblemSeverity.ERROR:
                    logger.error("%s%s", " " * (indent + 2), str(problem))
                    self.numErrors += 1
                elif problem.sev == ProblemSeverity.WARNING:
                    logger.warning("%s%s", " " * (indent + 2), str(problem))
                    self.numWarnings += 1
                else:
                    logger.info("%s%s", " " * (indent + 2), str(problem))
            for child in self.children:
                child.printTree(indent + 2)
                self.numErrors += child.numErrors
                self.numWarnings += child.numWarnings
            if (indent == 0):
                logger.info("Total errors: %d, warnings: %d", self.numErrors, self.numWarnings)

    def containsProblemType(self, type: Any) -> bool:
        """This returns true if this object or any of its children have a problem of the given type"""
        return next((True for _ in self.findMatchingProblems(lambda p: isinstance(p, type))), False)

    def getErrorsAsStructuredData(self) -> list[dict[str, Any]]:
        """Returns all validation errors as structured data for logging systems"""
        structured_errors: list[dict[str, Any]] = []

        def collect_errors(node: 'ValidationTree', path: str = "") -> None:
            current_path = f"{path}.{node.object.getSourceReferenceString()}" if path else node.object.getSourceReferenceString()

            for problem in node.problems:
                structured_errors.append({
                    "path": current_path,
                    "message": str(problem),
                    "severity": problem.sev.name,
                    "object_type": node.object.__class__.__name__
                })

            for child in node.children:
                collect_errors(child, current_path)

        collect_errors(self)
        return structured_errors


class ANSI_SQL_NamedObject(UserDSLObject):
    name: str
    """This is the base class for objects in the model which must have an SQL identifier compatible name. These
    objects may have names which are using in creating database artifacts such as Tables, views, columns"""
    def __init__(self, name: str, filename: Optional[str] = None, linenumber: Optional[int] = None) -> None:
        UserDSLObject.__init__(self, filename, linenumber)
        self.name: str = name
        """The name of the object"""
        if not is_valid_sql_identifier(self.name):
            raise NameMustBeANSISQLIdentifierException(self.name)
        UserDSLObject.__init__(self, filename, linenumber)

    def __eq__(self, other: object) -> bool:
        return UserDSLObject.__eq__(self, other) and isinstance(other, ANSI_SQL_NamedObject) and self.name == other.name

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

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "name": self.name}


def track_sources(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to enable source tracking for specific functions"""
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        old_setting = is_source_tracking_enabled()
        enable_source_tracking()
        try:
            return func(*args, **kwargs)
        finally:
            if not old_setting:
                disable_source_tracking()
    return wrapper
