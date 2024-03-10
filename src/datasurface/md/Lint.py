

from enum import Enum
from typing import Any, Callable, Generator


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

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) and self.description == o.description and self.sev == o.sev


class UnknownObjectReference(ValidationProblem):
    """This indicates an unknown object"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Unknown object {obj}", sev)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, UnknownObjectReference)

    def __hash__(self) -> int:
        return hash(self.description)


class AttributeNotSet(ValidationProblem):
    """This indicates a required attribute has not been specified"""
    def __init__(self, key: str) -> None:
        super().__init__(f"Attribute {key} not set", ProblemSeverity.ERROR)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AttributeNotSet)

    def __hash__(self) -> int:
        return hash(self.description)


class ObjectIsDeprecated(ValidationProblem):
    """This indicates an object is deprecated"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is deprecated", sev)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, ObjectIsDeprecated)

    def __hash__(self) -> int:
        return hash(self.description)


class DataTransformerMissing(ValidationProblem):
    """This indicates a data transformer is missing"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Data transformer for {obj} is missing", sev)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, DataTransformerMissing)

    def __hash__(self) -> int:
        return hash(self.description)


class ObjectWrongType(ValidationProblem):
    """This indicates an object is the wrong type"""
    def __init__(self, obj: object, expectedType: type, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is not of type {expectedType}", sev)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, ObjectWrongType)

    def __hash__(self) -> int:
        return hash(self.description)


class ObjectMissing(ValidationProblem):
    """This indicates an object is missing"""
    def __init__(self, container: object, missingObject: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {missingObject} is missing from {container}", sev)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, ObjectMissing)

    def __hash__(self) -> int:
        return hash(self.description)


class UnauthorizedAttributeChange(ValidationProblem):
    """This indicates an unauthorized change, two objects are different"""
    def __init__(self, attribute: str, obj1: object, obj2: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Unauthorized {attribute} change: {obj1} is different from {obj2}", sev)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, UnauthorizedAttributeChange)

    def __hash__(self) -> int:
        return hash(self.description)


class NameHasBadSynthax(ValidationProblem):
    """This indicates a name has bad syntax"""
    def __init__(self, name: str) -> None:
        super().__init__(f"Name {name} has bad syntax", ProblemSeverity.ERROR)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, NameHasBadSynthax)

    def __hash__(self) -> int:
        return hash(self.description)


class NameMustBeSQLIdentifier(NameHasBadSynthax):
    """This indicates a name is not a valid SQL identifier"""
    def __init__(self, name: str, sev: ProblemSeverity) -> None:
        super().__init__(f"Name {name} is not a valid SQL identifier")

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, NameMustBeSQLIdentifier)

    def __hash__(self) -> int:
        return hash(self.description)


class ObjectNotCompatibleWithPolicy(ValidationProblem):
    """This indicates an object is not compatible with a policy"""
    def __init__(self, obj: object, policy: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is not compatible with policy {policy}", sev)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, ObjectNotCompatibleWithPolicy)

    def __hash__(self) -> int:
        return hash(self.description)


class DuplicateObject(ValidationProblem):
    """This indicates an object is duplicated"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is duplicated", sev)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, DuplicateObject)

    def __hash__(self) -> int:
        return hash(self.description)


class ConstraintViolation(ValidationProblem):
    """This indicates a constraint violation"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Constraint violation {obj}", sev)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, ConstraintViolation)

    def __hash__(self) -> int:
        return hash(self.description)


class ProductionDatastoreMustHaveClassifications(ValidationProblem):
    """This indicates a production Datastore has a Dataset where the DataClassification of some attributes is unspecified"""
    def __init__(self, store: object, dataset: object) -> None:
        super().__init__(f"Production Datastore {store} has a dataset {dataset} with missing DataClassifications", ProblemSeverity.ERROR)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, ProductionDatastoreMustHaveClassifications)

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


class ValidationTree:
    """This is a tree of issues found while running a set of checks against the model. It is used to collect issues. Each node in the
    tree represents an object in the model. Each node can list a set of issues found with that node"""
    def __init__(self, obj: object) -> None:
        self.object: object = obj
        self.numErrors: int = 0
        self.numWarnings: int = 0

        """The original object that is in use"""
        self.children: list[ValidationTree] = []
        """The list of children of this object"""
        self.problems: list[ValidationProblem] = []
        """The list of problems with this object"""

    def addSubTree(self, obj: object) -> 'ValidationTree':
        """This creates a subtree of this object"""
        child: ValidationTree = ValidationTree(obj)
        self.children.append(child)
        return child

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
        return self.findMatchingProblems(lambda p: p.sev != ProblemSeverity.ERROR)

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
        if (self.getErrors() or self.getWarnings()):  # If something to see here or in the children then
            print(" " * indent, self.object)
            for problem in self.problems:
                print(" " * (indent + 2), str(problem))
                if (problem.sev == ProblemSeverity.ERROR):
                    self.numErrors += 1
                if (problem.sev == ProblemSeverity.WARNING):
                    self.numWarnings += 1
            for child in self.children:
                child.printTree(indent + 2)
                self.numErrors += child.numErrors
                self.numWarnings += child.numWarnings
            if (indent == 0):
                print(f"Total errors: {self.numErrors}, warnings: {self.numWarnings}")

    def containsProblemType(self, type: Any) -> bool:
        """This returns true if this object or any of its children have a problem of the given type"""
        return next((True for _ in self.findMatchingProblems(lambda p: isinstance(p, type))), False)
