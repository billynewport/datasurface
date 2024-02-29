

from enum import Enum
from typing import Any


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


class AttributeNotSet(ValidationProblem):
    """This indicates a required attribute has not been specified"""
    def __init__(self, key: str) -> None:
        super().__init__(f"Attribute {key} not set", ProblemSeverity.ERROR)


class ObjectIsDeprecated(ValidationProblem):
    """This indicates an object is deprecated"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is deprecated", sev)


class DataTransformerMissing(ValidationProblem):
    """This indicates a data transformer is missing"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Data transformer for {obj} is missing", sev)


class ObjectWrongType(ValidationProblem):
    """This indicates an object is the wrong type"""
    def __init__(self, obj: object, expectedType: type, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is not of type {expectedType}", sev)


class ObjectMissing(ValidationProblem):
    """This indicates an object is missing"""
    def __init__(self, container: object, missingObject: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {missingObject} is missing from {container}", sev)


class UnauthorizedAttributeChange(ValidationProblem):
    """This indicates an unauthorized change, two objects are different"""
    def __init__(self, attribute: str, obj1: object, obj2: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Unauthorized {attribute} change: {obj1} is different from {obj2}", sev)


class NameMustBeSQLIdentifier(ValidationProblem):
    """This indicates a name is not a valid SQL identifier"""
    def __init__(self, name: str, sev: ProblemSeverity) -> None:
        super().__init__(f"Name {name} is not a valid SQL identifier", sev)


class ObjectNotCompatibleWithPolicy(ValidationProblem):
    """This indicates an object is not compatible with a policy"""
    def __init__(self, obj: object, policy: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is not compatible with policy {policy}", sev)


class DuplicateObject(ValidationProblem):
    """This indicates an object is duplicated"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Object {obj} is duplicated", sev)


class ConstraintViolation(ValidationProblem):
    """This indicates a constraint violation"""
    def __init__(self, obj: object, sev: ProblemSeverity) -> None:
        super().__init__(f"Constraint violation {obj}", sev)


class ProductionDatastoreMustHaveClassifications(ValidationProblem):
    """This indicates a production Datastore has a Dataset where the DataClassification of some attributes is unspecified"""
    def __init__(self, store: object, dataset: object) -> None:
        super().__init__(f"Production Datastore {store} has a dataset {dataset} with missing DataClassifications", ProblemSeverity.ERROR)


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

    def createChild(self, obj: object) -> 'ValidationTree':
        """This creates a child of this object"""
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

    def hasErrors(self) -> bool:
        """This returns true if this object or any of its children have ERROR severity problems"""
        for problem in self.problems:
            if (problem.sev == ProblemSeverity.ERROR):
                return True
        for child in self.children:
            if (child.hasErrors()):
                return True
        return False

    def hasIssues(self) -> bool:
        """This returns true if this object or any of its children have non ERROR severity problems"""
        for problem in self.problems:
            if (problem.sev != ProblemSeverity.ERROR):
                return True
        for child in self.children:
            if (child.hasIssues()):
                return True
        return False

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
        if (self.hasErrors() or self.hasIssues()):  # If something to see here or in the children then
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
        for p in self.problems:
            if isinstance(p, type):
                return True
        for child in self.children:
            if (child.containsProblemType(type)):
                return True
        return False
