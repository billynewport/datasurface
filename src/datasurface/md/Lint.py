

from enum import Enum


class ProblemSeverity(Enum):
    """This is the severity of the problem"""
    ERROR = 0
    WARNING = 1
    INFO = 2
    
class ValidationProblem:
    def __init__(self, desc : str, sev : ProblemSeverity = ProblemSeverity.ERROR) -> None:
        self.description : str = desc
        """A description of what the issue is"""
        self.sev : ProblemSeverity = sev

    def __str__(self) -> str:
        return f"{self.sev}:{self.description}"

class ValidationTree:
    def __init__(self, obj : object) -> None:
        self.object : object = obj

        """The original object that is in use"""
        self.children : list[ValidationTree] = []
        """The list of children of this object"""
        self.problems : list[ValidationProblem] = []
        """The list of problems with this object"""

    def createChild(self, obj : object) -> 'ValidationTree':
        """This creates a child of this object"""
        child : ValidationTree = ValidationTree(obj)
        self.children.append(child)
        return child
    
    def addProblem(self, problem : str, sev : ProblemSeverity = ProblemSeverity.ERROR) -> None:
        """This adds a problem to this object"""
        self.problems.append(ValidationProblem(problem, sev))

    def hasErrors(self) -> bool:
        """This returns true if this object or any of its children have ERROR severity problems"""
        for problem in self.problems:
            if(problem.sev == ProblemSeverity.ERROR):
                return True
            
        for child in self.children:
            if(child.hasErrors()):
                return True
        return False
    
    def hasIssues(self) -> bool:
        """This returns true if this object or any of its children have non ERROR severity problems"""
        for problem in self.problems:
            if(problem.sev != ProblemSeverity.ERROR):
                return True
            
        for child in self.children:
            if(child.hasIssues()):
                return True
        return False
    
    def checkTypeMatches(self, obj : object, *expectedType : type) -> bool:
        """Returns true if any type matches, false if not and adds a problem"""

        for type in expectedType:
            if(isinstance(obj, type)):
                return True
        self.addProblem("Unexpected type " + str(obj.__class__.__name__))
        return False

    def printTree(self, indent : int = 0) -> None:
        """This prints the tree of objects"""
        if(self.hasErrors()): # If something to see here or in the children then
            print(" " * indent, self.object)
            for problem in self.problems:
                print(" " * (indent + 2), str(problem))
            for child in self.children:
                child.printTree(indent + 2)
    

