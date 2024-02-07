from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar


T = TypeVar('T')

class Policy(ABC, Generic[T]):
    """Base class for all policies"""
    def __init__(self, name : str):
        self.name : str = name
    
    @abstractmethod
    def isCompatible(self, obj : T) -> bool:
        """Check if obj meets the policy"""
        raise NotImplementedError()
    
    def __eq__(self, v : object) -> bool:
        return isinstance(v, Policy) and self.name == v.name
    
    def __hash__(self) -> int:
        return hash(self.name)
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

P = TypeVar('P')

class AllowDisallowPolicy(Policy[P]):
    """This checks whether an object is explicitly allowed or explicitly forbidden"""
    def __init__(self, name : str, allowed : Optional[set[P]] = None, notAllowed : Optional[set[P]] = None) -> None:
        super().__init__(name)
        self.allowed : Optional[set[P]] = allowed
        self.notAllowed : Optional[set[P]] = notAllowed
        if(self.allowed and self.notAllowed):
            commonValues : set[P] = self.allowed.intersection(self.notAllowed)
            if len(commonValues) != 0:
                raise Exception("AllowDisallow groups overlap")

    def isCompatible(self, obj : P) -> bool:
        if self.allowed and not obj in self.allowed:
            return False
        if self.notAllowed and obj in self.notAllowed:
            return False
        return True
    
    def __hash__(self) -> int:
        return hash(self.name)
      
    def __str__(self):
        return f"{self.__class__.__name__}({self.name}, {self.allowed},{self.notAllowed})"
    

