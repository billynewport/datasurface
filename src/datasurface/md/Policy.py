from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, Optional, TypeVar
from datasurface.md import Documentation

from datasurface.md.Documentation import Documentable


T = TypeVar('T')


class Policy(ABC, Generic[T], Documentable):
    """Base class for all policies"""
    def __init__(self, name: str, doc: Optional[Documentation] = None) -> None:
        Documentable.__init__(self, doc)
        ABC.__init__(self)
        self.name: str = name

    @abstractmethod
    def isCompatible(self, obj: T) -> bool:
        """Check if obj meets the policy"""
        raise NotImplementedError()

    def __eq__(self, v: object) -> bool:
        return ABC.__eq__(self, v) and Documentable.__eq__(self, v) and isinstance(v, Policy) and self.name == v.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"


P = TypeVar('P')


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

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AllowDisallowPolicy) and self.name == o.name

    def __str__(self):
        return f"{self.__class__.__name__}({self.name}, {self.allowed},{self.notAllowed})"


class DataClassification(ABC):
    """Base class for defining data classifications"""
    def __init__(self):
        pass

    def __eq__(self, o: object) -> bool:
        return isinstance(o, DataClassification)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}"


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

    def __eq__(self, v: object) -> bool:
        return super().__eq__(v) and isinstance(v, DataClassificationPolicy) and self.allowed == v.allowed and self.notAllowed == v.notAllowed

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
