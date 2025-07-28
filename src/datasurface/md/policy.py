"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from typing import TypeVar, Generic, Any, Optional
from abc import abstractmethod
from datasurface.md.documentation import Documentation, Documentable
from datasurface.md.json import JSONable
from enum import Enum

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
        rc: dict[str, Any] = Documentable.to_json(self)
        rc.update({"name": self.name, "_type": self.__class__.__name__})
        return rc


P = TypeVar('P', bound=JSONable)


L = TypeVar('L')


class Literal(JSONable, Generic[L]):
    def __init__(self, value: L) -> None:
        JSONable.__init__(self)
        self.value: L = value

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "value": self.value})
        return rc

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
        return super().__eq__(other) and isinstance(other, AllowDisallowPolicy) and self.name == other.name and \
            self.allowed == other.allowed and self.notAllowed == other.notAllowed

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

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "dcType": self.dcType.name, "name": self.name})
        return rc


class DataClassificationPolicy(AllowDisallowPolicy[DataClassification]):
    """This checks whether a data classification is explicitly allowed or explicitly forbidden"""
    def __init__(self, name: str, doc: Optional[Documentation], allowed: Optional[set[DataClassification]] = None,
                 notAllowed: Optional[set[DataClassification]] = None) -> None:
        super().__init__(name, doc, allowed, notAllowed)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DataClassificationPolicy) and self.allowed == other.allowed and self.notAllowed == other.notAllowed

    def __hash__(self) -> int:
        return super().__hash__()

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc


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

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc
