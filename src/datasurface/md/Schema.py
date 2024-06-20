"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from typing import Any, List, Optional, OrderedDict, Type, Union, cast
from abc import ABC, abstractmethod
from enum import Enum
import json

from datasurface.md.Policy import DataClassification, DataClassificationPolicy

from .Lint import ValidationProblem, ValidationTree
from .utils import ANSI_SQL_NamedObject, is_valid_sql_identifier
from .Documentation import Documentable, Documentation


def handleUnsupportedObjectsToJson(obj: object) -> str:
    if isinstance(obj, Enum):
        return obj.name
    elif isinstance(obj, DataType):
        return obj.to_json()
    raise Exception(f"Unsupported object {obj} in to_json")


class DataType(ABC):
    """Base class for all data types"""
    def __init__(self) -> None:
        pass

    def __eq__(self, __value: object) -> bool:
        return isinstance(self, DataType) and type(self) is type(__value)

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

    def to_json(self) -> str:
        """Converts this object to a JSON string"""
        d: dict[str, Any] = dict(self.__dict__)
        d["type"] = self.__class__.__name__

        return json.dumps(d, default=handleUnsupportedObjectsToJson)


class BoundedDataType(DataType):
    def __init__(self, maxSize: Optional[int]) -> None:
        super().__init__()
        self.maxSize: Optional[int] = maxSize

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)
        if (self.maxSize is not None and self.maxSize <= 0):
            vTree.addProblem("Max size must be > 0")

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, BoundedDataType) and self.maxSize == __value.maxSize

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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, ArrayType) and self.dataType == __value.dataType

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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, MapType) and \
            self.keyType == __value.keyType and self.valueType == __value.valueType

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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, StructType) and self.fields == __value.fields

    def isBackwardsCompatibleWith(self, other: DataType, vTree: ValidationTree) -> bool:
        rc: bool = super().isBackwardsCompatibleWith(other, vTree)
        if rc and isinstance(other, StructType):
            for key, value in self.fields.items():
                if key in other.fields:
                    if not value.isBackwardsCompatibleWith(other.fields[key], vTree):
                        vTree.addProblem(f"Field {key} is not backwards compatible")
                        rc = False
                else:
                    vTree.addProblem(f"Field {key} is not present in other")
                    rc = False
        else:
            rc = False
        return rc

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

    def __str__(self) -> str:
        if (self.maxSize is None and self.collationString is None):
            return str(self.__class__.__name__) + "()"
        elif (self.maxSize is None and self.collationString is not None):
            return str(self.__class__.__name__) + f"(collationString='{self.collationString}')"
        elif (self.maxSize is not None and self.collationString is None):
            return str(self.__class__.__name__) + f"({self.maxSize})"
        else:
            return str(self.__class__.__name__) + f"({self.maxSize}, '{self.collationString}')"

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TextDataType) and self.collationString == __value.collationString

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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, NumericDataType)


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

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)
        if (self.sizeInBits <= 0):
            vTree.addProblem("Size must be > 0")

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FixedIntegerDataType) and self.sizeInBits == __value.sizeInBits and \
                self.isSigned == __value.isSigned

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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TinyInt)

    def __hash__(self) -> int:
        return hash(str(self))


class SmallInt(FixedIntegerDataType):
    """16 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(16, SignedOrNot.SIGNED)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, SmallInt)

    def __hash__(self) -> int:
        return hash(str(self))


class Integer(FixedIntegerDataType):
    """32 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(32, SignedOrNot.SIGNED)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Integer)

    def __hash__(self) -> int:
        return hash(str(self))


class BigInt(FixedIntegerDataType):
    """64 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(64, SignedOrNot.SIGNED)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, BigInt)

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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, CustomFloat) and self.maxExponent == __value.maxExponent and \
            self.minExponent == __value.minExponent and self.precision == __value.precision and \
            self.nonFiniteBehavior == __value.nonFiniteBehavior and self.nanEncoding == __value.nanEncoding

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


class IEEE16(CustomFloat):
    """Half precision IEEE754"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=16, precision=11, maxExponent=15, minExponent=-14)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, IEEE16)


class IEEE32(CustomFloat):
    """IEEE754 32 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=32, precision=24, maxExponent=127, minExponent=-126)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, IEEE32)


class Float(IEEE32):
    """Alias 32 bit IEEE floating point number"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Float)


class IEEE64(CustomFloat):
    """IEEE754 64 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=64, precision=53, maxExponent=1023, minExponent=-1022)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, IEEE64)


class Double(IEEE64):
    """Alias for IEEE64"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Double)


class SimpleCustomFloat(CustomFloat):
    """This is a CustomFloat but uses a simplified str method which is just the class name rather than the fully
    specified CustomFloat. It prevents all standard types needing to implement their own str method."""
    def __init__(self, maxExponent: int, minExponent: int, precision: int, sizeInBits: int,
                 nonFiniteBehavior: NonFiniteBehavior = NonFiniteBehavior.IEEE754, nanEncoding: FloatNanEncoding = FloatNanEncoding.IEEE) -> None:
        super().__init__(maxExponent, minExponent, precision, sizeInBits, nonFiniteBehavior, nanEncoding)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class IEEE128(SimpleCustomFloat):
    """IEEE754 128 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=128, precision=113, maxExponent=16383, minExponent=-16382)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, IEEE128)


class IEEE256(SimpleCustomFloat):
    """IEEE754 256 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=256, precision=237, maxExponent=262143, minExponent=-262142)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, IEEE256)


class FP8_E4M3(SimpleCustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=8, precision=3, maxExponent=15, minExponent=-14)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FP8_E4M3)


class FP6_E2M3(SimpleCustomFloat):
    """IEEE 6 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=6, precision=3, maxExponent=3, minExponent=-2)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FP6_E2M3)


class FP6_E3M2(SimpleCustomFloat):
    """IEEE 6 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=6, precision=2, maxExponent=7, minExponent=-6)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FP6_E3M2)


class FP4_E2M1(SimpleCustomFloat):
    """IEEE 4 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=4, precision=1, maxExponent=3, minExponent=-2)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FP4_E2M1)


class FP8_E5M2(SimpleCustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=8, precision=2, maxExponent=31, minExponent=-30)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FP8_E5M2)


class FP8_E5M2FNUZ(SimpleCustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=8, precision=3, maxExponent=15, minExponent=-15, nonFiniteBehavior=NonFiniteBehavior.NanOnly,
                         nanEncoding=FloatNanEncoding.NegativeZero)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FP8_E5M2FNUZ)


class FP8_E4M3FNUZ(SimpleCustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=8, precision=4, maxExponent=7, minExponent=-7, nonFiniteBehavior=NonFiniteBehavior.NanOnly,
                         nanEncoding=FloatNanEncoding.NegativeZero)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FP8_E4M3FNUZ)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class FP8_E8M0(SimpleCustomFloat):
    """Open Compute 8 bit exponent only number, this is typically used as a scaling factor in micro scale floating point types"""
    def __init__(self) -> None:
        super().__init__(sizeInBits=8, precision=0, maxExponent=255, minExponent=-254)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FP8_E8M0)


class MicroScaling_CustomFloat(NumericDataType):
    """Represents an array of numbers of size batchSize where each element is scaled by a common factor
    with type scaleType. The element type is elementType. This is intended for machine learning applications which
    can use this type of floating point number specification"""
    def __init__(self, batchSize: int, scaleType: Type[CustomFloat], elementType: Type[CustomFloat]) -> None:
        super().__init__()
        self.batchSize: int = batchSize
        self.scaleType: Type[CustomFloat] = scaleType
        self.elementType: Type[CustomFloat] = elementType

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, MicroScaling_CustomFloat) and self.batchSize == __value.batchSize and \
            self.scaleType == __value.scaleType and self.elementType == __value.elementType

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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, MXFP8_E4M3)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class MXFP8_E5M2(MicroScaling_CustomFloat):
    """MicroScaling 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(batchSize=32, scaleType=FP8_E8M0, elementType=FP8_E5M2)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, MXFP8_E5M2)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class MXFP6_E2M3(MicroScaling_CustomFloat):
    """MicroScaling 6 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(batchSize=32, scaleType=FP8_E8M0, elementType=FP6_E2M3)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, MXFP6_E2M3)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class MXFP6_E3M2(MicroScaling_CustomFloat):
    """MicroScaling 6 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(batchSize=32, scaleType=FP8_E8M0, elementType=FP6_E3M2)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, MXFP6_E3M2)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class MXFP4_E2M1(MicroScaling_CustomFloat):
    """MicroScaling 4 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(batchSize=32, scaleType=FP8_E8M0, elementType=FP4_E2M1)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, MXFP4_E2M1)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class Decimal(BoundedDataType):
    """Signed Fixed decimal number with fixed size fraction"""
    def __init__(self, maxSize: int, precision: int) -> None:
        super().__init__(maxSize)
        self.precision: int = precision

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)
        if (self.precision < 0):
            vTree.addProblem("Precision must be >= 0")
        if (self.maxSize is None):
            vTree.addProblem("Decimal must have a maximum size")
        else:
            if (self.precision > self.maxSize):
                vTree.addProblem("Precision must be <= maxSize")

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Decimal) and self.precision == __value.precision

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

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, TimeZone) and self.timeZone == __value.timeZone

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.timeZone})"


class TemporalDataType(DataType):
    """Base class for all temporal data types"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TemporalDataType)

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)


class Timestamp(TemporalDataType):
    """Timestamp with microsecond precision, this includes a Date and a timestamp with microsecond precision"""
    def __init__(self, tz: TimeZone = TimeZone("UTC")) -> None:
        super().__init__()
        self.timeZone: TimeZone = tz

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Timestamp) and self.timeZone == __value.timeZone

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        vTree.checkTypeMatches(other, Timestamp, Date)
        if isinstance(other, Timestamp) and self.timeZone != other.timeZone:
            vTree.addProblem(f"Timezone has changed from {other.timeZone} to {self.timeZone}")
        return not vTree.hasErrors()


class Date(TemporalDataType):
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Date)

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        vTree.checkTypeMatches(other, Date)
        return not vTree.hasErrors()


class Interval(TemporalDataType):
    """This is a time interval defined as number of months, number of days and number of milliseconds. Each number is a 32 bit signed integer"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Interval)

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Interval):
            super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()


class UniCodeType(TextDataType):
    """Base class for unicode datatypes"""
    def __init__(self, maxSize: Optional[int], collationString: Optional[str]) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, UniCodeType)

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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, NonUnicodeString)

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, NonUnicodeString):
            super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()


class VarChar(NonUnicodeString):
    """Variable length non unicode string with maximum size"""
    def __init__(self, maxSize: Optional[int] = None, collationString: Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, VarChar)


class NVarChar(UniCodeType):
    """Variable length unicode string with maximum size"""
    def __init__(self, maxSize: Optional[int] = None, collationString: Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, NVarChar)


class String(NVarChar):
    """Alias for NVarChar"""
    def __init__(self, maxSize: Optional[int] = None, collationString: Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, String)


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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Char)

    def __str__(self) -> str:
        sz: int = 1 if self.maxSize is None else self.maxSize
        return strForFixedSizeString(self.__class__.__name__, sz, self.collationString)


class NChar(UniCodeType):
    """Unicode fixed length character string"""
    def __init__(self, maxSize: int = 1, collationString: Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, NChar)

    def __str__(self) -> str:
        sz: int = 1 if self.maxSize is None else self.maxSize
        return strForFixedSizeString(self.__class__.__name__, sz, self.collationString)


class Boolean(DataType):
    """Boolean value"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Boolean)

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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Variant)

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Variant):
            super().isBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()


class Binary(BoundedDataType):
    """Binary blob"""
    def __init__(self, maxSize: Optional[int] = None) -> None:
        super().__init__(maxSize)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Binary)

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


class DDLColumn(ANSI_SQL_NamedObject, Documentable):
    """This is an individual attribute within a DDLTable schema"""
    def __init__(self, name: str, dataType: DataType, *args: Union[NullableStatus, DataClassification, PrimaryKeyStatus, Documentation]) -> None:
        super().__init__(name)
        Documentable.__init__(self, None)
        self.type: DataType = dataType
        self.primaryKey: PrimaryKeyStatus = DEFAULT_primaryKey
        self.classification: Optional[list[DataClassification]] = None
        self.nullable: NullableStatus = DEFAULT_nullable
        self.add(*args)

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


class AttributeList:
    """A list of column names."""
    def __init__(self, colNames: list[str]) -> None:
        self.colNames: List[str] = []
        for col in colNames:
            self.colNames.append(col)

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, AttributeList) and self.colNames == __value.colNames

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

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, PrimaryKeyList)


class PartitionKeyList(AttributeList):
    """A list of column names used for partitioning ingested data"""
    def __init__(self, colNames: list[str]) -> None:
        super().__init__(colNames)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, PartitionKeyList)


class NotBackwardsCompatible(ValidationProblem):
    """This is a validation problem that indicates that the schema is not backwards compatible"""
    def __init__(self, problem: str) -> None:
        super().__init__(problem)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, NotBackwardsCompatible)

    def __hash__(self) -> int:
        return hash(str(self))


class Schema(ABC, Documentable):
    """This is a basic schema in the system. It has base meta attributes common for all schemas and core methods for all schemas"""
    def __init__(self) -> None:
        Documentable.__init__(self, None)
        self.primaryKeyColumns: Optional[PrimaryKeyList] = None
        self.ingestionPartitionColumns: Optional[PartitionKeyList] = None
        """How should this dataset be partitioned for ingestion and storage"""

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
            self.primaryKeyColumns.lint(tree.addSubTree(pkTree))
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
