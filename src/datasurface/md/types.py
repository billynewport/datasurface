"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.lint import UserDSLObject, ValidationTree
from abc import abstractmethod
from typing import Any, Optional, OrderedDict, Type, cast
from enum import Enum


class DataType(UserDSLObject):
    """Base class for all data types. These DataTypes are not nullable. Nullable status is a property of
    columns and is specified in the DDLColumn constructor"""
    def __init__(self) -> None:
        UserDSLObject.__init__(self)
        pass

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other)

    def __str__(self) -> str:
        return str(self.__class__.__name__) + "()"

    @abstractmethod
    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """This method should check if this type is backwards compatible with the other type and add problems to the vTree if not"""

    def isBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        self.checkIfBackwardsCompatibleWith(other, vTree)
        return not vTree.hasErrors()

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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, BoundedDataType):
            o: BoundedDataType = cast(BoundedDataType, other)

            # If this is unlimited then its compatible
            if (self.maxSize is None):
                return

            # Must be unlimited to be compatible with unlimited
            if (self.maxSize and o.maxSize is None):
                vTree.addProblem(f"maxSize has been reduced from unlimited to {self.maxSize}")

            if (self.maxSize == o.maxSize):
                return
            if (o.maxSize and self.maxSize and self.maxSize < o.maxSize):
                vTree.addProblem(f"maxSize has been reduced from {o.maxSize} to {self.maxSize}")


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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        super().checkIfBackwardsCompatibleWith(other, vTree)
        if vTree.checkTypeMatches(other, ArrayType):
            o: ArrayType = cast(ArrayType, other)
            self.dataType.checkIfBackwardsCompatibleWith(o.dataType, vTree)

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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        super().checkIfBackwardsCompatibleWith(other, vTree)
        if vTree.checkTypeMatches(other, MapType):
            o: MapType = cast(MapType, other)
            self.keyType.checkIfBackwardsCompatibleWith(o.keyType, vTree)
            self.valueType.checkIfBackwardsCompatibleWith(o.valueType, vTree)

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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        super().checkIfBackwardsCompatibleWith(other, vTree)
        if vTree.checkTypeMatches(other, StructType):
            o: StructType = cast(StructType, other)
            # Check for removed or modified fields
            for key, value in o.fields.items():
                if key not in self.fields:
                    vTree.addProblem(f"Field {key} has been removed")

            # Check for added or modified fields
            for key, value in self.fields.items():
                if key not in o.fields:
                    vTree.addProblem(f"Field {key} has been added")
                if value.isBackwardsCompatibleWith(o.fields[key], vTree):
                    vTree.addProblem(f"Field {key} is not backwards compatible")

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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        vTree.checkTypeMatches(other, NumericDataType)

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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, FixedIntegerDataType):
            otherFSBDT: FixedIntegerDataType = cast(FixedIntegerDataType, other)
            if (not (self.sizeInBits == otherFSBDT.sizeInBits and self.isSigned == otherFSBDT.isSigned)):
                if (self.sizeInBits < otherFSBDT.sizeInBits):
                    vTree.addProblem(f"Size has been reduced from {otherFSBDT.sizeInBits} to {self.sizeInBits}")
                if (self.isSigned != otherFSBDT.isSigned):
                    vTree.addProblem(f"Signedness has changed from {otherFSBDT.isSigned} to {self.isSigned}")
                super().checkIfBackwardsCompatibleWith(other, vTree)

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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, CustomFloat):
            otherCF: CustomFloat = cast(CustomFloat, other)

            # Can this object be stored without precision loss in otherCF
            if otherCF.isRepresentableBy(self):
                super().checkIfBackwardsCompatibleWith(other, vTree)
            else:
                vTree.addProblem("New Type loses precision on current type")

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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Must be same type and batchSize, scaleType and elementType must be backwards compatible with other"""
        if vTree.checkTypeMatches(other, self.__class__):
            otherMSCF: MicroScaling_CustomFloat = cast(MicroScaling_CustomFloat, other)
            if (self.batchSize < otherMSCF.batchSize):
                vTree.addProblem(f"Batch size has been reduced from {otherMSCF.batchSize} to {self.batchSize}")
            if (self.scaleType != otherMSCF.scaleType):
                vTree.addProblem(f"Scale type has changed from {otherMSCF.scaleType} to {self.scaleType}")
            if (self.elementType != otherMSCF.elementType):
                vTree.addProblem(f"Element type has changed from {otherMSCF.elementType} to {self.elementType}")
            super().checkIfBackwardsCompatibleWith(other, vTree)
        super().checkIfBackwardsCompatibleWith(other, vTree)

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)
        if (self.batchSize <= 0):
            vTree.addProblem("Batch size must be > 0")

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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Decimal):
            otherD: Decimal = cast(Decimal, other)
            if (self.precision < otherD.precision):
                vTree.addProblem(f"Precision has been reduced from {otherD.precision} to {self.precision}")
            super().checkIfBackwardsCompatibleWith(other, vTree)

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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Timestamps are backwards compatible with timestamps and dates."""
        super().checkIfBackwardsCompatibleWith(other, vTree)
        if vTree.checkTypeMatches(other, Timestamp, Date):
            if isinstance(other, Timestamp):
                if self.timeZone != other.timeZone:
                    vTree.addProblem(f"Timezone has changed from {other.timeZone} to {self.timeZone}")


class Date(TemporalDataType):
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Date)

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        super().checkIfBackwardsCompatibleWith(other, vTree)
        vTree.checkTypeMatches(other, Date)


class Interval(TemporalDataType):
    """This is a time interval defined as number of months, number of days and number of milliseconds. Each number is a 32 bit signed integer"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Interval)

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Interval):
            super().checkIfBackwardsCompatibleWith(other, vTree)


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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, UniCodeType):
            super().checkIfBackwardsCompatibleWith(other, vTree)


class NonUnicodeString(TextDataType):
    """Base class for non unicode datatypes with collation"""
    def __init__(self, maxSize: Optional[int], collationString: Optional[str]) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, NonUnicodeString)

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, NonUnicodeString):
            super().checkIfBackwardsCompatibleWith(other, vTree)


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

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Boolean):
            super().checkIfBackwardsCompatibleWith(other, vTree)

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)


class Variant(BoundedDataType):
    """JSON type datatype"""
    def __init__(self, maxSize: Optional[int] = None) -> None:
        super().__init__(maxSize)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Variant)

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Variant):
            super().checkIfBackwardsCompatibleWith(other, vTree)


class Binary(BoundedDataType):
    """Binary blob"""
    def __init__(self, maxSize: Optional[int] = None) -> None:
        super().__init__(maxSize)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, Binary)

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Binary):
            super().checkIfBackwardsCompatibleWith(other, vTree)


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


class GeometryType(Enum):
    """Spatial geometry types supported by Geography"""
    POINT = "POINT"
    LINESTRING = "LINESTRING"
    POLYGON = "POLYGON"
    MULTIPOINT = "MULTIPOINT"
    MULTILINESTRING = "MULTILINESTRING"
    MULTIPOLYGON = "MULTIPOLYGON"
    GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION"


class SpatialReferenceSystem:
    """Spatial Reference System (SRID) definition for geographic coordinates"""
    def __init__(self, srid: int, name: str = "") -> None:
        self.srid: int = srid
        """Spatial Reference System Identifier (e.g., 4326 for WGS84)"""
        self.name: str = name
        """Human readable name for the SRS"""

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "srid": self.srid, "name": self.name}

    def __eq__(self, other: object) -> bool:
        return isinstance(other, SpatialReferenceSystem) and self.srid == other.srid and self.name == other.name

    def __str__(self) -> str:
        if self.name:
            return f"{self.__class__.__name__}({self.srid}, '{self.name}')"
        return f"{self.__class__.__name__}({self.srid})"


class Geography(DataType):
    """Geographic/spatial data type for storing geometric objects with coordinate systems.

    Supports standard OGC geometry types (Point, LineString, Polygon, etc.) with
    spatial reference system information. Can optionally constrain to specific
    geometry types and has configurable maximum storage size.
    """

    def __init__(self,
                 srs: SpatialReferenceSystem = SpatialReferenceSystem(4326, "WGS84"),
                 geometryType: Optional[GeometryType] = None,
                 maxSize: Optional[int] = None) -> None:
        """
        Creates a Geography data type.

        Args:
            srs: Spatial reference system (defaults to WGS84)
            geometryType: Optional constraint to specific geometry type (Point, Polygon, etc.)
            maxSize: Optional maximum storage size in bytes
        """
        super().__init__()
        self.srs: SpatialReferenceSystem = srs
        self.geometryType: Optional[GeometryType] = geometryType
        self.maxSize: Optional[int] = maxSize

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"srs": self.srs.to_json()})
        if self.geometryType is not None:
            rc.update({"geometryType": self.geometryType.value})
        if self.maxSize is not None:
            rc.update({"maxSize": self.maxSize})
        return rc

    def lint(self, vTree: ValidationTree) -> None:
        super().lint(vTree)
        if self.maxSize is not None and self.maxSize <= 0:
            vTree.addProblem("Max size must be > 0")
        if self.srs.srid < 0:
            vTree.addProblem("SRID must be >= 0")

    def __eq__(self, other: object) -> bool:
        return (super().__eq__(other) and isinstance(other, Geography) and
                self.srs == other.srs and
                self.geometryType == other.geometryType and
                self.maxSize == other.maxSize)

    def checkIfBackwardsCompatibleWith(self, other: 'DataType', vTree: ValidationTree) -> None:
        """Geography types are backwards compatible if SRS matches and geometry types are compatible"""
        if vTree.checkTypeMatches(other, Geography):
            otherGeo: Geography = cast(Geography, other)

            # SRS must match exactly
            if self.srs != otherGeo.srs:
                vTree.addProblem(f"Spatial reference system has changed from {otherGeo.srs} to {self.srs}")

            # Geometry type constraint compatibility
            if self.geometryType != otherGeo.geometryType:
                if otherGeo.geometryType is None and self.geometryType is not None:
                    vTree.addProblem(f"Geometry type constraint added: {self.geometryType}")
                elif otherGeo.geometryType is not None and self.geometryType is None:
                    # This is OK - removing constraint is compatible
                    pass
                else:
                    vTree.addProblem(f"Geometry type has changed from {otherGeo.geometryType} to {self.geometryType}")

            # Size constraint compatibility
            if self.maxSize is not None and otherGeo.maxSize is not None:
                if self.maxSize < otherGeo.maxSize:
                    vTree.addProblem(f"Max size has been reduced from {otherGeo.maxSize} to {self.maxSize}")
            elif self.maxSize is not None and otherGeo.maxSize is None:
                vTree.addProblem(f"Max size constraint added: {self.maxSize}")

    def __str__(self) -> str:
        parts = []
        if self.geometryType is not None:
            parts.append(f"geometryType={self.geometryType.value}")
        if self.maxSize is not None:
            parts.append(f"maxSize={self.maxSize}")
        if self.srs.srid != 4326:  # Only show if not default WGS84
            parts.append(f"srs={self.srs}")

        if parts:
            return f"{self.__class__.__name__}({', '.join(parts)})"
        return f"{self.__class__.__name__}()"
