from typing import List, Optional, OrderedDict, Union, cast
from abc import ABC, abstractmethod
from enum import Enum

from .Lint import ValidationTree
from .utils import ANSI_SQL_NamedObject, Policy, is_valid_sql_identifier
from .Documentation import Documentation

class DataType(ABC):
    """Base class for all data types"""
    def __init__(self) -> None:
        pass

    def __eq__(self, __value: object) -> bool:
        return isinstance(self, DataType) and type(self) == type(__value)

    def __str__(self) -> str:
        return str(self.__class__.__name__) + "()"

    @abstractmethod    
    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        return True
    
    @abstractmethod
    def lint(self, vTree : ValidationTree) -> None:
        """This method is called to lint the data type. All validation of parameters should
        be here and not in the constructor"""
        pass
    
class BoundedDataType(DataType):
    def __init__(self, maxSize : Optional[int]) -> None:
        super().__init__()
        self.maxSize : Optional[int] = maxSize

    def lint(self, vTree : ValidationTree) -> None:
        super().lint(vTree)
        if(self.maxSize != None and self.maxSize <= 0):
            vTree.addProblem("Max size must be > 0")

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, BoundedDataType) and self.maxSize == __value.maxSize
    
    def __str__(self) -> str:
        if(self.maxSize == None):
            return str(self.__class__.__name__) + "()"
        else:
            return str(self.__class__.__name__) + f"({self.maxSize})"
        
    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if(isinstance(other, BoundedDataType) == False):
            vTree.addProblem(f"Cannot compare {self.__class__.__name__} with {other.__class__.__name__}")
            return False
        otherBT : BoundedDataType = cast(BoundedDataType, other)

        # If this is unlimited then its compatible
        if(self.maxSize == None):
            return True

        # Must be unlimited to be compatible with unlimited
        if(self.maxSize and otherBT.maxSize == None):
            vTree.addProblem(f"maxSize has been reduced from unlimited to {self.maxSize}")
                
        if(self.maxSize == otherBT.maxSize):
            return True
        if(otherBT.maxSize and self.maxSize and self.maxSize < otherBT.maxSize):
            vTree.addProblem(f"maxSize has been reduced from {otherBT.maxSize} to {self.maxSize}")
        return vTree.hasErrors() == False

class TextDataType(BoundedDataType):
    def __init__(self, maxSize : Optional[int], collationString : Optional[str]) -> None:
        super().__init__(maxSize)
        self.collationString : Optional[str] = collationString
        """The collation and/or character encoding for this string. Unicode strings
        can have a collation but it is not required. Non unicode strings must have a collation"""


    def __str__(self) -> str:
        if(self.maxSize == None and self.collationString == None):
            return str(self.__class__.__name__) + "()"
        elif(self.maxSize == None and self.collationString != None):
            return str(self.__class__.__name__) + f"(collationString='{self.collationString}')"
        elif(self.maxSize != None and self.collationString == None):
            return str(self.__class__.__name__) + f"({self.maxSize})"
        else:
            return str(self.__class__.__name__) + f"({self.maxSize}, '{self.collationString}')"
        
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TextDataType)
    
    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, TextDataType):
            otherTD : TextDataType = cast(TextDataType, other)
            if(self.collationString != otherTD.collationString):
                vTree.addProblem(f"Collation has changed from {otherTD.collationString} to {self.collationString}")
            super().isBackwardsCompatibleWith(other, vTree)
        return vTree.hasErrors() == False

class NumericDataType(DataType):
    """Base class for all numeric data types"""
    def __init__(self) -> None:
        super().__init__()

    def isBackwardsCompatibleWith(self, other: DataType, vTree : ValidationTree) -> bool:
        vTree.checkTypeMatches(other, NumericDataType)
        return vTree.hasErrors() == False

class SignedOrNot(Enum):
    SIGNED = 0
    UNSIGNED = 1

class FixedSizeBinaryDataType(NumericDataType):
    """"""
    def __init__(self, sizeInBits : int, isSigned : SignedOrNot) -> None:
        super().__init__()
        self.sizeInBits : int = sizeInBits
        """Number of bits including sign bit if present"""
        self.isSigned : SignedOrNot = isSigned
        """Is this a signed integer"""

    def lint(self, vTree : ValidationTree) -> None:
        super().lint(vTree)
        if(self.sizeInBits <= 0):
            vTree.addProblem("Size must be > 0")

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FixedSizeBinaryDataType) and self.sizeInBits == __value.sizeInBits and self.isSigned == __value.isSigned

    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, FixedSizeBinaryDataType):
            otherFSBDT : FixedSizeBinaryDataType = cast(FixedSizeBinaryDataType, other)
            if(not (self.sizeInBits == otherFSBDT.sizeInBits and self.isSigned == otherFSBDT.isSigned)):
                if(self.sizeInBits < otherFSBDT.sizeInBits):
                    vTree.addProblem(f"Size has been reduced from {otherFSBDT.sizeInBits} to {self.sizeInBits}")
                if(self.isSigned != otherFSBDT.isSigned):
                    vTree.addProblem(f"Signedness has changed from {otherFSBDT.isSigned} to {self.isSigned}")
                super().isBackwardsCompatibleWith(other, vTree)
        return vTree.hasErrors() == False
    
class TinyInt(FixedSizeBinaryDataType):
    """8 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(8, SignedOrNot.SIGNED)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TinyInt)

class SmallInt(FixedSizeBinaryDataType):
    """16 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(16, SignedOrNot.SIGNED)
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, SmallInt)

class Integer(FixedSizeBinaryDataType):
    """32 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(32, SignedOrNot.SIGNED)
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Integer)

class BigInt(FixedSizeBinaryDataType):
    """64 bit signed integer"""
    def __init__(self) -> None:
        super().__init__(64, SignedOrNot.SIGNED)
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, BigInt)

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
    def __init__(self, maxExponent : int, minExponent : int, precision : int, sizeInBits : int, nonFiniteBehavior : NonFiniteBehavior = NonFiniteBehavior.IEEE754, nanEncoding : FloatNanEncoding = FloatNanEncoding.IEEE) -> None:
        """Creates a custom floating point number with the given exponent range and precision"""
        super().__init__()
        self.sizeInBits : int = sizeInBits
        self.maxExponent : int = maxExponent
        self.minExponent : int = minExponent
        self.precision : int = precision
        """Number of bits in the significand, this includes the sign bit"""
        self.nonFiniteBehavior : NonFiniteBehavior = nonFiniteBehavior
        self.nanEncoding : FloatNanEncoding = nanEncoding

    def lint(self, vTree : ValidationTree) -> None:
        super().lint(vTree)
        if(self.sizeInBits <= 0):
            vTree.addProblem("Size must be > 0")
        if(self.maxExponent <= 0):
            vTree.addProblem("Max exponent must be > 0")
        if(self.minExponent >= 0):
            vTree.addProblem("Min exponent must be < 0")
        if(self.precision <= 0):
            vTree.addProblem("Precision must be > 0")
        if(self.maxExponent <= self.minExponent):
            vTree.addProblem("Max exponent must be > min exponent")
        if(self.precision > self.sizeInBits):
            vTree.addProblem("Precision must be <= size in bits")
        if(self.nonFiniteBehavior == NonFiniteBehavior.NanOnly and self.nanEncoding == FloatNanEncoding.NegativeZero):
            vTree.addProblem("Non finite behavior cannot be NanOnly and nan encoding cannot be NegativeZero")

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, CustomFloat) and self.maxExponent == __value.maxExponent and self.minExponent == __value.minExponent and self.precision == __value.precision and self.nonFiniteBehavior == __value.nonFiniteBehavior and self.nanEncoding == __value.nanEncoding

    def isRepresentableBy(self, other : 'CustomFloat') -> bool:
        """Returns true if this float can be represented by the other float excluding non finite behaviors"""
        return self.maxExponent <= other.maxExponent and self.minExponent >= other.minExponent and self.precision <= other.precision

    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, CustomFloat):
            otherCF : CustomFloat = cast(CustomFloat, other)

            # Can this object be stored without precision loss in otherCF
            if otherCF.isRepresentableBy(self):
                return super().isBackwardsCompatibleWith(other, vTree)
            vTree.addProblem("New Type loses precision on current type")
        return vTree.hasErrors() == False
    
class IEEE16(CustomFloat):
    """Half precision IEEE754"""
    def __init__(self) -> None:
        super().__init__(sizeInBits = 16, precision = 11, maxExponent = 15, minExponent = -14)

class IEEE32(CustomFloat):
    """IEEE754 32 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits = 32, precision = 24, maxExponent = 127, minExponent = -126)

class Float(IEEE32):
    """Alias 32 bit IEEE floating point number"""
    def __init__(self) -> None:
        super().__init__()

class IEEE64(CustomFloat):
    """IEEE754 64 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits = 64, precision = 53, maxExponent = 1023, minExponent = -1022)

class Double(IEEE64):
    """Alias for IEEE64"""
    def __init__(self) -> None:
        super().__init__()

class IEEE128(CustomFloat):
    """IEEE754 128 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits = 128, precision = 113, maxExponent = 16383, minExponent = -16382)

class IEEE256(CustomFloat):
    """IEEE754 256 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits = 256, precision = 237, maxExponent = 262143, minExponent = -262142)

class FP8_E4M3(CustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits = 8, precision = 3, maxExponent = 15, minExponent = -14)

class FP8_E5M2(CustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits = 8, precision = 3, maxExponent = 15, minExponent = -14)

class FP8_E5M2FNUZ(CustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits = 8, precision = 3, maxExponent = 15, minExponent = -15, nonFiniteBehavior = NonFiniteBehavior.NanOnly, nanEncoding = FloatNanEncoding.NegativeZero)

class FP8_E4M3FNUZ(CustomFloat):
    """IEEE 8 bit floating point number"""
    def __init__(self) -> None:
        super().__init__(sizeInBits = 8, precision = 4, maxExponent = 7, minExponent = -7, nonFiniteBehavior = NonFiniteBehavior.NanOnly, nanEncoding = FloatNanEncoding.NegativeZero)

class Decimal(BoundedDataType):
    """Signed Fixed decimal number with fixed size fraction"""
    def __init__(self, maxSize : int, precision : int) -> None:
        super().__init__(maxSize)
        self.precision : int = precision

    def lint(self, vTree : ValidationTree) -> None:
        super().lint(vTree)
        if(self.precision < 0):
            vTree.addProblem("Precision must be >= 0")
        if(self.maxSize == None):
            vTree.addProblem("Decimal must have a maximum size")
        else:
            if(self.precision > self.maxSize):
                vTree.addProblem("Precision must be <= maxSize")

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Decimal) and self.precision == __value.precision        

    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Decimal):
            otherD : Decimal = cast(Decimal, other)
            if(self.precision < otherD.precision):
                vTree.addProblem(f"Precision has been reduced from {otherD.precision} to {self.precision}")
            return super().isBackwardsCompatibleWith(other, vTree)
        return vTree.hasErrors() == False
    
    def __str__(self) -> str:
        if(self.maxSize == None):
            raise Exception("Decimal must have a maximum size")
        return str(self.__class__.__name__) + f"({self.maxSize},{self.precision})"
    
class TemporalDataType(DataType):
    """Base class for all temporal data types"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TemporalDataType)
    
    def lint(self, vTree : ValidationTree) -> None:
        super().lint(vTree)

class Timestamp(TemporalDataType):
    """Timestamp with microsecond precision, this includes a Date and a timestamp with microsecond precision"""
    def __init__(self) -> None:
        super().__init__()

    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        vTree.checkTypeMatches(other, Timestamp, Date)
        return vTree.hasErrors() == False

class Date(TemporalDataType):
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Date)
    
    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        vTree.checkTypeMatches(other, Date)
        return vTree.hasErrors() == False

class Interval(TemporalDataType):
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Interval)
    
    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Interval):
            super().isBackwardsCompatibleWith(other, vTree)
        return vTree.hasErrors() == False

class UniCodeType(TextDataType):
    """Base class for unicode datatypes"""
    def __init__(self, maxSize : Optional[int], collationString : Optional[str]) -> None:
        super().__init__(maxSize, collationString)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, UniCodeType) and self.maxSize == __value.maxSize
    

    def __str__(self) -> str:
        if(self.maxSize == None and self.collationString == None):
            return str(self.__class__.__name__) + "()"
        elif(self.maxSize == None):
            return str(self.__class__.__name__) + f"(collationString='{self.collationString}')"
        elif(self.collationString == None):
            return str(self.__class__.__name__) + f"({self.maxSize})"
        else:
            return str(self.__class__.__name__) + f"({self.maxSize}, '{self.collationString}')"
            
        
    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, UniCodeType):
            super().isBackwardsCompatibleWith(other, vTree)
        return vTree.hasErrors() == False

class NonUnicodeString(TextDataType):
    """Base class for non unicode datatypes with collation"""
    def __init__(self, maxSize: Optional[int], collationString : Optional[str]) -> None:
        super().__init__(maxSize, collationString)

    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, NonUnicodeString):
            super().isBackwardsCompatibleWith(other, vTree)
        return vTree.hasErrors() == False

class VarChar(NonUnicodeString):
    """Variable length non unicode string with maximum size"""
    def __init__(self, maxSize : Optional[int] = None, collationString : Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

class NVarChar(UniCodeType):
    """Variable length unicode string with maximum size"""
    def __init__(self, maxSize : Optional[int] = None, collationString : Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

class String(NVarChar):
    """Alias for NVarChar"""
    def __init__(self, maxSize : Optional[int] = None, collationString : Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

def strForFixedSizeString(clsName : str, maxSize : int, collationString : Optional[str]) -> str:
    if(maxSize == 1 and collationString == None):
        return str(clsName) + "()"
    elif(collationString == None):
        return str(clsName) + f"({maxSize})"
    else:
        return str(clsName) + f"({maxSize}, '{collationString}')"

class Char(TextDataType):
    """Non unicode fixed length character string"""
    def __init__(self, maxSize : int = 1, collationString : Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)
    def __str__(self) -> str:
        sz : int = 1 if self.maxSize == None else self.maxSize
        return strForFixedSizeString(self.__class__.__name__, sz, self.collationString)


class NChar(UniCodeType):
    """Unicode fixed length character string"""
    def __init__(self, maxSize : int = 1, collationString : Optional[str] = None) -> None:
        super().__init__(maxSize, collationString)

    def __str__(self) -> str:
        sz : int = 1 if self.maxSize == None else self.maxSize
        return strForFixedSizeString(self.__class__.__name__, sz, self.collationString)

class Boolean(DataType):
    """Boolean value"""
    def __init__(self) -> None:
        super().__init__()

    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Boolean):
            super().isBackwardsCompatibleWith(other, vTree)
        return vTree.hasErrors() == False
    
    def lint(self, vTree : ValidationTree) -> None:
        super().lint(vTree)

class Variant(BoundedDataType):
    """JSON type datatype"""
    def __init__(self, maxSize : Optional[int] = None) -> None:
        super().__init__(maxSize)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Variant)
    
    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Variant):
            super().isBackwardsCompatibleWith(other, vTree)
        return vTree.hasErrors() == False

class Binary(BoundedDataType):
    """Binary blob"""
    def __init__(self, maxSize : Optional[int] = None) -> None:
        super().__init__(maxSize)

    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Binary):
            super().isBackwardsCompatibleWith(other, vTree)
        return vTree.hasErrors() == False

class Vector(DataType):
    """Fixed length vector of floats for machine learning"""
    def __init__(self, dimensions : int) -> None:
        super().__init__()
        self.dimensions : int = dimensions

    def lint(self, vTree : ValidationTree) -> None:
        super().lint(vTree)
        if(self.dimensions <= 0):
            vTree.addProblem("Dimensions must be > 0")

    def __str__(self) -> str:
        return str(self.__class__.__name__) + f"({self.dimensions})"
    
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, Vector) and self.dimensions == __value.dimensions

    def isBackwardsCompatibleWith(self, other : 'DataType', vTree : ValidationTree) -> bool:
        """Returns true if this data type is backwards compatible with the other data type"""
        if vTree.checkTypeMatches(other, Vector):
            otherV : Vector = cast(Vector, other)
            if(self.dimensions < otherV.dimensions):
                vTree.addProblem(f"Dimensions has been reduced from {otherV.dimensions} to {self.dimensions}")
            else:
                super().isBackwardsCompatibleWith(other, vTree)
        return vTree.hasErrors() == False

class DataClassification(Enum):
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
    """Non material public information"""
    CSI = 6
    """Sensitive confidential information"""
    PC3 = 7
    """Personal confidential information, social security numbers, credit card numbers, etc."""

class DataClassificationPolicy(Policy[DataClassification]):
    """This checks whether a data classification is explicitly allowed or explicitly forbidden"""
    def __init__(self, allowed : Optional[set[DataClassification]] = None, notAllowed : Optional[set[DataClassification]] = None) -> None:
        self.allowed : Optional[set[DataClassification]] = allowed
        self.notAllowed : Optional[set[DataClassification]] = notAllowed

    def isCompatible(self, obj : DataClassification) -> bool:
        if self.allowed and not obj in self.allowed:
            return False
        if self.notAllowed and obj in self.notAllowed:
            return False
        return True

class VerifyNoPrivacyDataVerify(DataClassificationPolicy):
    def __init__(self) -> None:
        super().__init__(None, {DataClassification.PC1, DataClassification.PC2, DataClassification.CPI, DataClassification.MNPI, DataClassification.CSI, DataClassification.PC3})

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

DEFAULT_classification : DataClassification = DataClassification.PC3
DEFAULT_primaryKey : PrimaryKeyStatus = PrimaryKeyStatus.NOT_PK
DEFAULT_nullable : NullableStatus = NullableStatus.NULLABLE

class DDLColumn(ANSI_SQL_NamedObject):


    """Column definition for a table"""
    def __init__(self, name : str, dataType : DataType, *args : Union[NullableStatus, DataClassification, PrimaryKeyStatus, Documentation]) -> None:
        super().__init__(name)
        self.type : DataType = dataType
        self.primaryKey : PrimaryKeyStatus = DEFAULT_primaryKey
        self.classification : DataClassification = DEFAULT_classification
        self.nullable : NullableStatus = DEFAULT_nullable
        self.documentation : Optional[Documentation] = None
        for arg in args:
            if(type(arg) == NullableStatus):
                self.nullable = arg
            elif(type(arg) == DataClassification):
                self.classification = arg
            elif(type(arg) == PrimaryKeyStatus):
                self.primaryKey = arg
            elif(isinstance(arg, Documentation)):
                self.documentation = arg

    def __eq__(self, o: object) -> bool:
        if(type(o) is not DDLColumn):
            return False
        return super().__eq__(o) and self.type == o.type and self.primaryKey == o.primaryKey and self.nullable == o.nullable and self.classification == o.classification and self.documentation == o.documentation
    
    def isBackwardsCompatibleWith(self, other : object, vTree : ValidationTree) -> bool:
        """Returns true if this column is backwards compatible with the other column"""
        # TODO Add support to changing the column data type to a compatible type
        super().isBackwardsCompatibleWith(other, vTree)
        if not isinstance(other, DDLColumn):
            vTree.addProblem(f"Cannot compare {self.__class__.__name__} with {other.__class__.__name__}")
            return False

        self.type.isBackwardsCompatibleWith(other.type, vTree)
        if(self.nullable != other.nullable):
            vTree.addProblem(f"Nullable status for {self.name} changed from {self.nullable} to {other.nullable}")
        if(self.classification != other.classification):
            vTree.addProblem(f"Data classification for {self.name} changed from {self.classification} to {other.classification}")
        return vTree.hasErrors() == False
    
    def lint(self, tree : ValidationTree) -> None:
        super().nameLint(tree)
        self.type.lint(tree)
        if(self.primaryKey == PrimaryKeyStatus.PK and self.nullable == NullableStatus.NULLABLE):
            tree.addProblem(f"Primary key column {self.name} cannot be nullable")

    def __str__(self) -> str:
        return f"DDLColumn({self.name})"

class AttributeList:
    """A list of column names."""
    def __init__(self, colNames : list[str]) -> None:
        self.colNames : List[str] = []
        for col in colNames:
            self.colNames.append(col)

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, AttributeList) and self.colNames == __value.colNames
    
    def lint(self, tree : ValidationTree) -> None:
        for col in self.colNames:
            if not is_valid_sql_identifier(col):
                tree.addProblem(f"Column name {col} is not a valid ANSI SQL identifier")

    def __str__(self) -> str:
        return f"AttributeList({self.colNames})"

class PrimaryKeyList(AttributeList):
    """A list of columns to be used as the primary key"""
    def __init__(self, colNames: list[str]) -> None:
        super().__init__(colNames)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, PrimaryKeyList)
    
    def __str__(self) -> str:
        return f"PrimaryKeyList({self.colNames})"

# TODO Switch from PK flag to PK column name list
class Schema(ABC):
    """This is a basic schema in the system. It has base meta attributes common for all schemas and core methods for all schemas"""
    def __init__(self) -> None:
        self.defaultDataClassification : DataClassification = DataClassification.PC3
        self.primaryKeyColumns : Optional[PrimaryKeyList] = None 
        self.ingestionPartitionColumns : Optional[AttributeList] = None
        """How should this dataset be partitioned for ingestion and storage"""

    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, Schema)):
            return self.defaultDataClassification == __value.defaultDataClassification and self.primaryKeyColumns == __value.primaryKeyColumns and self.ingestionPartitionColumns == __value.ingestionPartitionColumns
        else:
            return False
    
    @abstractmethod
    def getHubSchema(self) -> 'Schema':
        """Returns the hub schema for this schema"""
        raise NotImplementedError()
    
    @abstractmethod
    def isBackwardsCompatibleWith(self, other : 'Schema', vTree : ValidationTree) -> bool:
        """Returns true if this schema is backward compatible with the other schema"""
        # Primary keys cannot change
        if(self.primaryKeyColumns != other.primaryKeyColumns):
            vTree.addProblem(f"Primary key columns cannot change from {self.primaryKeyColumns} to {other.primaryKeyColumns}")
        # Partitioning cannot change
        if(self.ingestionPartitionColumns != other.ingestionPartitionColumns):
            vTree.addProblem(f"Partitioning cannot change from {self.ingestionPartitionColumns} to {other.ingestionPartitionColumns}")
        return vTree.hasErrors() == False

    @abstractmethod
    def checkClassificationsAreOnly(self, verifier : DataClassificationPolicy) -> bool:
        """Returns true if all columns in this schema have the specified classification"""
        raise NotImplementedError()
        
    @abstractmethod
    def lint(self, tree : ValidationTree) -> None:
        """This method performs linting on this schema"""
        if(self.primaryKeyColumns):
            self.primaryKeyColumns.lint(tree.createChild(self.primaryKeyColumns))
                
        if(self.ingestionPartitionColumns):
            self.ingestionPartitionColumns.lint(tree.createChild(self.ingestionPartitionColumns))

    
class DDLTable(Schema):
    """Table definition"""

    def __init__(self, *args : Union[DDLColumn, PrimaryKeyList, Documentation]) -> None:
        super().__init__()
        self.columns : dict[str, DDLColumn] = OrderedDict[str, DDLColumn]()
        self.primaryKeyColumns : Optional[PrimaryKeyList] = None
        self.documentation : Optional[Documentation] = None
        self.add(*args)

    def checkClassificationsAreOnly(self, verifier: DataClassificationPolicy) -> bool:
        """Check all columns comply with the verifier"""
        for col in self.columns.values():
            if(not verifier.isCompatible(col.classification)):
                return False
        return True
    
    def add(self, *args : Union[DDLColumn, PrimaryKeyList, Documentation]):
        """Add a column or primary key list to the table"""
        for c in args:
            if(type(c) == DDLColumn):
                if(self.columns.get(c.name) != None):
                    raise Exception(f"Duplicate column {c.name}")
                self.columns[c.name] = c
            elif(type(c) == PrimaryKeyList):
                self.primaryKeyColumns = c
            elif(isinstance(c, Documentation)):
                self.documentation = c
        self.calculateKeys()

    def calculateKeys(self):            
        """If a primarykey list is specified then set each column pk correspondingly otherwise construct a primary key list from the pk flag"""
        if(self.primaryKeyColumns):
            for col in self.columns.values():
                col.primaryKey = PrimaryKeyStatus.NOT_PK
            for col in self.primaryKeyColumns.colNames:
                self.columns[col].primaryKey = PrimaryKeyStatus.PK
        else:
            keyNameList : List[str] = []
            for col in self.columns.values():
                if(col.primaryKey == PrimaryKeyStatus.PK):
                    keyNameList.append(col.name)
            self.primaryKeyColumns = PrimaryKeyList(keyNameList)

    def getHubSchema(self) -> 'Schema':
        """Returns the hub schema for this schema"""
        return self
    
    def getColumnByName(self, name : str) -> Optional[DDLColumn]:
        """Returns a column by name"""
        col : DDLColumn = self.columns[name]
        return col
    
    def __eq__(self, o: object) -> bool:
        if(super().__eq__(o) == False):
            return False
        if(type(o) is not DDLTable):
            return False
        return self.columns == o.columns and self.columns == o.columns and self.primaryKeyColumns == o.primaryKeyColumns

    def isBackwardsCompatibleWith(self, other : 'Schema', vTree : ValidationTree) -> bool:
        """Returns true if this schema is backward compatible with the other schema"""
        super().isBackwardsCompatibleWith(other, vTree)
        if vTree.checkTypeMatches(other, DDLTable):
            currentDDL : DDLTable = cast(DDLTable, other)
            # New tables must contain all old columns
            for col in currentDDL.columns.values():
                if(col.name not in self.columns):
                    vTree.addProblem(f"Column {col.name} is missing from the new schema")
            # Existing columns must be compatible
            for col in self.columns.values():
                cTree : ValidationTree = vTree.createChild(col)
                newCol : Optional[DDLColumn] = currentDDL.columns.get(col.name)
                if(newCol):
                    newCol.isBackwardsCompatibleWith(col, cTree)

            # Now check additional columns
            newColumnNames : set[str] = set(self.columns.keys())
            currColNames : set[str] = set(currentDDL.columns.keys())  
            additionalColumns : set[str] = newColumnNames.difference(currColNames)
            for colName in additionalColumns:
                col : DDLColumn = self.columns[colName]
                # Additional columns cannot be primary keys
                if(col.primaryKey == PrimaryKeyStatus.PK):
                    vTree.addProblem(f"Column {col.name} cannot be a new primary key column")
                # Additional columns must be nullable
                if col.nullable == NullableStatus.NOT_NULLABLE:
                    vTree.addProblem(f"Column {col.name} must be nullable")
        return vTree.hasErrors() == False
    
    def lint(self, tree : ValidationTree) -> None:
        """This method performs linting on this schema"""
        super().lint(tree)
        if self.primaryKeyColumns:
            pkTree : ValidationTree = tree.createChild(self.primaryKeyColumns)
            self.primaryKeyColumns.lint(tree.createChild(pkTree))
            for colName in self.primaryKeyColumns.colNames:
                if(colName not in self.columns):
                    pkTree.addProblem(f"Primary key column {colName} is not in the column list")
                else:
                    col : DDLColumn = self.columns[colName]
                    if(col.primaryKey != PrimaryKeyStatus.PK):
                        tree.addProblem(f"Column {colName} should be marked primary key column")
                    if(col.nullable == NullableStatus.NULLABLE):
                        tree.addProblem(f"Primary key {colName} cannot be nullable")
            for col in self.columns.values():
                colTree : ValidationTree = tree.createChild(col)
                col.lint(colTree)
                if col.primaryKey == PrimaryKeyStatus.PK and col.name not in self.primaryKeyColumns.colNames:
                    tree.addProblem(f"Column {col.name} is marked as primary key but is not in the primary key list")
        else:
            tree.addProblem("Table must have a primary key list")

        # If partitioning columns are specified then they must exist and be non nullable
        if self.ingestionPartitionColumns:
            for colName in self.ingestionPartitionColumns.colNames:
                if(colName not in self.columns):
                    tree.addProblem(f"Partitioning column {colName} is not in the column list")
                else:
                    col : DDLColumn = self.columns[colName]
                    if(col.nullable == NullableStatus.NULLABLE):
                        tree.addProblem(f"Partitioning column {colName} cannot be nullable")

    def __str__(self) -> str:
        return f"DDLTable()"

    