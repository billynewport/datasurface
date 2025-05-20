"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest
import copy

from datasurface.md import DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md import DDLTable
from datasurface.md import ValidationTree, UserDSLObject
from datasurface.md import Boolean, Char, Date, Interval, NVarChar, Timestamp, VarChar, Variant
from datasurface.md.types import String, IEEE32, IEEE64, IEEE16, IEEE128, BigInt, SmallInt, Decimal, Vector, Binary


class TestSchemaCompatibility(unittest.TestCase):

    def assertCompatible(self, rc: ValidationTree):
        self.assertFalse(rc.hasErrors())

    def assertNotCompatible(self, rc: ValidationTree):
        self.assertTrue(rc.hasErrors())

    def test_ColumnCompatibility(self):
        col1: DDLColumn = DDLColumn("col1", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)

        col2: DDLColumn = copy.deepcopy(col1)

        t: ValidationTree = ValidationTree(col1)
        self.assertTrue(col1.checkForBackwardsCompatibility(col2, t))

        # Bigger string is still compatible
        col1.type = String(25)
        t = ValidationTree(col1)
        self.assertTrue(col1.checkForBackwardsCompatibility(col2, t))

        # Check a small string is not compatible
        col1.type = String(15)
        t = ValidationTree(col1)
        self.assertFalse(col1.checkForBackwardsCompatibility(col2, t))

        # Check String isn't compatible with IEEE32
        col1.type = IEEE32()
        t = ValidationTree(col1)
        self.assertFalse(col1.checkForBackwardsCompatibility(col2, t))
        t = ValidationTree(col2)
        self.assertFalse(col2.checkForBackwardsCompatibility(col1, t))

        col1.type = IEEE64()
        col2.type = IEEE32()

        # Check IEEE32 is compatible with IEEE64
        t = ValidationTree(col1)
        self.assertTrue(col1.checkForBackwardsCompatibility(col2, t))
        col2.type = IEEE16()

        col1.type = IEEE128()
        t = ValidationTree(col1)
        self.assertTrue(col1.checkForBackwardsCompatibility(col2, t))

        col1.type = BigInt()
        t = ValidationTree(col1)
        self.assertFalse(col1.checkForBackwardsCompatibility(col2, t))

        # Test SmallInt can be replaced with a Bigint
        col2.type = SmallInt()
        t = ValidationTree(col1)
        self.assertTrue(col1.checkForBackwardsCompatibility(col2, t))
        # BigInt can't be replaced with a SmallInt
        t = ValidationTree(col2)
        self.assertFalse(col2.checkForBackwardsCompatibility(col1, t))

        # Check lots of backward compatibility cases
        self.assertTrue(Decimal(10, 2).isBackwardsCompatibleWith(Decimal(10, 2), ValidationTree(UserDSLObject())))
        self.assertTrue(String(10).isBackwardsCompatibleWith(String(10), ValidationTree(UserDSLObject())))
        self.assertTrue(String().isBackwardsCompatibleWith(String(10), ValidationTree(UserDSLObject())))  # Unlimited
        self.assertTrue(IEEE32().isBackwardsCompatibleWith(IEEE32(), ValidationTree(UserDSLObject())))
        self.assertTrue(IEEE32().isBackwardsCompatibleWith(IEEE16(), ValidationTree(UserDSLObject())))
        self.assertFalse(IEEE32().isBackwardsCompatibleWith(IEEE64(), ValidationTree(UserDSLObject())))
        self.assertTrue(IEEE64().isBackwardsCompatibleWith(IEEE64(), ValidationTree(UserDSLObject())))
        self.assertTrue(IEEE128().isBackwardsCompatibleWith(IEEE128(), ValidationTree(UserDSLObject())))

        self.assertTrue(BigInt().isBackwardsCompatibleWith(BigInt(), ValidationTree(UserDSLObject())))
        self.assertTrue(SmallInt().isBackwardsCompatibleWith(SmallInt(), ValidationTree(UserDSLObject())))

        self.assertTrue(Boolean().isBackwardsCompatibleWith(Boolean(), ValidationTree(UserDSLObject())))

        self.assertTrue(Timestamp().isBackwardsCompatibleWith(Timestamp(), ValidationTree(UserDSLObject())))
        self.assertTrue(Timestamp().isBackwardsCompatibleWith(Date(), ValidationTree(UserDSLObject())))

        self.assertTrue(Date().isBackwardsCompatibleWith(Date(), ValidationTree(UserDSLObject())))
        self.assertFalse(Date().isBackwardsCompatibleWith(Timestamp(), ValidationTree(UserDSLObject())))

        self.assertTrue(Interval().isBackwardsCompatibleWith(Interval(), ValidationTree(UserDSLObject())))
        self.assertFalse(Interval().isBackwardsCompatibleWith(Date(), ValidationTree(UserDSLObject())))
        self.assertFalse(Interval().isBackwardsCompatibleWith(Timestamp(), ValidationTree(UserDSLObject())))

        self.assertTrue(VarChar(10).isBackwardsCompatibleWith(VarChar(10),  ValidationTree(UserDSLObject())))
        self.assertTrue(VarChar().isBackwardsCompatibleWith(VarChar(10),  ValidationTree(UserDSLObject())))
        self.assertTrue(NVarChar(10).isBackwardsCompatibleWith(NVarChar(10), ValidationTree(UserDSLObject())))
        self.assertTrue(Char(10).isBackwardsCompatibleWith(Char(10), ValidationTree(UserDSLObject())))

        self.assertTrue(Variant(10).isBackwardsCompatibleWith(Variant(10), ValidationTree(UserDSLObject())))
        self.assertTrue(Binary(10).isBackwardsCompatibleWith(Binary(10), ValidationTree(UserDSLObject())))
        self.assertFalse(Variant(10).isBackwardsCompatibleWith(Binary(10), ValidationTree(UserDSLObject())))
        self.assertTrue(Vector(10).isBackwardsCompatibleWith(Vector(10), ValidationTree(UserDSLObject())))

        self.assertFalse(Boolean().isBackwardsCompatibleWith(Vector(10), ValidationTree(UserDSLObject())))

    def testDDLTableBackwardsCompatibility(self):
        t1: DDLTable = DDLTable(
            DDLColumn("key", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("firstName", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK),
            DDLColumn("lastName", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK))

        # Adding a nullable column is backwards compatible
        t2: DDLTable = copy.deepcopy(t1)
        t2.add(DDLColumn("middleName", String(20), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        t: ValidationTree = ValidationTree(t2)
        self.assertTrue(t2.checkForBackwardsCompatibility(t1, t))

        # Adding a non-nullable column is not backwards compatible
        t2.add(DDLColumn("middleName2", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK))
        t = ValidationTree(t2)
        self.assertFalse(t2.checkForBackwardsCompatibility(t1, t))

        # Adding a PK column is not backwards compatible
        t2: DDLTable = copy.deepcopy(t1)
        t2.add(DDLColumn("middleName3", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK))
        t = ValidationTree(t2)
        self.assertFalse(t2.checkForBackwardsCompatibility(t1, t))

        # Removing a column is not backwards compatible
        t2: DDLTable = copy.deepcopy(t1)
        t2.columns.pop("firstName")
        t = ValidationTree(t2)
        self.assertFalse(t2.checkForBackwardsCompatibility(t1, t))
