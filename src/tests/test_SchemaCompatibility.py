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
from datasurface.md.schema import PrimaryKeyList, PartitionKeyList
from datasurface.md.policy import SimpleDC, SimpleDCTypes


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

    def test_ColumnAttributeChanges(self):
        """Test compatibility when column attributes change (nullable, primary key, etc.)"""
        # Test nullable status changes
        col_nullable = DDLColumn("test_col", String(20), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
        col_not_nullable = DDLColumn("test_col", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK)

        # Changing from nullable to not nullable should fail
        t = ValidationTree(col_not_nullable)
        self.assertFalse(col_not_nullable.checkForBackwardsCompatibility(col_nullable, t))

        # Changing from not nullable to nullable should fail
        t = ValidationTree(col_nullable)
        self.assertFalse(col_nullable.checkForBackwardsCompatibility(col_not_nullable, t))

        # Test primary key status changes
        col_pk = DDLColumn("test_col", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)
        col_not_pk = DDLColumn("test_col", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK)

        # Any primary key status change should fail compatibility
        t = ValidationTree(col_pk)
        self.assertFalse(col_pk.checkForBackwardsCompatibility(col_not_pk, t))

        t = ValidationTree(col_not_pk)
        self.assertFalse(col_not_pk.checkForBackwardsCompatibility(col_pk, t))

        # Test data classification changes
        col_no_class = DDLColumn("test_col", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK)
        col_with_class = DDLColumn("test_col", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK,
                                   classifications=[SimpleDC(SimpleDCTypes.PC1)])

        # Classification changes should fail compatibility
        t = ValidationTree(col_with_class)
        self.assertFalse(col_with_class.checkForBackwardsCompatibility(col_no_class, t))

        t = ValidationTree(col_no_class)
        self.assertFalse(col_no_class.checkForBackwardsCompatibility(col_with_class, t))

    def test_AdvancedTypeCompatibility(self):
        """Test advanced type compatibility scenarios"""
        # Decimal precision and scale compatibility
        self.assertTrue(Decimal(12, 2).isBackwardsCompatibleWith(Decimal(10, 2), ValidationTree(UserDSLObject())))
        self.assertFalse(Decimal(10, 2).isBackwardsCompatibleWith(Decimal(12, 2), ValidationTree(UserDSLObject())))
        # Note: Higher scale decimals are considered backwards compatible with lower scale
        self.assertTrue(Decimal(10, 4).isBackwardsCompatibleWith(Decimal(10, 2), ValidationTree(UserDSLObject())))
        self.assertFalse(Decimal(10, 2).isBackwardsCompatibleWith(Decimal(10, 4), ValidationTree(UserDSLObject())))

        # Vector dimension compatibility
        self.assertTrue(Vector(20).isBackwardsCompatibleWith(Vector(10), ValidationTree(UserDSLObject())))
        self.assertFalse(Vector(10).isBackwardsCompatibleWith(Vector(20), ValidationTree(UserDSLObject())))

        # Binary size compatibility
        self.assertTrue(Binary(20).isBackwardsCompatibleWith(Binary(10), ValidationTree(UserDSLObject())))
        self.assertFalse(Binary(10).isBackwardsCompatibleWith(Binary(20), ValidationTree(UserDSLObject())))

        # String type cross-compatibility tests
        self.assertFalse(String(10).isBackwardsCompatibleWith(VarChar(10), ValidationTree(UserDSLObject())))
        self.assertFalse(VarChar(10).isBackwardsCompatibleWith(NVarChar(10), ValidationTree(UserDSLObject())))
        self.assertTrue(Char(10).isBackwardsCompatibleWith(VarChar(10), ValidationTree(UserDSLObject())))
        self.assertTrue(VarChar(10).isBackwardsCompatibleWith(Char(10), ValidationTree(UserDSLObject())))

        # Variant size compatibility
        self.assertTrue(Variant(20).isBackwardsCompatibleWith(Variant(10), ValidationTree(UserDSLObject())))
        self.assertFalse(Variant(10).isBackwardsCompatibleWith(Variant(20), ValidationTree(UserDSLObject())))

        # IEEE numeric promotion chain
        self.assertTrue(IEEE64().isBackwardsCompatibleWith(IEEE32(), ValidationTree(UserDSLObject())))
        self.assertTrue(IEEE128().isBackwardsCompatibleWith(IEEE64(), ValidationTree(UserDSLObject())))
        self.assertTrue(IEEE128().isBackwardsCompatibleWith(IEEE32(), ValidationTree(UserDSLObject())))
        self.assertTrue(IEEE128().isBackwardsCompatibleWith(IEEE16(), ValidationTree(UserDSLObject())))

        # Integer promotion chain
        self.assertTrue(BigInt().isBackwardsCompatibleWith(SmallInt(), ValidationTree(UserDSLObject())))
        self.assertFalse(SmallInt().isBackwardsCompatibleWith(BigInt(), ValidationTree(UserDSLObject())))

    def test_StringTypeLimitsAndUnlimited(self):
        """Test string type compatibility with unlimited sizes"""
        # Unlimited string types should be compatible with limited ones
        self.assertTrue(String().isBackwardsCompatibleWith(String(100), ValidationTree(UserDSLObject())))
        self.assertTrue(VarChar().isBackwardsCompatibleWith(VarChar(50), ValidationTree(UserDSLObject())))

        # Limited should not be compatible with unlimited if size would decrease
        # Note: This depends on implementation - unlimited might be treated as very large

        # Same type, increasing size should be compatible
        self.assertTrue(String(200).isBackwardsCompatibleWith(String(100), ValidationTree(UserDSLObject())))
        self.assertTrue(VarChar(150).isBackwardsCompatibleWith(VarChar(100), ValidationTree(UserDSLObject())))
        self.assertTrue(NVarChar(300).isBackwardsCompatibleWith(NVarChar(200), ValidationTree(UserDSLObject())))

        # Decreasing size should not be compatible
        self.assertFalse(String(50).isBackwardsCompatibleWith(String(100), ValidationTree(UserDSLObject())))
        self.assertFalse(VarChar(75).isBackwardsCompatibleWith(VarChar(100), ValidationTree(UserDSLObject())))

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

    def test_ExistingColumnChanges(self):
        """Test compatibility when existing columns are modified"""
        base_table = DDLTable(
            DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("name", String(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK),
            DDLColumn("age", SmallInt(), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        # Note: DDLTable backwards compatibility does NOT allow existing column changes,
        # even if the individual types are compatible. This is a stricter policy.

        # Test that type changes to existing columns are not allowed
        type_change_table = copy.deepcopy(base_table)
        type_change_table.columns["age"].type = BigInt()

        t = ValidationTree(type_change_table)
        self.assertFalse(type_change_table.checkForBackwardsCompatibility(base_table, t))

        # Test that size changes to existing columns are not allowed
        size_change_table = copy.deepcopy(base_table)
        size_change_table.columns["name"].type = String(100)

        t = ValidationTree(size_change_table)
        self.assertFalse(size_change_table.checkForBackwardsCompatibility(base_table, t))

        # Test that nullable status changes are not allowed
        nullable_change_table = copy.deepcopy(base_table)
        nullable_change_table.columns["name"].nullable = NullableStatus.NOT_NULLABLE

        t = ValidationTree(nullable_change_table)
        self.assertFalse(nullable_change_table.checkForBackwardsCompatibility(base_table, t))

        # Test that only adding nullable columns is allowed
        add_column_table = copy.deepcopy(base_table)
        add_column_table.add(DDLColumn("email", String(100), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        t = ValidationTree(add_column_table)
        self.assertTrue(add_column_table.checkForBackwardsCompatibility(base_table, t))

    def test_PrimaryKeyChanges(self):
        """Test compatibility when primary keys change"""
        base_table = DDLTable(
            DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("code", String(5), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK),
            DDLColumn("name", String(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        # Test adding a column to primary key (should be incompatible)
        expanded_pk_table = DDLTable(
            primary_key_list=PrimaryKeyList(["id", "code"]),
            columns=[
                DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE),
                DDLColumn("code", String(5), NullableStatus.NOT_NULLABLE),
                DDLColumn("name", String(50), NullableStatus.NULLABLE)
            ])

        t = ValidationTree(expanded_pk_table)
        self.assertFalse(expanded_pk_table.checkForBackwardsCompatibility(base_table, t))

        # Test removing a column from primary key (should be incompatible)
        reduced_pk_table = DDLTable(
            primary_key_list=PrimaryKeyList(["id"]),
            columns=[
                DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE),
                DDLColumn("code", String(5), NullableStatus.NOT_NULLABLE),
                DDLColumn("name", String(50), NullableStatus.NULLABLE)
            ])

        multi_pk_base = DDLTable(
            primary_key_list=PrimaryKeyList(["id", "code"]),
            columns=[
                DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE),
                DDLColumn("code", String(5), NullableStatus.NOT_NULLABLE),
                DDLColumn("name", String(50), NullableStatus.NULLABLE)
            ])

        t = ValidationTree(reduced_pk_table)
        self.assertFalse(reduced_pk_table.checkForBackwardsCompatibility(multi_pk_base, t))

        # Test reordering primary key columns (should be incompatible)
        reordered_pk_table = DDLTable(
            primary_key_list=PrimaryKeyList(["code", "id"]),
            columns=[
                DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE),
                DDLColumn("code", String(5), NullableStatus.NOT_NULLABLE),
                DDLColumn("name", String(50), NullableStatus.NULLABLE)
            ])

        t = ValidationTree(reordered_pk_table)
        self.assertFalse(reordered_pk_table.checkForBackwardsCompatibility(multi_pk_base, t))

    def test_PartitionKeyChanges(self):
        """Test compatibility when partition keys change"""
        base_table = DDLTable(
            primary_key_list=PrimaryKeyList(["id"]),
            partition_key_list=PartitionKeyList(["region"]),
            columns=[
                DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE),
                DDLColumn("region", String(20), NullableStatus.NOT_NULLABLE),
                DDLColumn("name", String(50), NullableStatus.NULLABLE)
            ])

        # Test changing partition keys (should be incompatible)
        changed_partition_table = DDLTable(
            primary_key_list=PrimaryKeyList(["id"]),
            partition_key_list=PartitionKeyList(["name"]),
            columns=[
                DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE),
                DDLColumn("region", String(20), NullableStatus.NOT_NULLABLE),
                DDLColumn("name", String(50), NullableStatus.NOT_NULLABLE)  # Must be not nullable for partitioning
            ])

        t = ValidationTree(changed_partition_table)
        self.assertFalse(changed_partition_table.checkForBackwardsCompatibility(base_table, t))

        # Test adding partition keys (should be incompatible)
        no_partition_table = DDLTable(
            primary_key_list=PrimaryKeyList(["id"]),
            columns=[
                DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE),
                DDLColumn("region", String(20), NullableStatus.NOT_NULLABLE),
                DDLColumn("name", String(50), NullableStatus.NULLABLE)
            ])

        t = ValidationTree(base_table)
        self.assertFalse(base_table.checkForBackwardsCompatibility(no_partition_table, t))

    def test_MultipleSimultaneousChanges(self):
        """Test compatibility with multiple changes happening at once"""
        base_table = DDLTable(
            DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("name", String(30), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK),
            DDLColumn("age", SmallInt(), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        # Test that only adding nullable columns is compatible
        add_only_table = copy.deepcopy(base_table)
        add_only_table.add(DDLColumn("email", String(100), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))
        add_only_table.add(DDLColumn("phone", String(20), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        t = ValidationTree(add_only_table)
        self.assertTrue(add_only_table.checkForBackwardsCompatibility(base_table, t))

        # Test that mixing column additions with changes to existing columns fails
        mixed_table = copy.deepcopy(base_table)
        mixed_table.columns["name"].type = String(50)  # Change existing column (not allowed)
        mixed_table.add(DDLColumn("email", String(100), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))  # Add new column (allowed)

        t = ValidationTree(mixed_table)
        self.assertFalse(mixed_table.checkForBackwardsCompatibility(base_table, t))

    def test_EdgeCasesAndErrorHandling(self):
        """Test edge cases and error conditions"""
        # Note: Empty table compatibility testing is not included as empty tables
        # would fail primary key validation, which is a separate linting concern

        # Test single column table
        single_col_table = DDLTable(
            DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK))

        single_col_table_expanded = copy.deepcopy(single_col_table)
        single_col_table_expanded.add(DDLColumn("data", String(100), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        t = ValidationTree(single_col_table_expanded)
        self.assertTrue(single_col_table_expanded.checkForBackwardsCompatibility(single_col_table, t))

        # Test table with many columns (stress test)
        many_columns_base = DDLTable(
            DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK))

        for i in range(50):
            many_columns_base.add(DDLColumn(f"col_{i}", String(20), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        many_columns_modified = copy.deepcopy(many_columns_base)
        many_columns_modified.add(DDLColumn("new_col", String(20), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        t = ValidationTree(many_columns_modified)
        self.assertTrue(many_columns_modified.checkForBackwardsCompatibility(many_columns_base, t))

    def test_ComplexTypeCompatibilityChains(self):
        """Test complex chains of type compatibility"""
        # Test transitivity: if A -> B and B -> C, we should verify A -> C
        small_decimal = Decimal(8, 2)
        medium_decimal = Decimal(10, 2)
        large_decimal = Decimal(12, 2)

        # Test chain compatibility
        self.assertTrue(medium_decimal.isBackwardsCompatibleWith(small_decimal, ValidationTree(UserDSLObject())))
        self.assertTrue(large_decimal.isBackwardsCompatibleWith(medium_decimal, ValidationTree(UserDSLObject())))
        self.assertTrue(large_decimal.isBackwardsCompatibleWith(small_decimal, ValidationTree(UserDSLObject())))

        # Test numeric type promotion chains
        ieee16 = IEEE16()
        ieee32 = IEEE32()
        ieee64 = IEEE64()
        ieee128 = IEEE128()

        self.assertTrue(ieee32.isBackwardsCompatibleWith(ieee16, ValidationTree(UserDSLObject())))
        self.assertTrue(ieee64.isBackwardsCompatibleWith(ieee32, ValidationTree(UserDSLObject())))
        self.assertTrue(ieee128.isBackwardsCompatibleWith(ieee64, ValidationTree(UserDSLObject())))

        # Test full chain
        self.assertTrue(ieee128.isBackwardsCompatibleWith(ieee16, ValidationTree(UserDSLObject())))

    def test_SpecialColumnScenarios(self):
        """Test special column scenarios"""
        # Test column with all attributes set
        full_column = DDLColumn(
            "full_col",
            String(50),
            NullableStatus.NOT_NULLABLE,
            PrimaryKeyStatus.PK,
            classifications=[SimpleDC(SimpleDCTypes.PC1)])

        # Test compatibility with column missing some attributes
        simple_column = DDLColumn("full_col", String(50), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)

        t = ValidationTree(simple_column)
        self.assertFalse(simple_column.checkForBackwardsCompatibility(full_column, t))

        # Test columns with same name but different everything else
        col_a = DDLColumn("same_name", String(10), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK)
        col_b = DDLColumn("same_name", BigInt(), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)

        t = ValidationTree(col_a)
        self.assertFalse(col_a.checkForBackwardsCompatibility(col_b, t))

    def test_DataTypeSpecificCompatibility(self):
        """Test compatibility for specific data types with their parameters"""
        # Test timestamp with timezone considerations (if supported)
        ts1 = Timestamp()
        ts2 = Timestamp()
        self.assertTrue(ts1.isBackwardsCompatibleWith(ts2, ValidationTree(UserDSLObject())))

        # Test interval compatibility
        interval1 = Interval()
        interval2 = Interval()
        self.assertTrue(interval1.isBackwardsCompatibleWith(interval2, ValidationTree(UserDSLObject())))

        # Test boolean compatibility (should be strict)
        bool1 = Boolean()
        bool2 = Boolean()
        self.assertTrue(bool1.isBackwardsCompatibleWith(bool2, ValidationTree(UserDSLObject())))

        # Boolean should not be compatible with anything else
        self.assertFalse(bool1.isBackwardsCompatibleWith(SmallInt(), ValidationTree(UserDSLObject())))
        self.assertFalse(SmallInt().isBackwardsCompatibleWith(bool1, ValidationTree(UserDSLObject())))

    def test_SchemaEvolutionWorkflows(self):
        """Test common schema evolution patterns"""
        # Version 1: Initial schema
        v1_table = DDLTable(
            DDLColumn("user_id", String(36), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("username", String(50), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK),
            DDLColumn("email", String(100), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK))

        # Version 2: Add optional fields (this is the only allowed schema evolution)
        v2_table = copy.deepcopy(v1_table)
        v2_table.add(DDLColumn("first_name", String(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))
        v2_table.add(DDLColumn("last_name", String(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        t = ValidationTree(v2_table)
        self.assertTrue(v2_table.checkForBackwardsCompatibility(v1_table, t))

        # Version 3: Try to expand field sizes (not allowed - existing column changes)
        v3_table = copy.deepcopy(v2_table)
        v3_table.columns["username"].type = String(100)
        v3_table.columns["email"].type = String(200)

        t = ValidationTree(v3_table)
        self.assertFalse(v3_table.checkForBackwardsCompatibility(v2_table, t))

        # Also test that it fails against v1
        t = ValidationTree(v3_table)
        self.assertFalse(v3_table.checkForBackwardsCompatibility(v1_table, t))

        # Version 4: Valid evolution - only add more nullable columns
        v4_table = copy.deepcopy(v2_table)
        v4_table.add(DDLColumn("middle_name", String(50), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))
        v4_table.add(DDLColumn("suffix", String(10), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        t = ValidationTree(v4_table)
        self.assertTrue(v4_table.checkForBackwardsCompatibility(v2_table, t))

        # Test full evolution chain - should work since we only added nullable columns
        t = ValidationTree(v4_table)
        self.assertTrue(v4_table.checkForBackwardsCompatibility(v1_table, t))
