import unittest
import copy

from datasurface.md import DDLColumn, String, NullableStatus, PrimaryKeyStatus, IEEE32, IEEE64, IEEE16, IEEE128, BigInt, SmallInt
from datasurface.md import DDLTable
from datasurface.md.Lint import ValidationTree

class TestSchemaCompatibility(unittest.TestCase):

    def assertCompatible(self, rc : ValidationTree):
        self.assertFalse(rc.hasIssues())

    def assertNotCompatible(self, rc : ValidationTree):
        self.assertTrue(rc.hasIssues())

    def test_ColumnCompatibility(self):
        col1 : DDLColumn = DDLColumn("col1", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)

        col2 : DDLColumn = copy.deepcopy(col1)

        t : ValidationTree = ValidationTree(col1)
        self.assertTrue(col1.isBackwardsCompatibleWith(col2, t))

        # Bigger string is still compatible
        col1.type = String(25)
        t = ValidationTree(col1)
        self.assertTrue(col1.isBackwardsCompatibleWith(col2, t))

        # Check a small string is not compatible
        col1.type = String(15)
        t = ValidationTree(col1)
        self.assertFalse(col1.isBackwardsCompatibleWith(col2, t))

        # Check String isn't compatible with IEEE32
        col1.type = IEEE32()
        t = ValidationTree(col1)
        self.assertFalse(col1.isBackwardsCompatibleWith(col2, t))
        t = ValidationTree(col2)
        self.assertFalse(col2.isBackwardsCompatibleWith(col1, t))

        col1.type = IEEE64()
        col2.type = IEEE32()

        # Check IEEE32 is compatible with IEEE64
        t = ValidationTree(col1)
        self.assertTrue(col1.isBackwardsCompatibleWith(col2, t))
        col2.type = IEEE16()

        col1.type = IEEE128()
        t = ValidationTree(col1)
        self.assertTrue(col1.isBackwardsCompatibleWith(col2, t))

        col1.type = BigInt()
        t = ValidationTree(col1)
        self.assertFalse(col1.isBackwardsCompatibleWith(col2, t))

        # Test SmallInt can be replaced with a Bigint
        col2.type = SmallInt()
        t = ValidationTree(col1)
        self.assertTrue(col1.isBackwardsCompatibleWith(col2, t))
        # BigInt can't be replaced with a SmallInt
        t = ValidationTree(col2)
        self.assertFalse(col2.isBackwardsCompatibleWith(col1, t))

    def testDDLTableBackwardsCompatibility(self):
        t1 : DDLTable = DDLTable(
            DDLColumn("key", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK), 
            DDLColumn("firstName", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK),
            DDLColumn("lastName", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK))
        
        # Adding a nullable column is backwards compatible
        t2 : DDLTable = copy.deepcopy(t1)
        t2.addColumn(DDLColumn("middleName", String(20), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        t : ValidationTree = ValidationTree(t2)
        self.assertTrue(t2.isBackwardsCompatibleWith(t1, t))

        # Adding a non-nullable column is not backwards compatible
        t2.add(DDLColumn("middleName2", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK))
        t = ValidationTree(t2)
        self.assertFalse(t2.isBackwardsCompatibleWith(t1, t))

        # Adding a PK column is not backwards compatible
        t2 : DDLTable = copy.deepcopy(t1)
        t2.add(DDLColumn("middleName3", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK))
        t = ValidationTree(t2)
        self.assertFalse(t2.isBackwardsCompatibleWith(t1, t))

        # Removing a column is not backwards compatible
        t2 : DDLTable = copy.deepcopy(t1)
        t2.columns.pop("firstName")
        t = ValidationTree(t2)
        self.assertFalse(t2.isBackwardsCompatibleWith(t1, t))
