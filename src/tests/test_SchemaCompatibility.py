import unittest
import copy

from datasurface.md import DDLColumn, String, NullableStatus, PrimaryKeyStatus, IEEE32, IEEE64, IEEE16, IEEE128, BigInt, SmallInt
from datasurface.md import DDLTable

class TestSchemaCompatibility(unittest.TestCase):

    def test_ColumnCompatibility(self):
        col1 : DDLColumn = DDLColumn("col1", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)

        col2 : DDLColumn = copy.deepcopy(col1)

        self.assertTrue(col1.isBackwardsCompatibleWith(col2))

        # Bigger string is still compatible
        col1.type = String(25)
        self.assertTrue(col1.isBackwardsCompatibleWith(col2))

        # Check a small string is not compatible
        col1.type = String(15)
        self.assertFalse(col1.isBackwardsCompatibleWith(col2))

        # Check String isn't compatible with IEEE32
        col1.type = IEEE32()
        self.assertFalse(col1.isBackwardsCompatibleWith(col2))
        self.assertFalse(col2.isBackwardsCompatibleWith(col1))

        col1.type = IEEE64()
        col2.type = IEEE32()

        # Check IEEE32 is compatible with IEEE64
        self.assertTrue(col1.isBackwardsCompatibleWith(col2))
        col2.type = IEEE16()

        col1.type = IEEE128()
        self.assertTrue(col1.isBackwardsCompatibleWith(col2))

        col1.type = BigInt()
        self.assertFalse(col1.isBackwardsCompatibleWith(col2))

        # Test SmallInt can be replaced with a Bigint
        col2.type = SmallInt()
        self.assertTrue(col1.isBackwardsCompatibleWith(col2))
        # BigInt can't be replaced with a SmallInt
        self.assertFalse(col2.isBackwardsCompatibleWith(col1))

    def testDDLTableBackwardsCompatibility(self):
        t1 : DDLTable = DDLTable(
            DDLColumn("key", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK), 
            DDLColumn("firstName", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK),
            DDLColumn("lastName", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK))
        
        # Adding a nullable column is backwards compatible
        t2 : DDLTable = copy.deepcopy(t1)
        t2.addColumn(DDLColumn("middleName", String(20), NullableStatus.NULLABLE, PrimaryKeyStatus.NOT_PK))

        self.assertTrue(t2.isBackwardsCompatibleWith(t1))

        # Adding a non-nullable column is not backwards compatible
        t2.add(DDLColumn("middleName2", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.NOT_PK))
        self.assertFalse(t2.isBackwardsCompatibleWith(t1))

        # Adding a PK column is not backwards compatible
        t2 : DDLTable = copy.deepcopy(t1)
        t2.add(DDLColumn("middleName3", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK))
        self.assertFalse(t2.isBackwardsCompatibleWith(t1))

