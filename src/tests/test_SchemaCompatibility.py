import unittest
import copy

from datasurface.md import DDLColumn, String, NullableStatus, PrimaryKeyStatus



class TestSchemaCompatibility(unittest.TestCase):

    def test_ColumnCompatibility(self):
        col1 : DDLColumn = DDLColumn("col1", String(20), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK)

        col2 : DDLColumn = copy.deepcopy(col1)

        self.assertTrue(col1.isBackwardsCompatibleWith(col2))

        # Bigger string is still compatible
        col1.type = String(25)
        self.assertTrue(col1.isBackwardsCompatibleWith(col2))


