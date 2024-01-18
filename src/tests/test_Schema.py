import unittest

from datasurface.md import DDLColumn, String, NullableStatus, PrimaryKeyStatus
from datasurface.md import DDLTable, PrimaryKeyList, DataClassification
from datasurface.md.Exceptions import NameMustBeANSISQLIdentifierException
from datasurface.md.Lint import ValidationTree

class TestSchemaCreation(unittest.TestCase):
    def testPrimaryKeys(self):

        # Create a column with the default values and verify they are as expected
        c : DDLColumn = DDLColumn("id", String(10))
        self.assertEqual(c.primaryKey, PrimaryKeyStatus.NOT_PK)
        self.assertEqual(c.nullable, NullableStatus.NULLABLE)
        self.assertEqual(c.type, String(10))
        self.assertEqual(c.name, "id")
        self.assertEqual(c.classification, DataClassification.PC3)

        try:
            # Cannot construct DDLColumn with invalid name
            c = DDLColumn("id a", String(10), NullableStatus.NOT_NULLABLE)
            self.fail("Should have thrown an exception")
        except NameMustBeANSISQLIdentifierException:
            pass

        c = DDLColumn("id_a", String(10), NullableStatus.NOT_NULLABLE)
        c.name = "Bad ANSI Name" # Has spaces
        tree : ValidationTree = ValidationTree(c)
        c.lint(tree)
        self.assertTrue(tree.hasErrors()) # Name is not ANSI SQL compliant

        # Create a table specifying the primary key on the columns
        t : DDLTable = DDLTable(
            DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("firstName", String(20)),
            DDLColumn("lastName", String(20)))
        
        self.assertIsNotNone(t.primaryKeyColumns)
        if(t.primaryKeyColumns == None):
            raise Exception("PrimaryKeyColumns is None")
        self.assertEqual(t.primaryKeyColumns.colNames, ["id"])
        self.assertEqual(t.columns['id'].primaryKey, PrimaryKeyStatus.PK)
        self.assertEqual(t.columns['firstName'].primaryKey, PrimaryKeyStatus.NOT_PK)
        self.assertEqual(t.columns['lastName'].primaryKey, PrimaryKeyStatus.NOT_PK)

        self.assertEqual(t.columns['id'].nullable, NullableStatus.NOT_NULLABLE)
        # Check default values are as expected
        self.assertEqual(t.columns['firstName'].nullable, NullableStatus.NULLABLE)
        self.assertEqual(t.columns['lastName'].nullable, NullableStatus.NULLABLE)

        # Create a table specifying the primary key using the PrimaryKeyList
        # The PK flags on the columns will be calculated from this. The values
        t : DDLTable = DDLTable(
            PrimaryKeyList(["id"]),
            DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE),
            DDLColumn("firstName", String(20)),
            DDLColumn("lastName", String(20)))
        
        self.assertIsNotNone(t.primaryKeyColumns)
        if(t.primaryKeyColumns == None):
            raise Exception("PrimaryKeyColumns is None")
        self.assertEqual(t.primaryKeyColumns.colNames, ["id"])
        self.assertEqual(t.columns['id'].primaryKey, PrimaryKeyStatus.PK)
        self.assertEqual(t.columns['firstName'].primaryKey, PrimaryKeyStatus.NOT_PK)
        self.assertEqual(t.columns['lastName'].primaryKey, PrimaryKeyStatus.NOT_PK)

