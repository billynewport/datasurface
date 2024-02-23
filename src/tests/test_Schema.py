from copy import deepcopy
from typing import Optional, cast
import unittest

from datasurface.md import AvroSchema, DDLColumn, Schema, String, NullableStatus, PrimaryKeyStatus
from datasurface.md import DDLTable, PrimaryKeyList
from datasurface.md.Exceptions import NameMustBeANSISQLIdentifierException
from datasurface.md.Governance import Dataset, Datastore, Ecosystem, ProductionStatus
from datasurface.md.Lint import ProductionDatastoreMustHaveClassifications, ValidationTree
from datasurface.md.Policy import SimpleDC, SimpleDCTypes
from tests.nwdb.eco import createEcosystem


class TestSchemaCreation(unittest.TestCase):
    def testPrimaryKeys(self):
        c: DDLColumn = DDLColumn("id", String(10))
        self.assertEqual(c.primaryKey, PrimaryKeyStatus.NOT_PK)
        self.assertEqual(c.nullable, NullableStatus.NULLABLE)
        self.assertEqual(c.type, String(10))
        self.assertEqual(c.name, "id")
        self.assertEqual(c.classification, None)

        try:
            # Cannot construct DDLColumn with invalid name
            c = DDLColumn("id a", String(10), NullableStatus.NOT_NULLABLE)
            self.fail("Should have thrown an exception")
        except NameMustBeANSISQLIdentifierException:
            pass

        c = DDLColumn("id_a", String(10), NullableStatus.NOT_NULLABLE)
        c.name = "Bad ANSI Name"  # Has spaces
        tree: ValidationTree = ValidationTree(c)
        c.lint(tree)
        self.assertTrue(tree.hasErrors())  # Name is not ANSI SQL compliant

        # Create a table specifying the primary key on the columns
        t: DDLTable = DDLTable(
            DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
            DDLColumn("firstName", String(20)),
            DDLColumn("lastName", String(20)))

        self.assertEqual(t, t)

        self.assertIsNotNone(t.primaryKeyColumns)
        if (t.primaryKeyColumns is None):
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
        t: DDLTable = DDLTable(
            PrimaryKeyList(["id"]),
            DDLColumn("id", String(10), NullableStatus.NOT_NULLABLE),
            DDLColumn("firstName", String(20)),
            DDLColumn("lastName", String(20)))

        self.assertIsNotNone(t.primaryKeyColumns)
        if (t.primaryKeyColumns is None):
            raise Exception("PrimaryKeyColumns is None")
        self.assertEqual(t.primaryKeyColumns.colNames, ["id"])
        self.assertEqual(t.columns['id'].primaryKey, PrimaryKeyStatus.PK)
        self.assertEqual(t.columns['firstName'].primaryKey, PrimaryKeyStatus.NOT_PK)
        self.assertEqual(t.columns['lastName'].primaryKey, PrimaryKeyStatus.NOT_PK)

    def test_AvroSchema(self):
        s1: Schema = AvroSchema.AvroSchema('{"type":"record","name":"test","fields":[{"name":"f1","type":"string"}]}', [SimpleDC(SimpleDCTypes.PC3)])
        s2: Schema = AvroSchema.AvroSchema('{"type":"record","name":"test","fields":[{"name":"f2","type":"string"}]}', [SimpleDC(SimpleDCTypes.PC3)])

        self.assertEqual(s1, s1)
        self.assertNotEqual(s1, s2)

        s2 = deepcopy(s1)
        self.assertEqual(s1, s2)

        s2.classification = [SimpleDC(SimpleDCTypes.PUB)]
        self.assertNotEqual(s1, s2)

        # Modify schema
        s2 = AvroSchema.AvroSchema('{"type":"record","name":"test","fields":[{"name":"f2other","type":"string"}]}', [SimpleDC(SimpleDCTypes.PC3)])
        self.assertNotEqual(s1, s2)

        # s1 is compatible with s1
        tree: ValidationTree = ValidationTree(s1)
        s1.isBackwardsCompatibleWith(s1, tree)
        self.assertFalse(tree.hasErrors())
        self.assertFalse(tree.hasIssues())

        # s1 is not compatible with s2
        tree = ValidationTree(s1)
        s1.isBackwardsCompatibleWith(s2, tree)
        self.assertTrue(tree.hasErrors())

        tree = ValidationTree(s2)
        s2.isBackwardsCompatibleWith(s1, tree)
        self.assertTrue(tree.hasErrors())

        s1 = AvroSchema.AvroSchema('{"type":"record","name":"test","fields":[{"name":"f2other","type":"string"}]}', [SimpleDC(SimpleDCTypes.PC3)],
                                   PrimaryKeyList(["f2other"]))

        tree = ValidationTree(s1)
        s1.lint(tree)

        self.assertFalse(tree.hasErrors())

    def test_DatasetClassificationLint(self):
        eco: Ecosystem = createEcosystem()
        tree: ValidationTree = eco.lintAndHydrateCaches()

        self.assertFalse(tree.hasErrors())
        self.assertFalse(tree.hasIssues())

        store: Datastore = eco.cache_getDatastoreOrThrow("NW_Data").datastore
        dataset: Dataset = store.datasets["suppliers"]
        schema: DDLTable = cast(DDLTable, dataset.originalSchema)
        self.assertEqual(dataset.dataClassificationOverride, [SimpleDC(SimpleDCTypes.PUB)])
        self.assertFalse(schema.hasDataClassifications())

        col: Optional[DDLColumn] = schema.getColumnByName("company_name")
        self.assertIsNotNone(col)
        if (col is None):
            raise Exception("Shouldnt be none")

        # Set a local classification on a column, a dataset level and one of these isn't allowed
        col.classification = [SimpleDC(SimpleDCTypes.PC1)]

        tree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.hasErrors())
        col.classification = None

        # Clear it and check it's good
        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())
        self.assertFalse(tree.hasIssues())

        # Now dataset and all columns do not have classification
        dataset.dataClassificationOverride = None

        # non production store: should have warnings, as dataset has no classifications set
        tree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())
        self.assertTrue(tree.hasIssues())

        self.assertEqual(store.productionStatus, ProductionStatus.NOT_PRODUCTION)

        # Production store:, dataset with no classifications is an error
        store.productionStatus = ProductionStatus.PRODUCTION

        tree = eco.lintAndHydrateCaches()
        self.assertTrue(tree.containsProblemType(ProductionDatastoreMustHaveClassifications))
        self.assertTrue(tree.hasIssues())
