"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

import unittest
from unittest import skipIf
from sqlalchemy import create_engine, text, MetaData
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md.lint import ValidationTree
from datasurface.md.sqlalchemyutils import datasetToSQLAlchemyTable
from datasurface.md.governance import Ecosystem
from typing import Optional

WWI_MODULE_DIR = "src/tests/wwi"


class Test_WWI_Postgres(unittest.TestCase):
    def setUp(self) -> None:
        """Set up test database connection."""
        self.engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
        self.test_db_name = 'test_wwi_db'

        # Create test database
        with self.engine.begin() as conn:
            conn.execute(text("COMMIT"))
            try:
                conn.execute(text(f"CREATE DATABASE {self.test_db_name}"))
            except Exception:
                pass  # Database might already exist

        # Connect to the test database
        self.test_engine = create_engine(f'postgresql://postgres:postgres@localhost:5432/{self.test_db_name}')

        # Enable PostGIS extension for spatial support
        self.postgis_available = False
        try:
            with self.test_engine.begin() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
                self.postgis_available = True
        except Exception as e:
            print(f"Warning: Could not enable PostGIS extension: {e}")
            print("Spatial tests will be skipped")

    def tearDown(self) -> None:
        """Clean up test database."""
        self.test_engine.dispose()

        # Drop test database
        with self.engine.begin() as conn:
            conn.execute(text("COMMIT"))
            try:
                # Terminate existing connections to the test database
                conn.execute(text(f"""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = '{self.test_db_name}' AND pid <> pg_backend_pid()
                """))
                conn.execute(text(f"DROP DATABASE IF EXISTS {self.test_db_name}"))
            except Exception as e:
                print(f"Warning: Could not drop test database {self.test_db_name}: {e}")

        self.engine.dispose()

    def test_load_wwi_ecosystem(self) -> None:
        """Test that the WWI ecosystem loads correctly."""
        eco: Optional[Ecosystem]
        ecoTree: Optional[ValidationTree]
        eco, ecoTree = loadEcosystemFromEcoModule(WWI_MODULE_DIR)

        if eco is None or (ecoTree is not None and ecoTree.hasErrors()):
            if ecoTree is not None:
                ecoTree.printTree()
            self.fail("Failed to load WWI ecosystem")

        self.assertIsNotNone(eco)
        self.assertEqual(eco.name, "WorldWideImporters")

        # Validate USA governance zone exists
        usa_zone = eco.getZoneOrThrow("USA")
        self.assertIsNotNone(usa_zone)

        # Validate team1 exists
        wwi_team = usa_zone.getTeamOrThrow("team1")
        self.assertIsNotNone(wwi_team)

        # Validate WWI_Data datastore exists
        wwi_datastore = wwi_team.dataStores["WWI_Data"]
        self.assertIsNotNone(wwi_datastore)

        print(f"WWI ecosystem loaded successfully with {len(wwi_datastore.datasets)} datasets")

    def test_create_wwi_tables_in_postgres(self) -> None:
        """Test creating ALL WWI tables in PostgreSQL using SQLAlchemy utilities.

        The WWI ecosystem contains 31 total datasets:
        - 28 non-spatial tables (always tested)
        - 3 spatial tables (only tested if PostGIS is available)
        This test creates real PostgreSQL tables and validates the schema.
        """
        # Load the WWI ecosystem
        eco: Optional[Ecosystem]
        ecoTree: Optional[ValidationTree]
        eco, ecoTree = loadEcosystemFromEcoModule(WWI_MODULE_DIR)

        if eco is None or (ecoTree is not None and ecoTree.hasErrors()):
            if ecoTree is not None:
                ecoTree.printTree()
            self.fail("Failed to load WWI ecosystem")

        assert eco is not None
        usa_zone = eco.getZoneOrThrow("USA")
        wwi_team = usa_zone.getTeamOrThrow("team1")
        wwi_datastore = wwi_team.dataStores["WWI_Data"]

        # Test creating tables for ALL WWI datasets
        all_datasets = list(wwi_datastore.datasets.keys())

        # Filter out spatial tables if PostGIS is not available
        spatial_tables = {'cities', 'countries', 'state_provinces'}
        if not getattr(self, 'postgis_available', False):
            test_datasets = [name for name in all_datasets if name not in spatial_tables]
            print(f"PostGIS not available - testing {len(test_datasets)} non-spatial tables")
            print(f"Skipping spatial tables: {sorted(spatial_tables)}")
        else:
            test_datasets = all_datasets
            print(f"PostGIS available - testing all {len(test_datasets)} tables including spatial ones")

        metadata = MetaData()
        created_tables = []

        print("\n=== Creating WWI Tables in PostgreSQL ===")

        for dataset_name in test_datasets:
            dataset = wwi_datastore.datasets[dataset_name]

            # Convert DataSurface dataset to SQLAlchemy table
            sqlalchemy_table = datasetToSQLAlchemyTable(dataset, dataset_name, metadata, self.test_engine)
            created_tables.append(sqlalchemy_table)

            print(f"Converted dataset '{dataset_name}' to SQLAlchemy table")
            print(f"   Columns: {len(sqlalchemy_table.columns)}")

            # Check for Geography columns specifically
            geo_columns = [col for col in sqlalchemy_table.columns
                           if 'Geography' in str(type(col.type))]
            if geo_columns:
                for geo_col in geo_columns:
                    print(f"   Geography column: {geo_col.name} ({type(geo_col.type)})")
                    srid_val = getattr(geo_col.type, 'srid', 'N/A')
                    geom_type_val = getattr(geo_col.type, 'geometry_type', 'N/A')
                    print(f"      SRID: {srid_val}")
                    print(f"      Geometry Type: {geom_type_val}")

        # Create all tables in the test database
        print(f"\n=== Creating {len(created_tables)} tables in PostgreSQL ===")
        print(f"Total datasets to create: {len(test_datasets)}")

        with self.test_engine.begin() as conn:
            # Create the tables
            metadata.create_all(self.test_engine)

            # Verify tables were created
            result = conn.execute(text("""
                SELECT table_name, column_name, data_type, udt_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position
            """))

            table_info = {}
            for row in result:
                table_name, column_name, data_type, udt_name = row
                if table_name not in table_info:
                    table_info[table_name] = []
                table_info[table_name].append({
                    'column': column_name,
                    'type': data_type,
                    'udt_name': udt_name
                })

            print("\n=== Verification: Created Tables ===")
            for table_name in test_datasets:
                self.assertIn(table_name, table_info, f"Table '{table_name}' should be created")
                columns = table_info[table_name]
                print(f"Table '{table_name}': {len(columns)} columns")

                # Check for specific columns we expect
                column_names = [col['column'] for col in columns]

                # All tables should have an ID column
                id_columns = [col for col in column_names if 'id' in col.lower()]
                self.assertTrue(len(id_columns) > 0, f"Table '{table_name}' should have at least one ID column")

                # Check for Geography columns (none expected in this basic test)
                geo_columns = [col for col in columns if col['udt_name'] == 'geometry']
                if len(geo_columns) > 0:
                    for geo_col in geo_columns:
                        print(f"   Geography column: {geo_col['column']} (PostgreSQL type: {geo_col['udt_name']})")

        print(f"Successfully created and verified {len(created_tables)} WWI tables in PostgreSQL")

    @skipIf(not hasattr(unittest.TestCase, 'postgis_available'), "PostGIS not available")
    def test_geography_column_creation(self) -> None:
        """Test that Geography columns are created correctly with proper spatial types."""
        if not getattr(self, 'postgis_available', False):
            self.skipTest("PostGIS extension not available")
        # Load the WWI ecosystem
        eco: Optional[Ecosystem]
        ecoTree: Optional[ValidationTree]
        eco, ecoTree = loadEcosystemFromEcoModule(WWI_MODULE_DIR)

        if eco is None or (ecoTree is not None and ecoTree.hasErrors()):
            if ecoTree is not None:
                ecoTree.printTree()
            self.fail("Failed to load WWI ecosystem")

        assert eco is not None
        usa_zone = eco.getZoneOrThrow("USA")
        wwi_team = usa_zone.getTeamOrThrow("team1")
        wwi_datastore = wwi_team.dataStores["WWI_Data"]

        # Test specifically the tables with Geography columns
        geography_test_cases = [
            ("cities", "location", "POINT"),
            ("countries", "border", "MULTIPOLYGON"),
            ("state_provinces", "border", "MULTIPOLYGON")
        ]

        metadata = MetaData()

        print("\n=== Testing Geography Column Creation ===")

        for table_name, geo_column, expected_geom_type in geography_test_cases:
            dataset = wwi_datastore.datasets[table_name]
            sqlalchemy_table = datasetToSQLAlchemyTable(dataset, table_name, metadata, self.test_engine)

            # Find the geography column
            geo_col = None
            for col in sqlalchemy_table.columns:
                if col.name == geo_column:
                    geo_col = col
                    break

            self.assertIsNotNone(geo_col, f"Geography column '{geo_column}' should exist in table '{table_name}'")
            assert geo_col is not None  # Type narrowing

            # Verify it's a Geography type from GeoAlchemy2
            self.assertIn('Geography', str(type(geo_col.type)),
                          f"Column '{geo_column}' should be a Geography type")

            # Check SRID (should be 4326 for WGS84)
            srid_val = getattr(geo_col.type, 'srid', None)
            if srid_val is not None:
                self.assertEqual(srid_val, 4326,
                                 f"Geography column '{geo_column}' should have SRID 4326 (WGS84)")

            # Check geometry type
            geom_type_val = getattr(geo_col.type, 'geometry_type', None)
            if geom_type_val is not None:
                self.assertEqual(geom_type_val, expected_geom_type,
                                 f"Geography column '{geo_column}' should have geometry type '{expected_geom_type}'")

            srid_val = getattr(geo_col.type, 'srid', 'N/A')
            geom_type_val = getattr(geo_col.type, 'geometry_type', 'N/A')
            print(f"{table_name}.{geo_column}: {type(geo_col.type)} with SRID {srid_val} and type {geom_type_val}")

        # Create the tables and verify in PostgreSQL
        with self.test_engine.begin() as conn:
            metadata.create_all(self.test_engine)

            # Query the actual PostgreSQL geometry columns
            result = conn.execute(text("""
                SELECT f_table_name, f_geometry_column, coord_dimension, srid, type
                FROM geometry_columns
                WHERE f_table_schema = 'public'
                ORDER BY f_table_name, f_geometry_column
            """))

            print("\n=== PostgreSQL Geometry Columns Information ===")
            geometry_info = list(result)

            for table_name, geo_column, expected_geom_type in geography_test_cases:
                # Find matching geometry column info
                matching_info = None
                for row in geometry_info:
                    if row[0] == table_name and row[1] == geo_column:
                        matching_info = row
                        break

                if matching_info:
                    table_name_pg, col_name_pg, coord_dim, srid, geom_type = matching_info
                    print(f"{table_name_pg}.{col_name_pg}: {geom_type} (SRID: {srid}, Dimensions: {coord_dim})")

                    # Verify SRID is correct
                    self.assertEqual(srid, 4326, f"PostgreSQL SRID should be 4326 for {table_name}.{geo_column}")
                else:
                    print(f"No geometry column info found for {table_name}.{geo_column}")

        print("Geography column creation and verification completed")


if __name__ == '__main__':
    unittest.main()
