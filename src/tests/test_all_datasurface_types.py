#!/usr/bin/env python3

"""Comprehensive test for all DataSurface type conversions to SQLAlchemy types"""

from sqlalchemy import create_engine
from sqlalchemy.types import (
    Boolean as SQLBoolean, Integer as SQLInteger, BigInteger, Float as SQLFloat, Double, DECIMAL,
    Date as SQLDate, TIMESTAMP, Interval as SQLInterval, LargeBinary, CHAR, VARCHAR
)
from geoalchemy2 import Geography as GA2Geography
from datasurface.md.sqlalchemyutils import ddlColumnToSQLAlchemyType
from datasurface.md.schema import DDLColumn, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import (
    Boolean, Integer, BigInt, Float, IEEE64, Decimal,
    Date, Timestamp, Interval, Variant, Char, NChar, VarChar, NVarChar,
    Geography, GeometryType, SpatialReferenceSystem
)
import unittest


class TestAllDatasurfaceTypes(unittest.TestCase):

    def test_all_datasurface_types(self):
        """Test conversion of all DataSurface types to SQLAlchemy types."""

        # Create both PostgreSQL and SQL Server engines for comparison
        pg_engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
        # Note: SQL Server engine would be: create_engine('mssql+pyodbc://...')

        print("=== DataSurface Type Conversion Test ===")
        print(f"Testing with PostgreSQL engine: {pg_engine.dialect.name}")
        print()

        # Define test cases: (name, datasurface_type, expected_sqlalchemy_type)
        test_cases = [
            # Basic types
            ("Boolean", Boolean(), SQLBoolean),
            ("Integer", Integer(), SQLInteger),
            ("BigInt", BigInt(), BigInteger),
            ("Float", Float(), SQLFloat),
            ("IEEE64", IEEE64(), Double),
            ("Decimal(10,2)", Decimal(maxSize=10, precision=2), DECIMAL),

            # Date/Time types
            ("Date", Date(), SQLDate),
            ("Timestamp", Timestamp(), TIMESTAMP),
            ("Interval", Interval(), SQLInterval),

            # Binary type
            ("Variant(1000)", Variant(maxSize=1000), LargeBinary),

            # Character types (the critical ones)
            ("Char(10)", Char(maxSize=10, collationString="utf8_general_ci"), CHAR),
            ("NChar(20)", NChar(maxSize=20, collationString="utf8_general_ci"), CHAR),
            ("VarChar(50)", VarChar(maxSize=50, collationString="utf8_general_ci"), VARCHAR),
            ("NVarChar(100)", NVarChar(maxSize=100, collationString="utf8_general_ci"), VARCHAR),

            # Geography type
            ("Geography(Point)", Geography(SpatialReferenceSystem(4326), GeometryType.POINT), GA2Geography),
        ]

        success_count = 0
        total_count = len(test_cases)

        for type_name, datasurface_type, expected_sqlalchemy_type in test_cases:
            try:
                # Create DDL column
                ddl_column = DDLColumn(
                    name=f"test_{type_name.lower().replace('(', '_').replace(')', '').replace(',', '_')}",
                    data_type=datasurface_type,
                    nullable=NullableStatus.NULLABLE,
                    primary_key=PrimaryKeyStatus.NOT_PK
                )

                # Convert to SQLAlchemy type
                sqlalchemy_column = ddlColumnToSQLAlchemyType(ddl_column, pg_engine)

                # Verify the type is what we expect
                self.assertIsInstance(sqlalchemy_column.type, expected_sqlalchemy_type,
                                      f"Expected {expected_sqlalchemy_type.__name__}, got {type(sqlalchemy_column.type).__name__}")

                # Get the SQL representation
                sql_type = sqlalchemy_column.type.compile(pg_engine.dialect)

                print(f"✅ {type_name:<20} -> {type(sqlalchemy_column.type).__name__:<15} -> SQL: {sql_type}")
                success_count += 1

            except Exception as e:
                self.fail(f"❌ {type_name:<20} -> ERROR: {e}")

        print()
        print(f"=== Results: {success_count}/{total_count} types converted successfully ===")

        # Check for problematic types - ensure N* types map to non-unicode SQLAlchemy types
        print()
        print("=== Critical Unicode Type Check ===")

        unicode_types = [
            ("NChar(32)", NChar(maxSize=32), CHAR),
            ("NVarChar(50)", NVarChar(maxSize=50), VARCHAR),
        ]

        for type_name, datasurface_type, expected_sqlalchemy_type in unicode_types:
            ddl_column = DDLColumn(
                name="test_unicode",
                data_type=datasurface_type,
                nullable=NullableStatus.NULLABLE,
                primary_key=PrimaryKeyStatus.NOT_PK
            )

            sqlalchemy_column = ddlColumnToSQLAlchemyType(ddl_column, pg_engine)

            # Verify correct SQLAlchemy type (should be CHAR/VARCHAR, not NCHAR/NVARCHAR)
            self.assertIsInstance(sqlalchemy_column.type, expected_sqlalchemy_type,
                                  f"Expected {expected_sqlalchemy_type.__name__}, got {type(sqlalchemy_column.type).__name__}")

            sql_type = str(sqlalchemy_column.type.compile(pg_engine.dialect))

            if 'NVARCHAR' in sql_type or 'NCHAR' in sql_type:
                self.fail(f"🚨 PROBLEM: {type_name} -> {sql_type} (contains problematic N* type)")
            else:
                print(f"✅ GOOD: {type_name} -> {type(sqlalchemy_column.type).__name__} -> SQL: {sql_type}")

        print()
        print("=== Test Complete ===")
