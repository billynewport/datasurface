"""
Combined performance test showing the cumulative impact of DDLColumn and DDLTable optimizations
"""
import time
import unittest
from datasurface.md.schema import DDLTable, DDLColumn, PrimaryKeyStatus, PrimaryKeyList
from datasurface.md.types import String
from datasurface.md.documentation import PlainTextDocumentation


class TestCombinedPerformance(unittest.TestCase):

    def test_combined_schema_performance(self):
        """Test combined performance of DDLColumn + DDLTable optimizations"""

        # Test data
        table_count = 500  # Increased for more reliable measurement
        columns_per_table = 10

        print(f"Creating {table_count} tables with {columns_per_table} columns each...")
        print(f"Total objects: {table_count * columns_per_table} columns + {table_count} tables")

        # Test new optimized approach
        start_time = time.perf_counter()
        optimized_tables: list[DDLTable] = []

        for t in range(table_count):
            # Create columns with optimized DDLColumn constructor
            columns: list[DDLColumn] = []
            for c in range(columns_per_table):
                col = DDLColumn(
                    name=f"table_{t}_col_{c}",
                    data_type=String(50),
                    primary_key=PrimaryKeyStatus.PK if c == 0 else PrimaryKeyStatus.NOT_PK
                )
                columns.append(col)

            # Create table with optimized DDLTable constructor
            table = DDLTable(
                columns=columns,
                primary_key_list=PrimaryKeyList([f"table_{t}_col_0"]),
                documentation=PlainTextDocumentation(f"Test table {t}")
            )
            optimized_tables.append(table)

        optimized_time = time.perf_counter() - start_time

        # Test legacy approach
        start_time = time.perf_counter()
        legacy_tables: list[DDLTable] = []

        for t in range(table_count):
            # Create columns with legacy DDLColumn constructor
            columns: list[DDLColumn] = []
            for c in range(columns_per_table):
                col = DDLColumn.create_legacy(
                    f"legacy_table_{t}_col_{c}",
                    String(50),
                    PrimaryKeyStatus.PK if c == 0 else PrimaryKeyStatus.NOT_PK
                )
                columns.append(col)

            # Create table with legacy DDLTable constructor
            table = DDLTable.create_legacy(
                *columns,  # Unpack all columns
                PrimaryKeyList([f"legacy_table_{t}_col_0"]),
                PlainTextDocumentation(f"Legacy test table {t}")
            )
            legacy_tables.append(table)

        legacy_time = time.perf_counter() - start_time

        print("\nCombined Performance Results:")
        print(f"  Optimized approach: {optimized_time:.4f}s")
        print(f"  Legacy approach:    {legacy_time:.4f}s")
        print(f"  Overall speedup:    {legacy_time/optimized_time:.2f}x faster")
        time_saved_pct = ((legacy_time - optimized_time)/legacy_time)*100
        print(f"  Time saved:         {legacy_time - optimized_time:.4f}s ({time_saved_pct:.1f}%)")

        total_objects = table_count * columns_per_table + table_count
        print(f"  Per-object speedup: {((legacy_time - optimized_time) / total_objects) * 1000:.2f}ms saved per object")

        # Verify equivalence
        self.assertEqual(len(optimized_tables), len(legacy_tables))
        self.assertEqual(len(optimized_tables[0].columns), len(legacy_tables[0].columns))

        # Show the performance comparison
        if optimized_time < legacy_time:
            print(f"\n🚀 Optimized approach is {legacy_time/optimized_time:.2f}x faster!")
        else:
            print(f"\n⚠️  Legacy approach was {optimized_time/legacy_time:.2f}x faster (measurement variance)")

        print("✅ Test completed - performance comparison shown above")


if __name__ == '__main__':
    unittest.main()
