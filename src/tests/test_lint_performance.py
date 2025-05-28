"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""
import unittest

from datasurface.md import Ecosystem, DefaultDataPlatform, DataPlatformKey, GovernanceZoneDeclaration, \
                           InfrastructureVendor, CloudVendor, InfrastructureLocation, TeamDeclaration, GovernanceZone, Team, Datastore, Dataset, \
                           DDLColumn, DDLTable, PrimaryKeyStatus
from datasurface.md.repo import GitHubRepository
from datasurface.md.lint import track_sources, enable_source_tracking, disable_source_tracking, is_source_tracking_enabled
from datasurface.md.types import String
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.platforms.legacy import LegacyDataPlatform
import time


class TestLintPerformance(unittest.TestCase):

    @track_sources
    def test_SourceCaptureForDSLObjects(self):
        store: Datastore = Datastore("Store1")
        # Split the source reference string into filename and line number
        source_ref = store.getSourceReferenceString()
        filename, lineno = source_ref.split('@')[1].split(':')
        # Only keep the filename, not the folder
        filename = filename.split('/')[-1]
        self.assertEqual(filename, "test_lint_performance.py")
        self.assertEqual(lineno, "22")

    def benchmark_object_creation(self, count: int = 10000) -> float:
        """Benchmark creating many UserDSLObject instances"""
        start_time = time.perf_counter()

        objects = []
        for i in range(count):
            # Create objects that use UserDSLObject constructor
            store = Datastore(f"Store{i}")
            dataset = Dataset(f"Dataset{i}")
            column = DDLColumn(f"Column{i}", String(10))
            objects.extend([store, dataset, column])  # type: ignore

        end_time = time.perf_counter()
        return end_time - start_time

    def createScaledEcosystem(self, numStores: int, numDatasetsPerStore: int, numColumnsPerDataset: int) -> Ecosystem:
        ecosys: Ecosystem = Ecosystem(
            "Test",
            GitHubRepository("billynewport/repo", "ECOmain"),
            LegacyDataPlatform(
                "LegacyA",
                PlainTextDocumentation("Test")),

            # Data Platforms
            DefaultDataPlatform(DataPlatformKey("LegacyA")),

            # GovernanceZones
            GovernanceZoneDeclaration("USA", GitHubRepository("billynewport/repo", "USAmain")),

            # Infra Vendors and locations
            InfrastructureVendor(
                "LegacyA",
                CloudVendor.PRIVATE,
                PlainTextDocumentation("Legacy infrastructure"),
                InfrastructureLocation(
                    "USA",
                    InfrastructureLocation("ny1"),  # New York City
                    InfrastructureLocation("nj1")))  # New Jersey
        )

        gzUSA: GovernanceZone = ecosys.getZoneOrThrow("USA")

        gzUSA.add(
                TeamDeclaration("LegacyApplicationTeam", GitHubRepository("billynewport/repo", "legacyAppTeamMain")),
            )

        legacy_Team: Team = ecosys.getTeamOrThrow("USA", "LegacyApplicationTeam")

        for storeIdx in range(numStores):
            storeName: str = f"Store{storeIdx}"
            store: Datastore = Datastore(storeName)
            for datasetIdx in range(numDatasetsPerStore):
                datasetName: str = f"Dataset{datasetIdx}"
                dataset: Dataset = Dataset(datasetName)
                schema: DDLTable = DDLTable(DDLColumn("id", String(10), PrimaryKeyStatus.PK))
                dataset.add(schema)
                for columnIdx in range(numColumnsPerDataset):
                    columnName: str = f"Column{columnIdx}"
                    column: DDLColumn = DDLColumn(columnName, String(10))
                    schema.add(column)
            legacy_Team.add(store)

        return ecosys

    def measure_lint_performance(self, test_name: str):
        start_time = time.perf_counter()
        ecosys: Ecosystem = self.createScaledEcosystem(50, 100, 50)
        creation_time = time.perf_counter() - start_time

        start_time = time.perf_counter()
        ecosys.lintAndHydrateCaches()
        lint_time = time.perf_counter() - start_time

        print(f"{test_name}: Creation time: {creation_time:.3f}s, Lint time: {lint_time:.3f}s")
        return creation_time, lint_time

    def test_object_creation_performance_comparison(self):
        """Test the performance difference between source tracking enabled/disabled"""
        print(f"Initial source tracking state: {is_source_tracking_enabled()}")

        # Test with source tracking disabled (should be faster)
        disable_source_tracking()
        print(f"Source tracking disabled: {is_source_tracking_enabled()}")
        disabled_time = self.benchmark_object_creation(10000)

        # Test with source tracking enabled (should be slower)
        enable_source_tracking()
        print(f"Source tracking enabled: {is_source_tracking_enabled()}")
        enabled_time = self.benchmark_object_creation(10000)

        # Restore original state
        disable_source_tracking()

        print("Object Creation Performance Results:")
        print(f"  Source tracking DISABLED: {disabled_time:.4f}s")
        print(f"  Source tracking ENABLED:  {enabled_time:.4f}s")
        print(f"  Speedup: {enabled_time/disabled_time:.2f}x")

        # The disabled version should be faster
        self.assertLess(disabled_time, enabled_time, "Source tracking disabled should be faster")

    def test_lint_performance_with_source_tracking(self):
        enable_source_tracking()
        try:
            _, _ = self.measure_lint_performance("WITH source tracking")
        finally:
            disable_source_tracking()

    def test_lint_performance_without_source_tracking(self):
        disable_source_tracking()
        _, _ = self.measure_lint_performance("WITHOUT source tracking")


if __name__ == '__main__':
    unittest.main()
