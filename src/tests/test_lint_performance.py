"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""
import unittest

from datasurface.md import Ecosystem, GitHubRepository, PlainTextDocumentation, DefaultDataPlatform, DataPlatformKey, GovernanceZoneDeclaration, \
                           InfrastructureVendor, CloudVendor, InfrastructureLocation, TeamDeclaration, GovernanceZone, Team, Datastore, Dataset, \
                           DDLColumn, DDLTable, String, PrimaryKeyStatus

from datasurface.platforms.legacy import LegacyDataPlatform
import time


class TestLintPerformance(unittest.TestCase):
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

    def test_lint_performance(self):
        start_time = time.time()
        ecosys: Ecosystem = self.createScaledEcosystem(10, 100, 50)
        end_time = time.time()
        print(f"Time taken to create ecosystem: {end_time - start_time} seconds")
        start_time = time.time()
        ecosys.lintAndHydrateCaches()
        end_time = time.time()
        print(f"Time taken to lint: {end_time - start_time} seconds")


if __name__ == '__main__':
    unittest.main()
