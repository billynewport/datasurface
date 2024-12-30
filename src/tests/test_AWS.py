"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import os
from typing import cast
import unittest

from datasurface.platforms.aws import AWSAuroraDatabase, AWSDMSTerraformFileFragmentManager, AWSDMSTerraformIaC, \
    AWSDMSIceBergDataPlatform, AWSSecret, AmazonAWSS3Bucket, \
    is_valid_s3_bucket_name
from datasurface.md import PlainTextDocumentation
from datasurface.md import GitHubRepository
from datasurface.md import CDCCaptureIngestion, DataPlatformCICDExecutor, Dataset, DatasetGroup, DatasetSink, \
    Datastore, DeprecationsAllowed, Ecosystem, InfrastructureLocation, IngestionConsistencyType, ProductionStatus, \
    Team, Workspace, WorkspaceFixedDataPlatform
from datasurface.md import FileBasedFragmentManager
from datasurface.md import NameHasBadSynthax, ValidationTree
from datasurface.md import EcosystemPipelineGraph, PlatformPipelineGraph, DataPlatformKey
from datasurface.md import SimpleDC, SimpleDCTypes
from datasurface.md import DDLColumn, DDLTable, NullableStatus, PrimaryKeyStatus, VarChar
from tests.nwdb.eco import createEcosystem


class TestAWS(unittest.TestCase):
    def test_AWSSecret(self):
        e: Ecosystem = createEcosystem()
        s: AWSSecret = AWSSecret("name", {e.getLocationOrThrow("MyCorp", ["USA", "NY_1"])})

        t: ValidationTree = ValidationTree(s)
        s.lint(e, t)
        self.assertFalse(t.hasErrors())

        s.secretName = "Secrets cannot have spaces"
        s.lint(e, t)
        self.assertTrue(t.containsProblemType(NameHasBadSynthax))


class TestAWSValidation(unittest.TestCase):
    def test_is_valid_s3_bucket_name(self):
        self.assertTrue(is_valid_s3_bucket_name('mybucket'))  # Valid name
        self.assertTrue(is_valid_s3_bucket_name('my.bucket'))  # Valid name with a dot
        self.assertTrue(is_valid_s3_bucket_name('my-bucket'))  # Valid name with a hyphen
        self.assertTrue(is_valid_s3_bucket_name('my.bucket-with.multiple.labels'))  # Valid name with multiple labels

        self.assertFalse(is_valid_s3_bucket_name('MyBucket'))  # Invalid name (contains uppercase letters)
        self.assertFalse(is_valid_s3_bucket_name('my_bucket'))  # Invalid name (contains underscore)
        self.assertFalse(is_valid_s3_bucket_name('mybucket.'))  # Invalid name (ends with a dot)
        self.assertFalse(is_valid_s3_bucket_name('.mybucket'))  # Invalid name (starts with a dot)
        self.assertFalse(is_valid_s3_bucket_name('mybucket-'))  # Invalid name (ends with a hyphen)
        self.assertFalse(is_valid_s3_bucket_name('-mybucket'))  # Invalid name (starts with a hyphen)
        self.assertFalse(is_valid_s3_bucket_name('my--bucket'))  # Invalid name (contains two consecutive hyphens)
        self.assertFalse(is_valid_s3_bucket_name('my.-bucket'))  # Invalid name (contains a dot followed by a hyphen)
        self.assertFalse(is_valid_s3_bucket_name('my-.bucket'))  # Invalid name (contains a hyphen followed by a dot)
        self.assertFalse(is_valid_s3_bucket_name('192.168.5.4'))  # Invalid name (formatted as an IP address)
        self.assertFalse(is_valid_s3_bucket_name('a'*256))  # Invalid name (too long)
        self.assertFalse(is_valid_s3_bucket_name(''))  # Invalid name (empty string)


class TestAWSBatchDMSPlatform(unittest.TestCase):

    def createAWSBatchPlatform(self, locs: set[InfrastructureLocation]) -> AWSDMSIceBergDataPlatform:
        p: AWSDMSIceBergDataPlatform = AWSDMSIceBergDataPlatform(
            "TestBatch",
            PlainTextDocumentation("Test docs"),
            DataPlatformCICDExecutor(GitHubRepository("owner/repo", "main")),  # Push generated IaC to this repo
            "vpcId",
            AWSSecret("platformRole", locs),
            locs,
            AmazonAWSS3Bucket("Staging", locs, None, "test-staging", "staging"),
            AmazonAWSS3Bucket("Ingestion", locs, None, "test-staging", "ingestion"),
            AmazonAWSS3Bucket("Code", locs, None, "test-staging", "code"),
            "GlueDatabaseName",
            "stagingIAMRole",
            "dataIAMRole",
            "awsGlueIAMRole")
        return p

    def test_AWSBatchDMSPlatform(self):
        e: Ecosystem = createEcosystem()
        eastRegion: InfrastructureLocation = e.getLocationOrThrow("MyCorp", ["USA", "NY_1"])

        p: AWSDMSIceBergDataPlatform = self.createAWSBatchPlatform({eastRegion})

        e.add(p)

        t: ValidationTree = e.lintAndHydrateCaches()
        t.printTree()
        self.assertFalse(t.hasErrors())


"""
     def test_awsProducer(self):
        eco: Ecosystem = createEcosystem()
        eu_west_3: InfrastructureLocation = eco.getLocationOrThrow("AWS", ["EU", "eu-west-3"])

        # Add AWS Batch DMS Platform
        eco.add(self.createAWSBatchPlatform({eu_west_3}))

        dmsPlat: AWSDMSIceBergDataPlatform = cast(AWSDMSIceBergDataPlatform, eco.getDataPlatformOrThrow("TestBatch"))

        producer: Datastore = Datastore(
            "TestProducer",
            PlainTextDocumentation("Test docs"),
            CDCCaptureIngestion(
                AWSAuroraDatabase(
                    "TestDB",
                    {eu_west_3},
                    "endpoint_producer",
                    "CustomerDB"
                ),
                AWSSecret(
                    "TestProducerIngestionCred",
                    {eu_west_3}),
                IngestionConsistencyType.MULTI_DATASET
                ),
            ProductionStatus.PRODUCTION,
            Dataset(
                "customers",
                SimpleDC(SimpleDCTypes.PC3),
                PlainTextDocumentation("This data includes customer information from the Northwind database. It contains PII data."),
                DDLTable(
                    DDLColumn("customer_id", VarChar(5), NullableStatus.NOT_NULLABLE, PrimaryKeyStatus.PK),
                    DDLColumn("company_name", VarChar(40), NullableStatus.NOT_NULLABLE),
                    DDLColumn("contact_name", VarChar(30)),
                    DDLColumn("contact_title", VarChar(30)),
                    DDLColumn("address", VarChar(60)),
                    DDLColumn("city", VarChar(15)),
                    DDLColumn("region", VarChar(15)),
                    DDLColumn("postal_code", VarChar(10)),
                    DDLColumn("country", VarChar(15)),
                    DDLColumn("phone", VarChar(24)),
                    DDLColumn("fax", VarChar(24))
                )
            ),
        )

        teamFrontOffice: Team = eco.getTeamOrThrow("EU", "FrontOffice")
        teamFrontOffice.add(producer)

        tree: ValidationTree = eco.lintAndHydrateCaches()
        self.assertFalse(tree.hasErrors())

        w: Workspace = Workspace(
            "TestWorkspace",
            PlainTextDocumentation("Test docs"),
            AWSAuroraDatabase(
                "TestConsumerDB",
                {eu_west_3},
                "endpoint_consumer",
                "CustomerDB"
            ),
            ProductionStatus.PRODUCTION,
            DatasetGroup(
                "ConsumerGroupA",
                PlainTextDocumentation("Test docs"),
                WorkspaceFixedDataPlatform(DataPlatformKey("TestBatch")),
                DatasetSink("TestProducer", "customers", DeprecationsAllowed.NEVER)
            )
        )

        teamFrontOffice.add(w)
        tree = eco.lintAndHydrateCaches()
        tree.printTree()
        self.assertFalse(tree.hasErrors())

        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)

        self.assertIsNotNone(graph.roots.get(dmsPlat))

        pi: PlatformPipelineGraph = graph.roots[dmsPlat]
        self.assertEqual(len(pi.workspaces), 1)

        render: AWSDMSTerraformIaC = AWSDMSTerraformIaC(dmsPlat.executor, pi)

        fileMgr: FileBasedFragmentManager = AWSDMSTerraformFileFragmentManager(
            dmsPlat,
            render
        )

        # Add the boiler plate code for the IaC to fileMgr here

        render.renderIaC(fileMgr)

        self.assertTrue(os.path.exists(fileMgr.rootDir))
        self.assertTrue(os.path.exists(fileMgr.rootDir + "/module/dms_ingest/main.tf"))
        self.assertTrue(os.path.exists(fileMgr.rootDir + "/module/glue_table/main.tf"))
        self.assertTrue(os.path.exists(fileMgr.rootDir + "/provider.tf"))
        self.assertTrue(os.path.exists(fileMgr.rootDir + "/main.tf"))
 """
