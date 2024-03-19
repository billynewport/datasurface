import unittest

from datasurface.md.AmazonAWS import AWSDMSIceBergDataPlatform, AWSSecret, AmazonAWSS3Bucket
from datasurface.md.Documentation import PlainTextDocumentation
from datasurface.md.GitOps import GitHubRepository
from datasurface.md.Governance import DataPlatformCICDExecutor, Ecosystem, InfrastructureLocation
from datasurface.md.Lint import NameHasBadSynthax, ValidationTree
from tests.nwdb.eco import createEcosystem


class TestAWS(unittest.TestCase):
    def test_AWSSecret(self):
        e: Ecosystem = createEcosystem()
        s: AWSSecret = AWSSecret("name", e.getLocationOrThrow("AWS", ["USA", "us-east-1"]))

        t: ValidationTree = ValidationTree(s)
        s.lint(e, t)
        self.assertFalse(t.hasErrors())

        s.secretName = "Secrets cannot have spaces"
        s.lint(e, t)
        self.assertTrue(t.containsProblemType(NameHasBadSynthax))


class TestAWSBatchDMSPlatform(unittest.TestCase):
    def test_AWSBatchDMSPlatform(self):
        e: Ecosystem = createEcosystem()
        eastRegion: InfrastructureLocation = e.getLocationOrThrow("AWS", ["USA", "us-east-1"])

        p: AWSDMSIceBergDataPlatform = AWSDMSIceBergDataPlatform(
            "TestBatch",
            PlainTextDocumentation("Test docs"),
            DataPlatformCICDExecutor(GitHubRepository("owner/repo", "main")),
            "vpcId",
            AWSSecret("platformRole", eastRegion),
            eastRegion,
            AmazonAWSS3Bucket("Staging", eastRegion, None, "TestStaging", "staging"),
            AmazonAWSS3Bucket("Ingestion", eastRegion, None, "TestStaging", "ingestion"),
            "GlueDatabaseName",
            "stagingIAMRole",
            "dataIAMRole",
            "awsGlueIAMRole")

        e.add(p)

        t: ValidationTree = e.lintAndHydrateCaches()
        self.assertFalse(t.hasErrors())
