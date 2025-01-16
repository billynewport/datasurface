"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import DataPlatform, DataPlatformExecutor, Documentation, Ecosystem, ValidationTree, CloudVendor, DataContainer, \
    ValidationProblem, ProblemSeverity, PlatformPipelineGraph, DataPlatformGraphHandler


class ZeroPlatformExecutor(DataPlatformExecutor):
    def __init__(self):
        super().__init__()

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass


class ZeroDataPlatform(DataPlatform):
    """This needs to store how to use the resources needed by this data platform. These include connection info to attach to the s3 service and associated bucket names and prefixes as well
    as a spark master and an airflow server. The airflow will primarily be used through a gitlab repository which airflow is syncing with,"""
    def __init__(
            self,
            name: str,
            doc: Documentation,
            stagingBucketName: str,
            dataBucketName: str,
            endPointUrl: str,
            credentialFileName: str):
        super().__init__(name, doc, ZeroPlatformExecutor())
        self.endPointUrl: str = endPointUrl
        """The s3 compatible endpoint to connect to, usually http://host:port"""
        self.stagingBucketName: str = stagingBucketName
        """The bucket to use for all staging data"""
        self.dataBucketName: str = dataBucketName
        """The bucket to use for all data"""
        self.credentialFileName: str = credentialFileName
        """The name of the file that contains the access_key on the first line and the secret key on the secondline"""

    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        return {CloudVendor.PRIVATE}

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        pass

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)

    def createGraphHandler(self, graph: PlatformPipelineGraph) -> DataPlatformGraphHandler:
        pass
