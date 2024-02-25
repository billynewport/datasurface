from typing import Optional
from datasurface.md import Documentation
from datasurface.md.Governance import CloudVendor, DataContainer, DataPlatform, Ecosystem, InfrastructureLocation, \
    InfrastructureVendor, ObjectStorage
from datasurface.md.Lint import ValidationTree


class AmazonAWSDataPlatform(DataPlatform):
    def __init__(self, name: str, doc: Documentation):
        super().__init__(name, doc)

    def getSupportedVendors(self, eco: Ecosystem) -> set[InfrastructureVendor]:
        rc: set[InfrastructureVendor] = set()
        iv: InfrastructureVendor = eco.getVendorOrThrow("AWS")
        rc.add(iv)
        return rc

    def __hash__(self) -> int:
        return hash(self.name)

    def _str__(self) -> str:
        return f"AmazonAWSDataPlatform({self.name})"

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, AmazonAWSDataPlatform)

    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        return dc.isUsingVendorsOnly(eco, {CloudVendor.AWS})

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass

    def getInternalDataContainers(self) -> set[DataContainer]:
        # TODO: Implement this method
        return set()


class AmazonAWSS3Bucket(ObjectStorage):
    def __init__(self, name: str, loc: InfrastructureLocation, endPointURI: Optional[str], bucketName: str, prefix: Optional[str]):
        super().__init__(name, loc, endPointURI, bucketName, prefix)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSS3Bucket)

    def __hash__(self) -> int:
        return hash(self.name)


class AmazonAWSKinesis(DataContainer):
    def __init__(self, name: str, loc: InfrastructureLocation, endPointURI: Optional[str]):
        super().__init__(name, loc)
        self.endPointURI: Optional[str] = endPointURI

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSKinesis) and \
            self.endPointURI == o.endPointURI

    def __hash__(self) -> int:
        return hash(self.name)


class AmazonAWSDynamoDB(DataContainer):
    def __init__(self, name: str, loc: InfrastructureLocation, endPointURI: Optional[str]):
        super().__init__(name, loc)
        self.endPointURI: Optional[str] = endPointURI

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSDynamoDB) and \
            self.endPointURI == o.endPointURI

    def __hash__(self) -> int:
        return hash(self.name)


class AmazonAWSSQS(DataContainer):
    def __init__(self, name: str, loc: InfrastructureLocation, queueURL: Optional[str]):
        super().__init__(name, loc)
        self.queueURL: Optional[str] = queueURL

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, AmazonAWSSQS) and \
            self.queueURL == o.queueURL

    def __hash__(self) -> int:
        return hash(self.name)
