

from datasurface.md.Governance import DataPlatform, Ecosystem, InfrastructureVendor


class AmazonAWSDataPlatform(DataPlatform):
    def __init__(self, name : str):
        super().__init__(name)
    
    def getSupportedVendors(self, eco : Ecosystem) -> set[InfrastructureVendor]:
        rc : set[InfrastructureVendor] = set()
        iv : InfrastructureVendor = eco.getVendorOrThrow("AWS")
        rc.add(iv)
        return rc
    
    def _str__(self) -> str:
        return f"AmazonAWSDataPlatform({self.name})"
    
