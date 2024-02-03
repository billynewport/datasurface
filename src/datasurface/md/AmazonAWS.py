

from datasurface.md.Governance import DataPlatform, Ecosystem, InfrastructureVendor
from datasurface.md.Lint import ValidationTree


class AmazonAWSDataPlatform(DataPlatform):
    def __init__(self, name : str):
        super().__init__(name)
    
    def getSupportedVendors(self, eco : Ecosystem) -> set[InfrastructureVendor]:
        rc : set[InfrastructureVendor] = set()
        iv : InfrastructureVendor = eco.getVendorOrThrow("AWS")
        rc.add(iv)
        return rc
    
    def __hash__(self) -> int:
        return hash(self.name)
    
    def _str__(self) -> str:
        return f"AmazonAWSDataPlatform({self.name})"
    
    def __eq__(self, __value : object) -> bool:
        return super().__eq__(__value) and isinstance(__value, AmazonAWSDataPlatform)
    
    def lint(self, eco : Ecosystem, tree : ValidationTree):
        pass
