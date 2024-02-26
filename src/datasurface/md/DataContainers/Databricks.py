from datasurface.md.Governance import InfrastructureLocation, URLSQLDatabase


class DataBricksWarehouse(URLSQLDatabase):
    """A connection to a DataBricks Warehouse"""
    def __init__(self, name: str, location: InfrastructureLocation, address: str, http_path: str, catalogName: str, schemaName: str):
        super().__init__(name, location, address, catalogName)
        self.httpPath: str = http_path
        self.catalogName: str = catalogName
        self.schemaName: str = schemaName

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, DataBricksWarehouse) and self.httpPath == __value.httpPath and \
            self.catalogName == __value.catalogName and self.schemaName == __value.schemaName

    def __hash__(self) -> int:
        return hash(self.name)
