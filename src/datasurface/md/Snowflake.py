from typing import Optional
from datasurface.md.Governance import InfrastructureLocation, URLSQLDatabase


class SnowFlakeDatabase(URLSQLDatabase):
    """References a Snowflake database"""
    def __init__(self, name: str, location: InfrastructureLocation, url: str, databaseName: str, account: str, warehouse: Optional[str] = None):
        super().__init__(name, location, url, databaseName)
        self.account: str = account
        self.warehouse: Optional[str] = warehouse

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, SnowFlakeDatabase) and self.account == __value.account and self.warehouse == __value.warehouse
