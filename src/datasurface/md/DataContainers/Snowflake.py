"""
Copyright (C) 2024 William Newport

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

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
