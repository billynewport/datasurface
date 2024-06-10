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
