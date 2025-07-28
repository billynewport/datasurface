"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from typing import Any
from datasurface.md.lint import ValidationTree, ProblemSeverity, ValidationProblem, UserDSLObject


class GenericKey(UserDSLObject):
    """Base class for all keys"""
    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return "GenericKey()"


class EcosystemKey(GenericKey):
    """Soft link to an ecosystem"""
    def __init__(self, ecoName: str) -> None:
        self.ecoName: str = ecoName

    def __eq__(self, other: object) -> bool:
        return isinstance(other, EcosystemKey) and self.ecoName == other.ecoName

    def __str__(self) -> str:
        return f"Ecosystem({self.ecoName})"

    def __hash__(self) -> int:
        return hash(str(self))

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "ecoName": self.ecoName})
        return rc


class GovernanceZoneKey(EcosystemKey):
    """Soft link to a governance zone"""
    def __init__(self, e: EcosystemKey, gz: str) -> None:
        super().__init__(e.ecoName)
        self.gzName: str = gz

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, GovernanceZoneKey) and self.gzName == other.gzName

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return super().__str__() + f".GovernanceZone({self.gzName})"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "gzName": self.gzName})
        return rc


class StoragePolicyKey(GovernanceZoneKey):
    """Soft link to a storage policy"""
    def __init__(self, gz: GovernanceZoneKey, policyName: str):
        super().__init__(gz, gz.gzName)
        self.policyName: str = policyName

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, StoragePolicyKey) and self.policyName == other.policyName

    def __str__(self) -> str:
        return super().__str__() + f".StoragePolicy({self.policyName})"

    def __hash__(self) -> int:
        return hash(str(self))

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "policyName": self.policyName})
        return rc


class InfrastructureVendorKey(EcosystemKey):
    """Soft link to an infrastructure vendor"""
    def __init__(self, eco: EcosystemKey, iv: str) -> None:
        super().__init__(eco.ecoName)
        self.ivName: str = iv

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, InfrastructureVendorKey) and self.ivName == other.ivName

    def __str__(self) -> str:
        return super().__str__() + f".InfrastructureVendor({self.ivName})"

    def __hash__(self) -> int:
        return hash(str(self))

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "vendorName": self.ivName})
        return rc


class DataPlatformKey(GenericKey):
    """This is a named reference to a DataPlatform. This allows a DataPlatform to be specified and
    resolved later at lint time."""
    def __init__(self, name: str) -> None:
        GenericKey.__init__(self)
        self.name: str = name

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DataPlatformKey) and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)


class InfraLocationKey(InfrastructureVendorKey):
    """Soft link to an infrastructure location"""
    def __init__(self, iv: InfrastructureVendorKey, loc: list[str]) -> None:
        super().__init__(iv, iv.ivName)
        self.locationPath: list[str] = loc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, InfraLocationKey) and self.locationPath == other.locationPath

    def __str__(self) -> str:
        return super().__str__() + f".InfraLocation({self.locationPath})"

    def __hash__(self) -> int:
        return hash(str(self))

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "locationPath": self.locationPath})
        return rc


class TeamDeclarationKey(GovernanceZoneKey):
    """Soft link to a team declaration"""
    def __init__(self, gz: GovernanceZoneKey, td: str) -> None:
        super().__init__(gz, gz.gzName)
        self.tdName: str = td

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, TeamDeclarationKey) and self.tdName == other.tdName

    def __str__(self) -> str:
        return super().__str__() + f".TeamDeclaration({self.tdName})"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "tdName": self.tdName})
        return rc


class WorkspaceKey(TeamDeclarationKey):
    def __init__(self, tdKey: TeamDeclarationKey, name: str) -> None:
        super().__init__(tdKey, tdKey.tdName)
        self.name: str = name

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, WorkspaceKey) and \
            self.name == other.name

    def __str__(self) -> str:
        return super().__str__() + f".WorkspaceKey({self.name})"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc


class DatastoreKey(TeamDeclarationKey):
    """Soft link to a datastore"""
    def __init__(self, td: TeamDeclarationKey, ds: str) -> None:
        super().__init__(td, td.tdName)
        self.dsName: str = ds

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DatastoreKey) and self.dsName == other.dsName

    def __str__(self) -> str:
        return super().__str__() + f".Datastore({self.dsName})"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "storeName": self.dsName})
        return rc


class InvalidLocationStringProblem(ValidationProblem):
    def __init__(self, problem: str, locStr: str, severity: ProblemSeverity) -> None:
        super().__init__(f"{problem}: {locStr}", severity)

    def __hash__(self) -> int:
        return hash(self.description)


class LocationKey(GenericKey):
    """This is used to reference a location on a vendor during DSL construction. This string has format vendor:loc1/loc2/loc3/..."""
    def __init__(self, locStr: str) -> None:
        GenericKey.__init__(self)
        self.locStr: str = locStr

    def _hash(self) -> int:
        return hash(self.locStr)

    def parseToVendorAndLocations(self) -> tuple[str, list[str]]:
        locList: list[str] = self.locStr.split(":")
        if (len(locList) != 2):
            raise Exception(f"Invalid location string {self.locStr}")
        vendor = locList[0]
        locationParts = locList[1].split("/")
        return vendor, locationParts

    def lint(self, tree: ValidationTree) -> None:
        # First check syntax is correct
        locList: list[str] = self.locStr.split(":")
        if (len(locList) != 2):
            tree.addRaw(InvalidLocationStringProblem("Format must be vendor:loc/loc/loc", self.locStr, ProblemSeverity.ERROR))
            return
        vendor = locList[0]
        if len(vendor) == 0:
            tree.addRaw(InvalidLocationStringProblem("Vendor cannot be empty", self.locStr, ProblemSeverity.ERROR))
        locationParts: list[str] = locList[1].split("/")
        if (len(locationParts) == 0):
            tree.addRaw(InvalidLocationStringProblem("One location must be specified", self.locStr, ProblemSeverity.ERROR))
            return
        if (len(locationParts[0]) == 0):
            tree.addRaw(InvalidLocationStringProblem("First location should not start with '/'", self.locStr, ProblemSeverity.ERROR))
            return
        for loc in locationParts:
            if (len(loc) == 0):
                tree.addRaw(InvalidLocationStringProblem("Empty locations not allowed", self.locStr, ProblemSeverity.ERROR))
                return

    def __str__(self) -> str:
        return f"LocationKey({self.locStr})"

    def __hash__(self) -> int:
        return hash(self.locStr)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "locStr": self.locStr})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, LocationKey)):
            return self.locStr == other.locStr
        return False
