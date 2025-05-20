"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from abc import ABC


class GenericKey(ABC):
    """Base class for all keys"""
    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return "GenericKey()"


class EcosystemKey(GenericKey):
    """Soft link to an ecosystem"""
    def __init__(self, ecoName: str) -> None:
        self.ecoName: str = ecoName

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, EcosystemKey) and self.ecoName == __value.ecoName

    def __str__(self) -> str:
        return f"Ecosystem({self.ecoName})"

    def __hash__(self) -> int:
        return hash(str(self))


class GovernanceZoneKey(EcosystemKey):
    """Soft link to a governance zone"""
    def __init__(self, e: EcosystemKey, gz: str) -> None:
        super().__init__(e.ecoName)
        self.gzName: str = gz

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, GovernanceZoneKey) and self.gzName == __value.gzName

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return super().__str__() + f".GovernanceZone({self.gzName})"


class StoragePolicyKey(GovernanceZoneKey):
    """Soft link to a storage policy"""
    def __init__(self, gz: GovernanceZoneKey, policyName: str):
        super().__init__(gz, gz.gzName)
        self.policyName: str = policyName

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, StoragePolicyKey) and self.policyName == __value.policyName

    def __str__(self) -> str:
        return super().__str__() + f".StoragePolicy({self.policyName})"

    def __hash__(self) -> int:
        return hash(str(self))


class InfrastructureVendorKey(EcosystemKey):
    """Soft link to an infrastructure vendor"""
    def __init__(self, eco: EcosystemKey, iv: str) -> None:
        super().__init__(eco.ecoName)
        self.ivName: str = iv

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, InfrastructureVendorKey) and self.ivName == __value.ivName

    def __str__(self) -> str:
        return super().__str__() + f".InfrastructureVendor({self.ivName})"

    def __hash__(self) -> int:
        return hash(str(self))


class InfraLocationKey(InfrastructureVendorKey):
    """Soft link to an infrastructure location"""
    def __init__(self, iv: InfrastructureVendorKey, loc: list[str]) -> None:
        super().__init__(iv, iv.ivName)
        self.locationPath: list[str] = loc

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, InfraLocationKey) and self.locationPath == __value.locationPath

    def __str__(self) -> str:
        return super().__str__() + f".InfraLocation({self.locationPath})"

    def __hash__(self) -> int:
        return hash(str(self))


class TeamDeclarationKey(GovernanceZoneKey):
    """Soft link to a team declaration"""
    def __init__(self, gz: GovernanceZoneKey, td: str) -> None:
        super().__init__(gz, gz.gzName)
        self.tdName: str = td

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TeamDeclarationKey) and self.tdName == __value.tdName

    def __str__(self) -> str:
        return super().__str__() + f".TeamDeclaration({self.tdName})"


class WorkspaceKey(TeamDeclarationKey):
    def __init__(self, tdKey: TeamDeclarationKey, name: str) -> None:
        super().__init__(tdKey, tdKey.tdName)
        self.name: str = name

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, WorkspaceKey) and \
            self.name == __value.name

    def __str__(self) -> str:
        return super().__str__() + f".WorkspaceKey({self.name})"


class DatastoreKey(TeamDeclarationKey):
    """Soft link to a datastore"""
    def __init__(self, td: TeamDeclarationKey, ds: str) -> None:
        super().__init__(td, td.tdName)
        self.dsName: str = ds

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, DatastoreKey) and self.dsName == __value.dsName

    def __str__(self) -> str:
        return super().__str__() + f".Datastore({self.dsName})"
