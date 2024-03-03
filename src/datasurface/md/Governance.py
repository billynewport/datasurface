from dataclasses import dataclass
from collections import OrderedDict
from typing import Any, Callable, Optional, Sequence, TypeVar, Union, cast
from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from typing import Generic

from datasurface.md.GitOps import GitControlledObject, Repository
from datasurface.md.Policy import AllowDisallowPolicy, Policy

from .Documentation import Documentable, Documentation

from .utils import ANSI_SQL_NamedObject, is_valid_hostname_or_ip, is_valid_sql_identifier, validate_cron_string
from .Schema import DataClassification, DataClassificationPolicy, Schema
from .Exceptions import AttributeAlreadySetException, ObjectAlreadyExistsException, ObjectDoesntExistException
from .Exceptions import UnknownArgumentException, DatastoreDoesntExistException, WorkspaceDoesntExistException
from .Lint import AttributeNotSet, ConstraintViolation, DataTransformerMissing, DuplicateObject, NameMustBeSQLIdentifier, \
        ObjectIsDeprecated, ObjectMissing, ObjectNotCompatibleWithPolicy, ObjectWrongType, ProductionDatastoreMustHaveClassifications, \
        UnauthorizedAttributeChange, ProblemSeverity, UnknownObjectReference, ValidationTree


class ProductionStatus(Enum):
    """This indicates whether the team is in production or not"""
    PRODUCTION = 0
    NOT_PRODUCTION = 1


class DeprecationStatus(Enum):
    """This indicates whether the team is deprecated or not"""
    NOT_DEPRECATED = 0
    DEPRECATED = 1


class DeprecationInfo(Documentable):
    """This is the deprecation information for an object"""
    def __init__(self, status: DeprecationStatus, reason: Optional[Documentation] = None) -> None:
        super().__init__(reason)
        self.status: DeprecationStatus = status
        """If it deprecated or not"""
        """If deprecated then this explains why and what an existing user should do, alternative dataset for example"""

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and \
            isinstance(__value, DeprecationInfo) and self.status == __value.status


def cyclic_safe_eq(a: object, b: object, visited: set[object]) -> bool:
    """This is a recursive equality checker which avoids infinite recursion by tracking visited objects. The \
        meta data objects have circular references which cause infinite recursion when using the default"""
    ida: int = id(a)
    idb: int = id(b)

    if (ida == idb):
        return True

    if (type(b) is not type(a)):
        return False

    if (idb > ida):
        ida, idb = idb, ida

    pair = (ida, idb)
    if (pair in visited):
        return True

    visited.add(pair)

    # Handle comparing dict objects
    if isinstance(a, dict) and isinstance(b, dict):
        d_a: dict[Any, Any] = a
        d_b: dict[Any, Any] = b

        if len(d_a) != len(d_b):
            return False
        for key in d_a:
            if key not in b or not cyclic_safe_eq(d_a[key], d_b[key], visited):
                return False
        return True

    # Handle comparing list objects
    if isinstance(a, list) and isinstance(b, list):
        l_a: list[Any] = a
        l_b: list[Any] = b

        if len(l_a) != len(l_b):
            return False
        for item_a, item_b in zip(l_a, l_b):
            if not cyclic_safe_eq(item_a, item_b, visited):
                return False
        return True

    # Now compare objects for equality
    try:
        self_vars: dict[str, Any] = vars(a)
    except TypeError:
        # This is a primitive type
        return a == b

    # Check same named attributes for equality
    for attr, value in vars(b).items():
        if (not attr.startswith("_")):
            if not cyclic_safe_eq(self_vars[attr], value, visited):
                return False

    return True


class GenericKey(ABC):

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


class PolicyMandatedRule(Enum):
    MANDATED_WITHIN_ZONE = 0
    """Policies with this are forcibly added to every dataset in the zone"""
    INDIVIDUALLY_MANDATED = 1
    """Policies with this are not added to datasets by default. They must be added individually to each dataset"""


class StoragePolicy(Policy['DataContainer']):
    '''This is the base class for storage policies. These are owned by a governance zone and are used to determine whether a container is
    compatible with the policy.'''

    def __init__(self, name: str, isMandatory: PolicyMandatedRule, doc: Optional[Documentation], deprecationStatus: DeprecationInfo) -> None:
        super().__init__(name, doc)
        self.mandatory: PolicyMandatedRule = isMandatory
        self.key: Optional[StoragePolicyKey] = None
        self.deprecationStatus: DeprecationInfo = deprecationStatus
        """If true then all data containers MUST comply with this policy regardless of whether a dataset specifies this policy or not"""

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, StoragePolicy) and self.name == __value.name and self.mandatory == __value.mandatory and \
            self.key == __value.key and self.deprecationStatus == __value.deprecationStatus

    def setGovernanceZone(self, gz: 'GovernanceZone') -> None:
        if gz.key is None:
            raise Exception("GovernanceZone key not set")
        self.key = StoragePolicyKey(gz.key, self.name)

    def isCompatible(self, obj: 'DataContainer') -> bool:
        '''This returns true if the container is compatible with the policy. This is used to determine whether data tagged with a policy can be
        stored in a specific container.'''
        return False


class StoragePolicyAllowAnyContainer(StoragePolicy):
    '''This is a storage policy that allows any container to be used.'''
    def __init__(self, name: str, isMandatory: PolicyMandatedRule, doc: Optional[Documentation] = None,
                 deprecationStatus: DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)) -> None:
        super().__init__(name, isMandatory, doc, deprecationStatus)

    def isCompatible(self, obj: 'DataContainer') -> bool:
        return True

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is StoragePolicyAllowAnyContainer and \
            self.name == __value.name and self.mandatory == __value.mandatory


class InfrastructureLocation(Documentable):
    """This is a location within a vendors physical location hierarchy. This object
    is only fully initialized after construction when either the setParentLocation or
    setVendor methods are called. This is because the vendor is required to set the parent"""

    def __init__(self, name: str, *args: Union[Documentation, 'InfrastructureLocation']) -> None:
        super().__init__(None)
        self.name: str = name
        self.key: Optional[InfraLocationKey] = None

        self.locations: dict[str, 'InfrastructureLocation'] = OrderedDict()
        """These are the 'child' locations under this location. A state location would have city children for example"""
        """This specifies the parent location of this location. State is parent on city and so on"""
        self.add(*args)

    def __str__(self) -> str:
        return f"InfrastructureLocation({self.name})"

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, tree: ValidationTree):
        """This checks if the vendor is valid for the specified ecosystem, governance zone and team"""
        if (self.key is None):
            tree.addRaw(AttributeNotSet("Location"))
        if (self.documentation):
            dTree: ValidationTree = tree.createChild(self.documentation)
            self.documentation.lint(dTree)

        for loc in self.locations.values():
            loc.lint(tree)

    def setParentLocation(self, parent: InfraLocationKey) -> None:
        locList: list[str] = list(parent.locationPath)
        locList.append(self.name)
        self.key = InfraLocationKey(parent, locList)
        self.add()

    def add(self, *args: Union[Documentation, 'InfrastructureLocation']) -> None:
        for loc in args:
            if (isinstance(loc, InfrastructureLocation)):
                self.addLocation(loc)
            else:
                self.documentation = loc
        if (self.key):
            for loc in self.locations.values():
                loc.setParentLocation(self.key)

    def addLocation(self, loc: 'InfrastructureLocation'):
        if self.locations.get(loc.name) is not None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc

    def __eq__(self, __value: object) -> bool:
        if super().__eq__(__value) and isinstance(__value, InfrastructureLocation):
            return self.name == __value.name and self.key == __value.key and self.locations == __value.locations
        return False

    def getEveryChildLocation(self) -> set['InfrastructureLocation']:
        """This returns every child location of this location"""
        rc: set[InfrastructureLocation] = set()
        for loc in self.locations.values():
            rc.add(loc)
            rc = rc.union(loc.getEveryChildLocation())
        return rc

    def getLocationOrThrow(self, locationName: str) -> 'InfrastructureLocation':
        """Returns the location with the specified name or throws an exception"""
        loc: Optional[InfrastructureLocation] = self.locations.get(locationName)
        if (loc):
            return loc
        else:
            raise Exception(f"Location {locationName} not found")

    def getLocation(self, locationName: str) -> Optional['InfrastructureLocation']:
        """Returns the location with the specified name or None"""
        return self.locations.get(locationName)

    def findLocationUsingKey(self, locationPath: list[str]) -> Optional['InfrastructureLocation']:
        """Returns the location using the path"""
        if (len(locationPath) == 0):
            return None
        else:
            locName: str = locationPath[0]
            loc: Optional[InfrastructureLocation] = self.locations.get(locName)
            if (loc):
                if (len(locationPath) == 1):
                    return loc
                else:
                    return loc.findLocationUsingKey(locationPath[1:])
            else:
                return None


class CloudVendor(Enum):
    """Cloud vendor. This is used with InfrastructureVendor types to associate them with a hard cloud vendor"""
    AWS = 0
    """Amazon Web Services"""
    AZURE = 1
    """Microsoft Azure"""
    GCP = 2
    """Google Cloud Platform"""
    IBM = 3
    """IBM Cloud"""
    ORACLE = 4
    """Oracle Cloud"""
    ALIBABA = 5
    """Alibaba Cloud"""
    AWS_CHINA = 6
    """AWS China"""
    TEN_CENT = 7
    HUAWEI = 8
    AZURE_CHINA = 9  # 21Vianet


class InfrastructureVendor(Documentable):
    """This is a vendor which supplies infrastructure for storage and compute. It could be an internal supplier within an
    enterprise or an external cloud provider"""
    def __init__(self, name: str, *args: Union[InfrastructureLocation, Documentation, CloudVendor]) -> None:
        super().__init__(None)
        self.name: str = name
        self.key: Optional[InfrastructureVendorKey] = None
        self.locations: dict[str, 'InfrastructureLocation'] = OrderedDict()
        self.hardCloudVendor: Optional[CloudVendor] = None

        self.add(*args)

    def __hash__(self) -> int:
        return hash(self.name)

    def setEcosystem(self, eco: 'Ecosystem') -> None:
        self.key = InfrastructureVendorKey(eco.key, self.name)

        self.add()

    def add(self, *args: Union['InfrastructureLocation', Documentation, CloudVendor]) -> None:
        for loc in args:
            if (isinstance(loc, InfrastructureLocation)):
                self.addLocation(loc)
            elif (isinstance(loc, CloudVendor)):
                self.hardCloudVendor = loc
            else:
                self.documentation = loc
        if (self.key):
            topLocationKey: InfraLocationKey = InfraLocationKey(self.key, [])
            for loc in self.locations.values():
                loc.setParentLocation(topLocationKey)

    def addLocation(self, loc: 'InfrastructureLocation'):
        if self.locations.get(loc.name) is not None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc

    def __eq__(self, __value: object) -> bool:
        if super().__eq__(__value) and isinstance(__value, InfrastructureVendor):
            return self.name == __value.name and self.key == __value.key and self.locations == __value.locations and \
                self.hardCloudVendor == __value.hardCloudVendor
        else:
            return False

    def getLocationOrThrow(self, locationName: str) -> 'InfrastructureLocation':
        """Returns the location with the specified name or throws an exception"""
        loc: Optional[InfrastructureLocation] = self.locations.get(locationName)
        if (loc):
            return loc
        else:
            raise Exception(f"Location {locationName} not found")

    def getLocation(self, locationName: str) -> Optional['InfrastructureLocation']:
        """Returns the location with the specified name or None"""
        return self.locations.get(locationName)

    def findLocationUsingKey(self, locationPath: list[str]) -> Optional[InfrastructureLocation]:
        """Returns the location using the path"""
        if (len(locationPath) == 0):
            return None
        else:
            locName: str = locationPath[0]
            loc: Optional[InfrastructureLocation] = self.locations.get(locName)
            if (loc):
                if (len(locationPath) == 1):
                    return loc
                else:
                    return loc.findLocationUsingKey(locationPath[1:])
            else:
                return None

    def lint(self, tree: ValidationTree):
        """This checks if the vendor is valid for the specified ecosystem, governance zone and team"""
        if (self.key is None):
            tree.addRaw(AttributeNotSet("Vendor"))
        if (self.documentation is None):
            tree.addRaw(AttributeNotSet("Documentation"))
        else:
            self.documentation.lint(tree)

        for loc in self.locations.values():
            lTree: ValidationTree = tree.createChild(loc)
            loc.lint(lTree)

    def __str__(self) -> str:
        return f"InfrastructureVendor({self.name}, {self.hardCloudVendor})"


class InfraStructureVendorPolicy(AllowDisallowPolicy[InfrastructureVendor]):
    """Allows a GZ to police which vendors can be used with datastore or workspaces within itself"""
    def __init__(self, name: str, doc: Documentation, allowed: Optional[set[InfrastructureVendor]] = None,
                 notAllowed: Optional[set[InfrastructureVendor]] = None):
        super().__init__(name, doc, allowed, notAllowed)

    def __str__(self):
        return f"InfraStructureVendorPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        return super().__eq__(v) and isinstance(v, InfraStructureVendorPolicy) and self.allowed == v.allowed and self.notAllowed == v.notAllowed

    def __hash__(self) -> int:
        return super().__hash__()


class InfraHardVendorPolicy(AllowDisallowPolicy[CloudVendor]):
    """Allows a GZ to police which vendors can be used with datastore or workspaces within itself"""
    def __init__(self, name: str, doc: Documentation, allowed: Optional[set[CloudVendor]] = None,
                 notAllowed: Optional[set[CloudVendor]] = None):
        super().__init__(name, doc, allowed, notAllowed)

    def __str__(self):
        return f"InfraStructureVendorPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        return super().__eq__(v) and isinstance(v, InfraStructureVendorPolicy) and self.allowed == v.allowed and self.notAllowed == v.notAllowed

    def __hash__(self) -> int:
        return super().__hash__()


class InfraStructureLocationPolicy(AllowDisallowPolicy[InfrastructureLocation]):
    """Allows a GZ to police which locations can be used with datastores or workspaces within itself"""
    def __init__(self, name: str, doc: Documentation, allowed: Optional[set[InfrastructureLocation]] = None,
                 notAllowed: Optional[set[InfrastructureLocation]] = None):
        super().__init__(name, doc, allowed, notAllowed)

    def __str__(self):
        return f"InfrastructureLocationPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        rc: bool = super().__eq__(v)
        rc = rc and isinstance(v, InfraStructureLocationPolicy)
        other: InfraStructureLocationPolicy = cast(InfraStructureLocationPolicy, v)
        rc = rc and self.allowed == other.allowed
        rc = rc and self.notAllowed == other.notAllowed
        rc = rc and self.name == other.name
        return rc

    def __hash__(self) -> int:
        return super().__hash__()


class DataPlatformPolicy(AllowDisallowPolicy['DataPlatform']):
    def __init__(self, name: str, doc: Optional[Documentation], allowed: Optional[set['DataPlatform']] = None,
                 notAllowed: Optional[set['DataPlatform']] = None):
        super().__init__(name, doc, allowed, notAllowed)

    def __str__(self):
        return f"DataPlatformPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        return super().__eq__(v) and isinstance(v, DataPlatformPolicy) and self.allowed == v.allowed and \
            self.notAllowed == v.notAllowed and self.name == v.name

    def __hash__(self) -> int:
        return super().__hash__()


class EncryptionSystem:
    """This describes"""
    def __init__(self) -> None:
        self.name: Optional[str] = None
        self.keyContainer: Optional['DataContainer'] = None
        """Are keys stored on site or at a third party?"""
        self.hasThirdPartySuperUser: bool = False

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())


class DataContainer(ABC, Documentable):
    """This is a container for data. It's a logical container. The data can be physically stored in
    one or more locations through replication or fault tolerance measures. It is owned by a data platform
    and is used to determine whether a dataset is compatible with the container by a governancezone."""
    def __init__(self, name: str, *args: Union[InfrastructureLocation, Documentation]) -> None:
        ABC.__init__(self)
        Documentable.__init__(self, None)
        self.locations: set[InfrastructureLocation] = set()
        self.name: str = name
        self.serverSideEncryptionKeys: Optional[EncryptionSystem] = None
        """This is the vendor ecnryption system providing the container. For example, if a cloud vendor
        hosts the container, do they have access to the container data?"""
        self.clientSideEncryptionKeys: Optional[EncryptionSystem] = None
        """This is the encryption system used by the client to encrypt data before sending to the container. This could be used
        to encrypt data before sending to a cloud vendor for example"""
        self.isReadOnly: bool = False
        self.add(*args)

    def add(self, *args: Union[InfrastructureLocation, Documentation]) -> None:
        for arg in args:
            if (isinstance(arg, InfrastructureLocation)):
                if (arg in self.locations):
                    raise Exception(f"Duplicate Location {arg}")
                self.locations.add(arg)
            else:
                self.documentation = arg

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, DataContainer):
            return self.name == __value.name and self.locations == __value.locations and \
                self.serverSideEncryptionKeys == __value.serverSideEncryptionKeys and \
                self.clientSideEncryptionKeys == __value.clientSideEncryptionKeys and \
                self.isReadOnly == __value.isReadOnly
        else:
            return False

    def getName(self) -> str:
        """Returns the name of the container"""
        return self.name

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    @abstractmethod
    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        # Verify that the location on the data container doesn't violate
        # the governance zone policies for vendor or location
        for loc in self.locations:
            gz.checkLocationIsAllowed(eco, loc, tree)
        if (self.documentation):
            dTree: ValidationTree = tree.createChild(self.documentation)
            self.documentation.lint(dTree)

    def __hash__(self) -> int:
        return hash(self.name)

    def isUsingVendorsOnly(self, eco: 'Ecosystem', vendors: set[CloudVendor]) -> bool:
        """Returns true if the container only uses locations managed by the provided set of cloud vendors"""
        for loc in self.locations:
            if (loc.key is None):
                return False
            v: InfrastructureVendor = eco.getVendorOrThrow(loc.key.ivName)
            if v.hardCloudVendor != CloudVendor.AWS:
                return False
        return True


class SQLDatabase(DataContainer):
    """A generic SQL Database data container"""
    def __init__(self, name: str, location: InfrastructureLocation, databaseName: str) -> None:
        super().__init__(name, location)
        self.databaseName: str = databaseName

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, SQLDatabase)):
            return super().__eq__(__value) and self.databaseName == __value.databaseName
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, tree)


class URLSQLDatabase(SQLDatabase):
    """This is a SQL database with a URL"""
    def __init__(self, name: str, location: InfrastructureLocation, url: str, databaseName: str) -> None:
        super().__init__(name, location, databaseName)
        self.url: str = url

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, URLSQLDatabase)):
            return super().__eq__(__value) and self.url == __value.url
        return False


class HostPortSQLDatabase(SQLDatabase):
    """This is a SQL database with a host and port"""
    def __init__(self, name: str, location: InfrastructureLocation, host: str, port: int, databaseName: str) -> None:
        super().__init__(name, location, databaseName)
        self.host: str = host
        self.port: int = port

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, HostPortSQLDatabase)):
            return super().__eq__(__value) and self.host == __value.host and self.port == __value.port
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, tree)
        if not is_valid_hostname_or_ip(self.host):
            tree.addProblem(f"Host '{self.host}' is not a valid hostname or IP address")
        if self.port < 0 or self.port > 65535:
            tree.addProblem(f"Port {self.port} is not a valid port number")


class ObjectStorage(DataContainer):
    """Generic Object storage service. Flat file storage"""
    def __init__(self, name: str, loc: InfrastructureLocation, endPointURI: Optional[str], bucketName: str, prefix: Optional[str]):
        super().__init__(name, loc)
        self.endPointURI: Optional[str] = endPointURI
        self.bucketName: str = bucketName
        self.prefix: Optional[str] = prefix


class Dataset(ANSI_SQL_NamedObject, Documentable):
    """This is a single collection of homogeneous records with a primary key"""
    def __init__(self, name: str, *args: Union[Schema, StoragePolicy, Documentation, DeprecationInfo, DataClassification]) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        self.originalSchema: Optional[Schema] = None
        # Explicit policies, note these need to be added to mandatory policies for the owning GZ
        self.policies: dict[str, StoragePolicy] = OrderedDict()
        self.dataClassificationOverride: Optional[list[DataClassification]] = None
        """This is the classification of the data in the dataset. The overrides any classifications on the schema"""
        self.deprecationStatus: DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)
        self.add(*args)

    def add(self, *args: Union[Schema, StoragePolicy, Documentation, DeprecationInfo, DataClassification]) -> None:
        for arg in args:
            if (isinstance(arg, Schema)):
                s: Schema = arg
                self.originalSchema = s
            elif (isinstance(arg, StoragePolicy)):
                p: StoragePolicy = arg
                if self.policies.get(p.name) is not None:
                    raise Exception(f"Duplicate policy {p.name}")
                self.policies[p.name] = p
            elif (isinstance(arg, DeprecationInfo)):
                self.deprecationStatus = arg
            elif (isinstance(arg, DataClassification)):
                if (self.dataClassificationOverride is None):
                    self.dataClassificationOverride = list()
                self.dataClassificationOverride.append(arg)
            else:
                d: Documentation = arg
                self.documentation = d

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, Dataset):
            return ANSI_SQL_NamedObject.__eq__(self, __value) and Documentable.__eq__(self, __value) and \
                self.name == __value.name and self.originalSchema == __value.originalSchema and \
                self.policies == __value.policies and \
                self.deprecationStatus == __value.deprecationStatus and self.dataClassificationOverride == __value.dataClassificationOverride
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', store: 'Datastore', tree: ValidationTree) -> None:
        """Place holder to validate constraints on the dataset"""
        self.nameLint(tree)
        if (self.dataClassificationOverride is not None):
            if (self.originalSchema and self.originalSchema.hasDataClassifications()):
                tree.addProblem("There are data classifications within the schema")
        else:
            if (self.originalSchema and not self.originalSchema.hasDataClassifications()):
                tree.addProblem("There are no data classifications for the dataset", ProblemSeverity.WARNING)
        for policy in self.policies.values():
            if (policy.key is None):
                tree.addRaw(AttributeNotSet(f"Storage policy {policy.name} is not associated with a governance zone"))
            else:
                if (policy.key.gzName != gz.name):
                    tree.addProblem("Datasets must be governed by storage policies from its managing zone")
                if (policy.deprecationStatus.status == DeprecationStatus.DEPRECATED):
                    if (store.isDatasetDeprecated(self)):
                        tree.addRaw(ObjectIsDeprecated(policy, ProblemSeverity.WARNING))
                    else:
                        tree.addRaw(ObjectIsDeprecated(policy, ProblemSeverity.ERROR))
        if (self.originalSchema):
            self.originalSchema.lint(tree)
        else:
            tree.addRaw(AttributeNotSet("originalSchema"))

    def checkClassificationsAreOnly(self, verifier: DataClassificationPolicy) -> bool:
        """This checks if the dataset only has the specified classifications"""

        # Dataset level classification overrides schema level classification
        if (self.dataClassificationOverride):
            for dc in self.dataClassificationOverride:
                if not verifier.isCompatible(dc):
                    return False
            return True
        else:
            if self.originalSchema:
                # check schema attribute classifications are good
                return self.originalSchema.checkClassificationsAreOnly(verifier)
            else:
                return True

    def isBackwardsCompatibleWith(self, other: object, vTree: ValidationTree) -> bool:
        """This checks if the dataset is backwards compatible with the other dataset. This means that the other dataset
        can be used in place of this dataset. This is used to check if a dataset can be replaced by another dataset
        when a new version is released"""
        if (not isinstance(other, Dataset)):
            vTree.addRaw(ObjectWrongType(other, Dataset, ProblemSeverity.ERROR))
            return False
        super().isBackwardsCompatibleWith(other, vTree)
        if (self.originalSchema is None):
            vTree.addRaw(AttributeNotSet(f"Original schema not set for {self.name}"))
        elif (other.originalSchema is None):
            vTree.addRaw(AttributeNotSet(f"Original schema not set for {other.name}"))
        else:
            self.originalSchema.isBackwardsCompatibleWith(other.originalSchema, vTree)
        return not vTree.hasErrors()

    def __str__(self) -> str:
        return f"Dataset({self.name})"

    def hasClassifications(self) -> bool:
        """This returns true if the dataset has classifications for everything"""
        if (self.dataClassificationOverride):
            return True
        if (self.originalSchema and self.originalSchema.hasDataClassifications()):
            return True
        return False


class Credential(ABC):
    """These allow a client to connect to a service/server"""
    def __init__(self) -> None:
        pass

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, Credential)):
            return True
        else:
            return False

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        pass


class FileSecretCredential(Credential):
    """This allows a secret to be read from the local filesystem. Usually the secret is
    placed in the file using an external service such as Docker secrets etc. The secret should be in the
    form of 2 lines, first line is user name, second line is password"""
    def __init__(self, filePath: str) -> None:
        super().__init__()
        self.secretFilePath: str = filePath

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is FileSecretCredential and self.secretFilePath == __value.secretFilePath

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        # TODO This needs to be better
        if (self.secretFilePath == ""):
            tree.addProblem("Secret file path is empty")

    def __str__(self) -> str:
        return f"FileSecretCredential({self.secretFilePath})"


class UserPasswordCredential(Credential):
    """This is a simple user name and password credential"""
    def __init__(self, username: str, password: str) -> None:
        super().__init__()
        self.username: str = username
        self.password: str = password

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is UserPasswordCredential and self.username == __value.username and self.password == __value.password

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if (self.username == ""):
            tree.addProblem("Username is empty")
        if (self.password == ""):
            tree.addProblem("Password is empty")

    def __str__(self) -> str:
        return f"UserPasswordCredential({self.username})"


class ClearTextCredential(UserPasswordCredential):
    """This is implemented for testing but should never be used in production. All
    credentials should be stored and retrieved using secrets Credential objects also
    provided."""
    def __init__(self, username: str, password: str) -> None:
        super().__init__(username, password)

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is ClearTextCredential

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        tree.addProblem("ClearText credential found", ProblemSeverity.WARNING)

    def __str__(self) -> str:
        return f"ClearTextCredential({self.username})"


class PyOdbcSourceInfo(DataContainer):
    """This describes how to connect to a database using pyodbc"""
    def __init__(self, name: str, loc: InfrastructureLocation, serverHost: str, databaseName: str, driver: str, connectionStringTemplate: str) -> None:
        if (loc.key is None):
            raise Exception("Location key not set")
        super().__init__(name, loc)
        self.serverHost: str = serverHost
        self.databaseName: str = databaseName
        self.driver: str = driver
        self.connectionStringTemplate: str = connectionStringTemplate

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is PyOdbcSourceInfo and self.serverHost == __value.serverHost and \
            self.databaseName == __value.databaseName and self.driver == __value.driver and self.connectionStringTemplate == __value.connectionStringTemplate

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        super().lint(eco, gz, t, tree)
# TODO validate the server string, its not just a host name
#        if (not is_valid_hostname_or_ip(self.serverHost)):
#            tree.addProblem(f"Server host {self.serverHost} is not a valid hostname or IP address")

    def __str__(self) -> str:
        return f"PyOdbcSourceInfo({self.serverHost})"


class CaptureType(Enum):
    SNAPSHOT = 0
    INCREMENTAL = 1


class IngestionConsistencyType(Enum):
    """This determines whether data is ingested in consistent groups across multiple datasets or
    whether each dataset is ingested independently"""
    SINGLE_DATASET = 0
    MULTI_DATASET = 1


class StepTrigger(ABC):
    """A step such as ingestion is driven in pulses triggered by these."""
    def __init__(self, name: str):
        super().__init__()
        self.name: str = name

    def __eq__(self, o: object) -> bool:
        return isinstance(o, StepTrigger) and self.name == o.name

    @abstractmethod
    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', tree: ValidationTree) -> None:
        pass


class CronTrigger(StepTrigger):
    """This allows the ingestion pulses to be specified using a cron string"""
    def __init__(self, name: str, cron: str):
        super().__init__(name)
        self.cron: str = cron

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, CronTrigger) and self.cron == o.cron

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if not validate_cron_string(self.cron):
            tree.addProblem(f"Invalid cron string <{self.cron}>")


class CaptureMetaData(ABC):
    """This describes how a platform can pull data for a Datastore"""

    def __init__(self, stepTrigger: Optional[StepTrigger] = None):
        super().__init__()
        self.singleOrMultiDatasetIngestion: Optional[IngestionConsistencyType] = None
        self.stepTrigger: Optional[StepTrigger] = None

    @abstractmethod
    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        if (self.singleOrMultiDatasetIngestion is None):
            tree.addRaw(AttributeNotSet("Single Or Multi ingestion not specified"))

        if (self.stepTrigger):
            self.stepTrigger.lint(eco, gz, t, tree)

    def __eq__(self, o: object) -> bool:
        return isinstance(o, CaptureMetaData)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class DataTransformerOutput(CaptureMetaData):
    """Specifies this datastore is ingested whenever a Datatransformer executes"""
    def __init__(self, workSpaceName: str) -> None:
        super().__init__()
        self.workSpaceName = workSpaceName
        self.singleOrMultiDatasetIngestion = IngestionConsistencyType.MULTI_DATASET

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, d, tree)
        w: Optional[Workspace] = t.workspaces.get(self.workSpaceName)

        if (w is None):
            tree.addRaw(UnknownObjectReference(f"workspace {self.workSpaceName}", ProblemSeverity.ERROR))
        else:
            if (w.dataTransformer is None):
                tree.addRaw(DataTransformerMissing(f"Workspace {self.workSpaceName} must have dataTransformer", ProblemSeverity.ERROR))
            else:
                if (w.dataTransformer.outputDatastore.name != d.name):
                    tree.addRaw(ConstraintViolation(
                        f"Specified Workspace {self.workSpaceName} output store name {w.dataTransformer.outputDatastore.name} "
                        f"doesnt match referring datastore {d.name}", ProblemSeverity.ERROR))

    def __str__(self):
        return f"DataTransformerOutput({self.workSpaceName})"

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, DataTransformerOutput) and self.workSpaceName == o.workSpaceName


class IngestionMetadata(CaptureMetaData):
    """Producers use these to describe HOW to snapshot and pull deltas from a data source in to
    data pipelines. The ingestion service interprets these to allow code free ingestion from
    supported sources and handle operation pipelines."""
    def __init__(self, dc: DataContainer, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__()
        self.credential: Optional[Credential] = None
        self.dataContainer: DataContainer = dc
        self.add(*args)

    def add(self, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        for arg in args:
            if (isinstance(arg, Credential)):
                c: Credential = arg
                if (self.credential is not None):
                    raise AttributeAlreadySetException("Credential already set")
                self.credential = c
            elif (isinstance(arg, StepTrigger)):
                if (self.stepTrigger is not None):
                    raise AttributeAlreadySetException("CaptureTrigger already set")
                self.stepTrigger = arg
            else:
                if (self.singleOrMultiDatasetIngestion is not None):
                    raise AttributeAlreadySetException("SingleOrMultiDatasetIngestion already set")
                sm: IngestionConsistencyType = arg
                self.singleOrMultiDatasetIngestion = sm

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, IngestionMetadata):
            return self.credential == __value.credential and self.singleOrMultiDatasetIngestion == __value.singleOrMultiDatasetIngestion and \
                self.dataContainer == __value.dataContainer and self.stepTrigger == __value.stepTrigger
        return False

    @abstractmethod
    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if (self.dataContainer):
            capTree: ValidationTree = tree.createChild(self.dataContainer)
            self.dataContainer.lint(eco, gz, t, capTree)
        # Credential is needed for a platform connect to a datacontainer and ingest data
        if (self.credential is None):
            tree.addRaw(AttributeNotSet("credential"))
        else:
            self.credential.lint(eco, tree)
        super().lint(eco, gz, t, d, tree)


class CDCCaptureIngestion(IngestionMetadata):
    """This indicates CDC can be used to capture deltas from the source"""
    def __init__(self, dc: DataContainer, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(dc, *args)

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, d, tree)

    def __str__(self) -> str:
        return "CDCCaptureIngestion()"

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is CDCCaptureIngestion


class SQLPullIngestion(IngestionMetadata):
    """This IMD describes how to pull a snapshot 'dump' from each dataset and then persist
    state variables which are used to next pull a delta per dataset and then persist the state
    again so that another delta can be pulled on the next pass and so on"""
    def __init__(self, dc: DataContainer, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(dc, *args)
        self.variableNames: list[str] = []
        """The names of state variables produced by snapshot and delta sql strings"""
        self.snapshotSQL: dict[str, str] = OrderedDict()
        """A SQL string per dataset which pulls a per table snapshot"""
        self.deltaSQL: dict[str, str] = OrderedDict()
        """A SQL string per dataset which pulls all rows which changed since last time for a table"""

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, SQLPullIngestion):
            return super().__eq__(__value) and self.variableNames == __value.variableNames and \
                self.snapshotSQL == __value.snapshotSQL and self.deltaSQL == __value.deltaSQL
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        raise NotImplementedError()

    def __str__(self) -> str:
        return "SQLPullIngestion()"


class Datastore(ANSI_SQL_NamedObject, Documentable):

    """This is a named group of datasets. It describes how to capture the data and make it available for processing"""
    def __init__(self, name: str, *args: Union[Dataset, CaptureMetaData, DataContainer, Documentation, ProductionStatus, DeprecationInfo]) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        self.datasets: dict[str, Dataset] = OrderedDict()
        self.key: Optional[DatastoreKey] = None
        self.cmd: Optional[CaptureMetaData] = None
        self.container: Optional[DataContainer] = None
        self.productionStatus: ProductionStatus = ProductionStatus.NOT_PRODUCTION
        self.deprecationStatus: DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)
        """Deprecating a store deprecates all datasets in the store regardless of their deprecation status"""
        self.add(*args)

    def setTeam(self, tdKey: TeamDeclarationKey):
        self.key = DatastoreKey(tdKey, self.name)

    def add(self, *args: Union[Dataset, CaptureMetaData, DataContainer, Documentation, ProductionStatus, DeprecationInfo]) -> None:
        for arg in args:
            if (type(arg) is Dataset):
                d: Dataset = arg
                if self.datasets.get(d.name) is not None:
                    raise ObjectAlreadyExistsException(f"Duplicate Dataset {d.name}")
                self.datasets[d.name] = d
            elif (isinstance(arg, CaptureMetaData)):
                i: CaptureMetaData = arg
                if (self.cmd):
                    raise AttributeAlreadySetException("CMD")
                self.cmd = i
            elif (isinstance(arg, DataContainer)):
                c: DataContainer = arg
                self.container = c
            elif (isinstance(arg, ProductionStatus)):
                self.productionStatus = arg
            elif (isinstance(arg, DeprecationInfo)):
                self.deprecationStatus = arg
            elif (isinstance(arg, Documentation)):
                doc: Documentation = arg
                self.documentation = doc

    def isDatasetDeprecated(self, dataset: Dataset) -> bool:
        """Returns true if the datastore is deprecated OR dataset is deprecated"""
        return self.deprecationStatus.status == DeprecationStatus.DEPRECATED or dataset.deprecationStatus.status == DeprecationStatus.DEPRECATED

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, Datastore):
            return ANSI_SQL_NamedObject.__eq__(self, __value) and Documentable.__eq__(self, __value) and \
                self.datasets == __value.datasets and self.cmd == __value.cmd and \
                self.container == __value.container and \
                self.productionStatus == __value.productionStatus and self.deprecationStatus == __value.deprecationStatus and \
                self.key == __value.key
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', storeTree: ValidationTree) -> None:
        self.nameLint(storeTree)
        if (self.key is None):
            storeTree.addRaw(AttributeNotSet(f"{self} has no key"))
        if (self.documentation):
            self.documentation.lint(storeTree)
        for dataset in self.datasets.values():
            dTree: ValidationTree = storeTree.createChild(dataset)
            dataset.lint(eco, gz, t, self, dTree)
            if (self.productionStatus == ProductionStatus.PRODUCTION):
                if (not dataset.hasClassifications()):
                    dTree.addRaw(ProductionDatastoreMustHaveClassifications(self, dataset))

        if (self.cmd):
            cmdTree: ValidationTree = storeTree.createChild(self.cmd)
            self.cmd.lint(eco, gz, t, self, cmdTree)
        else:
            storeTree.addRaw(AttributeNotSet("CaptureMetaData not set"))

    def isBackwardsCompatibleWith(self, other: object, vTree: ValidationTree) -> bool:
        """This checks if the other datastore is backwards compatible with this one. This means that the other datastore
        can be used to replace this one without breaking any data pipelines"""

        if (not isinstance(other, Datastore)):
            vTree.addRaw(ObjectWrongType(other, Datastore, ProblemSeverity.ERROR))
            return False
        super().isBackwardsCompatibleWith(other, vTree)
        # Check if the datasets are compatible
        for dataset in self.datasets.values():
            dTree: ValidationTree = vTree.createChild(dataset)
            otherDataset: Optional[Dataset] = other.datasets.get(dataset.name)
            if (otherDataset):
                dataset.isBackwardsCompatibleWith(otherDataset, dTree)
            else:
                dTree.addRaw(ObjectMissing(other, dataset, ProblemSeverity.ERROR))
        return not vTree.hasErrors()

    def __str__(self) -> str:
        return f"Datastore({self.name})"


class TeamCacheEntry:
    """This is used by Ecosystem to cache teams"""
    def __init__(self, t: 'Team', td: 'TeamDeclaration') -> None:
        self.team: Team = t
        self.declaration: TeamDeclaration = td


class WorkspaceCacheEntry:
    """This is used by Ecosystem to cache workspaces"""
    def __init__(self, w: 'Workspace', t: 'Team') -> None:
        self.workspace: Workspace = w
        self.team: Team = t


class DatastoreCacheEntry:
    """This is used by Ecosystem to cache datastores"""
    def __init__(self, d: 'Datastore', t: 'Team') -> None:
        self.datastore: Datastore = d
        self.team: Team = t


class DependentWorkspaces:
    """This tracks a Workspaces dependent on a datastore"""
    def __init__(self, workSpace: 'Workspace'):
        self.workspace: Workspace = workSpace
        self.dependencies: set[DependentWorkspaces] = set()

    def addDependency(self, dep: 'DependentWorkspaces') -> None:
        self.dependencies.add(dep)

    def flatten(self) -> set['Workspace']:
        """Returns a flattened list of dependencies"""
        rc: set[Workspace] = {self.workspace}
        for dep in self.dependencies:
            rc.update(dep.flatten())
        return rc

    def __str__(self) -> str:
        return f"Dependency({self.flatten()})"

    def __hash__(self) -> int:
        return hash(self.workspace.name)

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, DependentWorkspaces)):
            return self.workspace.name == __value.workspace.name
        else:
            return False


class DefaultDataPlatform:
    def __init__(self, p: 'DataPlatform'):
        self.defaultPlatform = p


# Add regulators here with their named retention policies for reference in Workspaces
# Feels like regulators are across GovernanceZones
class Ecosystem(GitControlledObject):

    def createGZone(self, name: str, repo: Repository) -> 'GovernanceZone':
        gz: GovernanceZone = GovernanceZone(name, repo)
        gz.setEcosystem(self)
        return gz

    def __init__(self, name: str, repo: Repository,
                 *args: Union['DataPlatform', Documentation, DefaultDataPlatform, InfrastructureVendor, 'GovernanceZoneDeclaration']) -> None:
        super().__init__(repo)
        self.name: str = name
        self.key: EcosystemKey = EcosystemKey(self.name)

        self.zones: AuthorizedObjectManager[GovernanceZone, GovernanceZoneDeclaration] = \
            AuthorizedObjectManager[GovernanceZone, GovernanceZoneDeclaration]("zones", lambda name, repo: self.createGZone(name, repo), repo)
        """This is the authorative list of governance zones within the ecosystem"""

        self.vendors: dict[str, InfrastructureVendor] = OrderedDict[str, InfrastructureVendor]()
        self.dataPlatforms: dict[str, DataPlatform] = OrderedDict[str, DataPlatform]()
        self.defaultDataPlatform: Optional[DataPlatform] = None
        self.resetCaches()
        self.add(*args)

    def resetCaches(self) -> None:
        """Empties the caches"""
        self.datastoreCache: dict[str, DatastoreCacheEntry] = {}
        """This is a cache of all data stores in the ecosystem"""
        self.workSpaceCache: dict[str, WorkspaceCacheEntry] = {}
        """This is a cache of all workspaces in the ecosystem"""
        self.teamCache: dict[str, TeamCacheEntry] = {}
        """This is a cache of all team declarations in the ecosystem"""

    def add(self, *args: Union['DataPlatform', DefaultDataPlatform, Documentation, InfrastructureVendor, 'GovernanceZoneDeclaration']) -> None:
        for arg in args:
            if isinstance(arg, InfrastructureVendor):
                if self.vendors.get(arg.name) is not None:
                    raise ObjectAlreadyExistsException(f"Duplicate Vendor {arg.name}")
                self.vendors[arg.name] = arg
            elif isinstance(arg, Documentation):
                self.documentation = arg
            elif isinstance(arg, DataPlatform):
                self.dataPlatforms[arg.name] = arg
            elif isinstance(arg, DefaultDataPlatform):
                if (self.defaultDataPlatform is not None):
                    raise AttributeAlreadySetException("Default DataPlatform already specified")
                else:
                    self.defaultDataPlatform = arg.defaultPlatform
                    self.dataPlatforms[self.defaultDataPlatform.name] = self.defaultDataPlatform
            else:
                self.zones.addAuthorization(arg)
                arg.key = GovernanceZoneKey(self.key, arg.name)

        for vendor in self.vendors.values():
            vendor.setEcosystem(self)

    def getDefaultDataPlatform(self) -> 'DataPlatform':
        """This returns the default DataPlatform or throws an Exception if it has not been specified"""
        if (self.defaultDataPlatform):
            return self.defaultDataPlatform
        else:
            raise Exception("No default data platform specified")

    def getVendor(self, name: str) -> Optional[InfrastructureVendor]:
        return self.vendors.get(name)

    def getVendorOrThrow(self, name: str) -> InfrastructureVendor:
        v: Optional[InfrastructureVendor] = self.getVendor(name)
        if (v):
            if (v.key is None):
                v.setEcosystem(self)
            return v
        else:
            raise ObjectDoesntExistException(f"Unknown vendor {name}")

    def getDataPlatform(self, name: str) -> Optional['DataPlatform']:
        return self.dataPlatforms.get(name)

    def getDataPlatformOrThrow(self, name: str) -> 'DataPlatform':
        p: Optional['DataPlatform'] = self.getDataPlatform(name)
        if (p):
            return p
        else:
            raise ObjectDoesntExistException(f"Unknown data platform {name}")

    def getLocation(self, vendorName: str, locKey: list[str]) -> Optional[InfrastructureLocation]:
        vendor: Optional[InfrastructureVendor] = self.getVendor(vendorName)
        loc: Optional[InfrastructureLocation] = None
        if vendor:
            loc: Optional[InfrastructureLocation] = vendor.findLocationUsingKey(locKey)
        return loc

    def getLocationOrThrow(self, vendorName: str, locKey: list[str]) -> InfrastructureLocation:
        vendor: InfrastructureVendor = self.getVendorOrThrow(vendorName)
        loc: Optional[InfrastructureLocation] = vendor.findLocationUsingKey(locKey)
        if (loc is None):
            raise ObjectDoesntExistException(f"Unknown location vendor: {vendor.name}/{locKey} ")
        return loc

    def getAllChildLocations(self, vendorName: str, locKey: list[str]) -> set[InfrastructureLocation]:
        """Returns all child locations. Typically used to return all locations within a country for example"""
        rc: set[InfrastructureLocation] = set(self.getLocationOrThrow(vendorName, locKey).locations.values())
        return rc

    def cache_addTeam(self, td: 'TeamDeclaration', t: 'Team'):
        if td.key is None:
            raise Exception("{td} key is None")
        globalTeamName: str = td.key.gzName + "/" + t.name
        if (self.teamCache.get(globalTeamName) is not None):
            raise ObjectAlreadyExistsException(f"Duplicate Team {globalTeamName}")
        self.teamCache[globalTeamName] = TeamCacheEntry(t, td)

    def cache_addWorkspace(self, team: 'Team', work: 'Workspace'):
        """This adds a workspace to the eco cache and flags duplicates"""
        if (self.workSpaceCache.get(work.name) is not None):
            raise ObjectAlreadyExistsException(f"Duplicate workspace {work.name}")
        self.workSpaceCache[work.name] = WorkspaceCacheEntry(work, team)

    def cache_addDatastore(self, store: 'Datastore', t: 'Team'):
        """This adds a store to the eco cache and flags duplicates"""
        if (self.datastoreCache.get(store.name) is not None):
            raise ObjectAlreadyExistsException(f"Duplicate data store {store.name}")
        self.datastoreCache[store.name] = DatastoreCacheEntry(store, t)

    def cache_getWorkspaceOrThrow(self, work: str) -> WorkspaceCacheEntry:
        """This returns the named workspace if it exists"""
        w: Optional[WorkspaceCacheEntry] = self.workSpaceCache.get(work)
        if (w):
            return w
        else:
            raise WorkspaceDoesntExistException(f"Unknown workspace {work}")

    def cache_getDatastoreOrThrow(self, store: str) -> DatastoreCacheEntry:
        s: Optional[DatastoreCacheEntry] = self.datastoreCache.get(store)
        if (s):
            return s
        else:
            raise DatastoreDoesntExistException(f"Unknown datastore {store}")

    def cache_getDataset(self, storeName: str, datasetName: str) -> Optional[Dataset]:
        """This returns the named dataset if it exists"""
        s: Optional[DatastoreCacheEntry] = self.datastoreCache.get(storeName)
        if (s):
            dataset = s.datastore.datasets.get(datasetName)
            return dataset
        return None

    def lintAndHydrateCaches(self) -> ValidationTree:
        """This validates the ecosystem and returns a list of problems which is empty if there are no issues"""
        self.resetCaches()

        # This will lint the ecosystem, zones, teams, datastores and datasets.
        ecoTree: ValidationTree = ValidationTree(self)

        # This will lint the ecosystem, zones, teams, datastores and datasets.
        # Workspaces are linted in a second pass later.
        # It populates the caches for zones, teams, stores and workspaces.
        """No need to dedup zones as the authorative list is already a dict"""
        for gz in self.zones.authorizedObjects.values():
            govTree: ValidationTree = ecoTree.createChild(gz)
            gz.lint(self, govTree)

        """All caches should now be populated"""

        for vendor in self.vendors.values():
            vTree: ValidationTree = ecoTree.createChild(vendor)
            vendor.lint(vTree)

        for pl in self.dataPlatforms.values():
            platTree: ValidationTree = ecoTree.createChild(pl)
            pl.lint(self, platTree)

        # Now lint the workspaces
        for workSpaceCacheEntry in self.workSpaceCache.values():
            workSpace = workSpaceCacheEntry.workspace
            wsTree: ValidationTree = ecoTree.createChild(workSpace)
            if (workSpace.key):
                gz: GovernanceZone = self.getZoneOrThrow(workSpace.key.gzName)
                workSpace.lint(self, gz, workSpaceCacheEntry.team, wsTree)
        self.superLint(ecoTree)
        self.zones.lint(ecoTree)
        if (self.documentation):
            self.documentation.lint(ecoTree)
        return ecoTree

    def calculateDependenciesForDatastore(self, storeName: str, wsVisitedSet: set[str] = set()) -> Sequence[DependentWorkspaces]:
        # TODO make tests
        rc: list[DependentWorkspaces] = []
        store: Datastore = self.datastoreCache[storeName].datastore

        # If the store is used in any Workspace then thats a dependency
        for w in self.workSpaceCache.values():
            # Do not enter a cyclic loop
            if (w.workspace.name not in wsVisitedSet):
                if (w.workspace.isDatastoreUsed(store)):
                    workspace: Workspace = w.workspace
                    dep: DependentWorkspaces = DependentWorkspaces(workspace)
                    rc.append(dep)
                    # prevent cyclic loops
                    wsVisitedSet.add(workspace.name)
                    # If the workspace has a data transformer then the output store's dependencies are also dependencies
                    if (workspace.dataTransformer is not None):
                        outputStore: Datastore = workspace.dataTransformer.outputDatastore
                        depList: Sequence[DependentWorkspaces] = self.calculateDependenciesForDatastore(outputStore.name, wsVisitedSet)
                        for dep2 in depList:
                            dep.addDependency(dep2)
        return rc

    def checkIfChangesAreAuthorized(self, proposed: GitControlledObject, changeSource: Repository, vTree: ValidationTree) -> None:
        """This checks if the ecosystem top level has changed relative to the specified change source"""
        """This checks if any Governance zones has been added or removed relative to e"""

        prop_eco: Ecosystem = cast(Ecosystem, proposed)

        self.checkTopLevelAttributeChangesAreAuthorized(prop_eco, changeSource, vTree)

        zTree: ValidationTree = vTree.createChild(self.zones)
        self.zones.checkIfChangesAreAuthorized(prop_eco.zones, changeSource, zTree)

    def __eq__(self, proposed: object) -> bool:
        if super().__eq__(proposed) and isinstance(proposed, Ecosystem):
            rc = self.name == proposed.name
            rc = rc and self.zones == proposed.zones
            rc = rc and self.key == proposed.key
            rc = rc and self.vendors == proposed.vendors
            rc = rc and self.dataPlatforms == proposed.dataPlatforms
            rc = rc and self.defaultDataPlatform == proposed.defaultDataPlatform
            return rc
        else:
            return False

    def areTopLevelChangesAuthorized(self, proposed: GitControlledObject, changeSource: Repository, tree: ValidationTree) -> bool:
        """This is a shallow equality check for the top level ecosystem object"""
        if (isinstance(proposed, Ecosystem)):
            rc: bool = True
            # If we are being modified by a potentially unauthorized source then check
            if (self.owningRepo != changeSource):
                rc = super().areTopLevelChangesAuthorized(proposed, changeSource, tree)
                if self.name != proposed.name:
                    tree.addRaw(UnauthorizedAttributeChange("name", self.name, proposed.name, ProblemSeverity.ERROR))
                    rc = False
                if self.owningRepo != proposed.owningRepo:
                    tree.addRaw(UnauthorizedAttributeChange("owningRepo", self.owningRepo, proposed.owningRepo, ProblemSeverity.ERROR))
                    rc = False
                zTree: ValidationTree = tree.createChild(self.zones)
                if not self.zones.areTopLevelChangesAuthorized(proposed.zones, changeSource, zTree):
                    rc = False
                if self.vendors != proposed.vendors:
                    vTree: ValidationTree = tree.createChild("Vendors")
                    self.showDictChangesAsProblems(self.vendors, proposed.vendors, vTree)
                    rc = False
                if self.dataPlatforms != proposed.dataPlatforms:
                    pTree: ValidationTree = tree.createChild("DataPlatforms")
                    self.showDictChangesAsProblems(self.dataPlatforms, proposed.dataPlatforms, pTree)
                    rc = False
                if self.defaultDataPlatform != proposed.defaultDataPlatform:
                    tree.addRaw(UnauthorizedAttributeChange("defaultDataPlatformn", self.defaultDataPlatform,
                                                            proposed.defaultDataPlatform, ProblemSeverity.ERROR))
                    rc = False
            return rc
        else:
            return False

    def getZone(self, gz: str) -> Optional['GovernanceZone']:
        """Returns the governance zone with the specified name"""
        zone: Optional[GovernanceZone] = self.zones.getObject(gz)
        return zone

    def getZoneOrThrow(self, gz: str) -> 'GovernanceZone':
        z: Optional[GovernanceZone] = self.getZone(gz)
        if (z):
            return z
        else:
            raise ObjectDoesntExistException(f"Unknown governance zone {gz}")

    def getTeam(self, gz: str, teamName: str) -> Optional['Team']:
        """Returns the team with the specified name in the specified zone"""
        zone: Optional[GovernanceZone] = self.getZone(gz)
        if (zone):
            t: Optional[Team] = zone.getTeam(teamName)
            return t
        else:
            return None

    def getTeamOrThrow(self, gz: str, teamName: str) -> 'Team':
        t: Optional[Team] = self.getTeam(gz, teamName)
        if (t):
            return t
        else:
            raise ObjectDoesntExistException(f"Unknown team {teamName} in governance zone {gz}")

    def __str__(self) -> str:
        return f"Ecosystem({self.name})"

    def checkIfChangesAreBackwardsCompatibleWith(self, originEco: 'Ecosystem', vTree: ValidationTree) -> None:
        """This checks if the proposed ecosystem is backwards compatible with the current ecosystem"""
        # Check if the zones are compatible
        for zone in self.zones.authorizedObjects.values():
            zTree: ValidationTree = vTree.createChild(zone)
            originZone: Optional[GovernanceZone] = originEco.getZone(zone.name)
            if originZone:
                zone.isBackwardsCompatibleWith(originZone, zTree)

    def checkIfChangesCanBeMerged(self, proposed: 'Ecosystem', source: Repository) -> ValidationTree:
        """This is called to check if the proposed changes can be merged in to the current ecosystem. It returns a ValidationTree with issues if not
        or an empty ValidationTree if allowed."""

        # First, the incoming ecosystem must be consistent and pass lint checks
        eTree: ValidationTree = proposed.lintAndHydrateCaches()

        # Any errors make us fail immediately
        # But we want warnings and infos to accumulate for the caller
        if eTree.hasErrors():
            return eTree

        # Check if the proposed changes being made by an authorized repository
        self.checkIfChangesAreAuthorized(proposed, source, eTree)
        if eTree.hasErrors():
            return eTree

        # Check if the proposed changes are backwards compatible this object
        proposed.checkIfChangesAreBackwardsCompatibleWith(self, eTree)
        return eTree


class Team(GitControlledObject):
    """This is the authoritive definition of a team within a goverance zone. All teams must have
    a corresponding TeamDeclaration in the owning GovernanceZone"""
    def __init__(self, name: str, repo: Repository, *args: Union[Datastore, 'Workspace', Documentation]) -> None:
        super().__init__(repo)
        self.name: str = name
        self.workspaces: dict[str, Workspace] = OrderedDict()
        self.dataStores: dict[str, Datastore] = OrderedDict()
        self.add(*args)

    def add(self, *args: Union[Datastore, 'Workspace', Documentation]) -> None:
        """Adds a workspace, datastore or gitrepository to the team"""
        for arg in args:
            if (isinstance(arg, Datastore)):
                s: Datastore = arg
                self.addStore(s)
            elif (isinstance(arg, Workspace)):
                w: Workspace = arg
                self.addWorkspace(w)
            else:
                d: Documentation = arg
                self.documentation = d

    def addStore(self, store: Datastore):
        """Adds a datastore to the team checking for duplicates"""
        if self.dataStores.get(store.name) is not None:
            raise ObjectAlreadyExistsException(f"Duplicate Datastore {store.name}")
        self.dataStores[store.name] = store

    def addWorkspace(self, w: 'Workspace'):
        if self.workspaces.get(w.name) is not None:
            raise ObjectAlreadyExistsException(f"Duplicate Workspace {w.name}")
        self.workspaces[w.name] = w
        if (w.dataTransformer):
            oStore: Datastore = w.dataTransformer.outputDatastore
            if oStore.cmd:
                raise AttributeAlreadySetException("Transformer {w.dataTransformer} Datastore CMD is set automatically, do not set manually")

            # Set CMD for Refiner output store
            oStore.add(DataTransformerOutput(w.name))
            self.addStore(w.dataTransformer.outputDatastore)

    def __eq__(self, __value: object) -> bool:
        if super().__eq__(__value) and isinstance(__value, Team):
            rc: bool = self.name == __value.name
            rc = rc and self.workspaces == __value.workspaces
            rc = rc and self.dataStores == __value.dataStores
            return rc
        return False

    def getStoreOrThrow(self, storeName: str) -> Datastore:
        rc: Optional[Datastore] = self.dataStores.get(storeName)
        if rc:
            return rc
        else:
            raise ObjectDoesntExistException(f"Unknown datastore {storeName}")

    def areTopLevelChangesAuthorized(self, proposed: GitControlledObject, changeSource: Repository, tree: ValidationTree) -> bool:
        """This is a shallow equality check for the top level team object"""
        # If we are being changed by an authorized source then it doesnt matter
        if (self.owningRepo == changeSource):
            return True
        if not super().areTopLevelChangesAuthorized(proposed, changeSource, tree):
            return False
        if not isinstance(proposed, Team):
            return False
        if self.dataStores != proposed.dataStores:
            dTree: ValidationTree = tree.createChild("Datastores")
            self.showDictChangesAsProblems(self.dataStores, proposed.dataStores, dTree)
            return False
        if self.workspaces != proposed.workspaces:
            wTree: ValidationTree = tree.createChild("Workspaces")
            self.showDictChangesAsProblems(self.workspaces, proposed.workspaces, wTree)
            return False
        return True

    def checkIfChangesAreAuthorized(self, proposed: GitControlledObject, changeSource: Repository, vTree: ValidationTree) -> None:
        """This checks if the team has changed relative to the specified change source"""
        prop_Team: Team = cast(Team, proposed)

        self.checkTopLevelAttributeChangesAreAuthorized(prop_Team, changeSource, vTree)

    def lint(self, eco: Ecosystem, gz: 'GovernanceZone', td: 'TeamDeclaration', teamTree: ValidationTree) -> None:
        """This validates a single team declaration and populates the datastore cache with that team's stores"""
        for s in self.dataStores.values():
            if eco.datastoreCache.get(s.name) is not None:
                teamTree.addRaw(DuplicateObject(s, ProblemSeverity.ERROR))
            else:
                storeTree: ValidationTree = teamTree.createChild(s)
                eco.cache_addDatastore(s, self)
                if (td.key):
                    s.setTeam(td.key)
                s.lint(eco, gz, self, storeTree)

        # Iterate over the workspaces to populate the cache but dont lint them yet
        for w in self.workspaces.values():
            if eco.workSpaceCache.get(w.name) is not None:
                teamTree.addRaw(DuplicateObject(w, ProblemSeverity.ERROR))
                # Cannot validate Workspace datasets until everything is loaded
            else:
                eco.cache_addWorkspace(self, w)
                if (gz.key):
                    w.setTeam(TeamDeclarationKey(gz.key, self.name))
                else:
                    teamTree.addRaw(AttributeNotSet(f"{gz} has no key"))
                wTree: ValidationTree = teamTree.createChild(w)

                # Check all classification allows policies from gz are satisfied on every sink
                for dccPolicy in gz.classificationPolicies.values():
                    for dsg in w.dsgs.values():
                        for sink in dsg.sinks.values():
                            store: Datastore = eco.cache_getDatastoreOrThrow(sink.storeName).datastore
                            dataset: Dataset = store.datasets[sink.datasetName]
                            if (not dataset.checkClassificationsAreOnly(dccPolicy)):
                                wTree.addRaw(ObjectNotCompatibleWithPolicy(sink, dccPolicy, ProblemSeverity.ERROR))
        self.superLint(teamTree)

    def __str__(self) -> str:
        return f"Team({self.name})"

    def isBackwardsCompatibleWith(self, originTeam: 'Team', vTree: ValidationTree):
        """This checks if the current team is backwards compatible with the origin team"""
        # Check if the datasets are compatible
        for store in self.dataStores.values():
            sTree: ValidationTree = vTree.createChild(store)
            originStore: Optional[Datastore] = originTeam.dataStores.get(store.name)
            if (originStore):
                store.isBackwardsCompatibleWith(originStore, sTree)


class NamedObjectAuthorization:
    """This represents a named object under the management of a repository. It is used to authorize the existence
    of the object before the specified repository can be used to edit/specify it."""
    def __init__(self, name: str, owningRepo: Repository) -> None:
        self.name: str = name
        self.owningRepo: Repository = owningRepo

    def lint(self, tree: ValidationTree):
        self.owningRepo.lint(tree)

    def __eq__(self, __value: object) -> bool:
        if (isinstance(__value, NamedObjectAuthorization)):
            return self.name == __value.name and self.owningRepo == __value.owningRepo
        else:
            return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"


G = TypeVar('G', bound=GitControlledObject)
N = TypeVar('N', bound=NamedObjectAuthorization)


class AuthorizedObjectManager(Generic[G, N], GitControlledObject):
    """This tracks a list of named authorizations and the named objects themselves in seperate lists. It is used
    to allow one repository to managed the authorization to create named objects using a second object specific repository or branch.
    Each named object can then be managed by a seperate repository. """
    def __init__(self, name: str, factory: Callable[[str, Repository], G], owningRepo: Repository) -> None:
        super().__init__(owningRepo)
        self.name: str = name
        self.authorizedNames: dict[str, N] = OrderedDict[str, N]()
        self.authorizedObjects: dict[str, G] = OrderedDict[str, G]()
        self.factory: Callable[[str, Repository], G] = factory

    def getNumObjects(self) -> int:
        """Returns the number of objects"""
        return len(self.authorizedObjects)

    def addAuthorization(self, t: N):
        """This is used to add a named authorization along with its owning repository to the list of authorizations."""
        if self.authorizedNames.get(t.name) is not None:
            raise ObjectAlreadyExistsException(f"Duplicate authorization {t.name}")
        self.authorizedNames[t.name] = t

    def defineAllObjects(self) -> None:
        """This 'defines' all declared objects"""
        for n in self.authorizedNames.values():
            self.getObject(n.name)

    def getObject(self, name: str) -> Optional[G]:
        """This returns a managed object for the specified name. Users can then fill out the attributes
        of the returned object."""
        noa: Optional[N] = self.authorizedNames.get(name)
        if (noa is None):
            return None
        t: Optional[G] = self.authorizedObjects.get(name)
        if (t is None):
            t = self.factory(name, noa.owningRepo)  # Create an instance of the object
            self.authorizedObjects[name] = t
        return t

    def __eq__(self, __value: object) -> bool:
        if (super().__eq__(__value) and isinstance(__value, AuthorizedObjectManager)):
            a: AuthorizedObjectManager[G, N] = cast(AuthorizedObjectManager[G, N], __value)
            rc: bool = self.authorizedNames == a.authorizedNames
            rc = rc and self.name == a.name
            rc = rc and self.authorizedObjects == a.authorizedObjects
            # Cannot test factory for equality
            # rc = rc and self.factory == a.factory
            return rc
        else:
            return False

    def areTopLevelChangesAuthorized(self, proposed: GitControlledObject, changeSource: Repository, tree: ValidationTree) -> bool:
        p: AuthorizedObjectManager[G, N] = cast(AuthorizedObjectManager[G, N], proposed)
        # If we are modified by an authorized source then it doesn't matter if its different or not
        if (self.owningRepo == changeSource):
            return True
        else:
            if (self.authorizedNames == p.authorizedNames):
                return True
            else:
                self.showDictChangesAsProblems(self.authorizedNames, p.authorizedNames, tree)
                return False

    def checkIfChangesAreAuthorized(self, proposed: GitControlledObject, changeSource: Repository, vTree: ValidationTree) -> None:
        proposedGZ: AuthorizedObjectManager[G, N] = cast(AuthorizedObjectManager[G, N], proposed)

        """This checks if the governance zone has changed relative to the specified change source"""
        """This checks if any teams have been added or removed relative to e"""

        self.checkTopLevelAttributeChangesAreAuthorized(proposedGZ, changeSource, vTree)

        # Get the current teams from the change source
        self.checkDictChangesAreAuthorized(self.authorizedObjects, proposedGZ.authorizedObjects, changeSource, vTree)

    def removeAuthorization(self, name: str) -> Optional[N]:
        """Removes the authorization from the list of authorized names"""
        r: Optional[N] = self.authorizedNames.pop(name)
        if (r and self.authorizedObjects.get(name) is not None):
            self.removeDefinition(name)
        return r

    def removeDefinition(self, name: str) -> Optional[G]:
        """Removes the object definition . This must be done by the object repo before the parent repo can remove the authorization"""
        r: Optional[G] = self.authorizedObjects.pop(name)
        return r

    def __str__(self) -> str:
        return f"AuthorizedObjectManager({self.name})"

    def lint(self, tree: ValidationTree):
        self.superLint(tree)


class TeamDeclaration(NamedObjectAuthorization):
    """This is a declaration of a team within a governance zone. It is used to authorize
    the team and to provide the official source of changes for that object and its children"""
    def __init__(self, name: str, authRepo: Repository) -> None:
        super().__init__(name, authRepo)
        self.authRepo: Repository = authRepo
        self.key: Optional[TeamDeclarationKey] = None

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TeamDeclaration) and self.authRepo == __value.authRepo and self.key == __value.key

    def setGovernanceZone(self, gz: 'GovernanceZone') -> None:
        """Sets the governance zone for this team and sets the team for all datastores and workspaces"""
        if gz.key:
            self.key = TeamDeclarationKey(gz.key, self.name)


class GovernanceZoneDeclaration(NamedObjectAuthorization):
    """This is a declaration of a governance zone within an ecosystem. It is used to authorize
    the definition of a governance zone and to provide the official source of changes for that object and its children"""
    def __init__(self, name: str, authRepo: Repository) -> None:
        super().__init__(name, authRepo)
        self.key: Optional[GovernanceZoneKey] = None

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, GovernanceZoneDeclaration) and self.key == __value.key


class GovernanceZone(GitControlledObject):
    """This declares the existence of a specific GovernanceZone and defines the teams it manages, the storage policies
    and which repos can be used to pull changes for various metadata"""
    def __init__(self, name: str, ownerRepo: Repository, *args: Union[InfraStructureLocationPolicy, InfraStructureVendorPolicy,
                                                                      StoragePolicy, DataClassificationPolicy, TeamDeclaration,
                                                                      Documentation, DataPlatformPolicy, InfraHardVendorPolicy]) -> None:
        super().__init__(ownerRepo)
        self.name: str = name
        self.key: Optional[GovernanceZoneKey] = None
        self.teams: AuthorizedObjectManager[Team, TeamDeclaration] = AuthorizedObjectManager[Team, TeamDeclaration](
            "teams", lambda name, repo: Team(name, repo), ownerRepo)

        self.storagePolicies: dict[str, StoragePolicy] = OrderedDict[str, StoragePolicy]()
        # Schemas for datasets defined in this GZ must comply with these classification restrictions
        self.classificationPolicies: dict[str, DataClassificationPolicy] = dict[str, DataClassificationPolicy]()
        # Only these vendors are allowed within this GZ (Datastores and Workspaces)
        self.vendorPolicies: dict[str, InfraStructureVendorPolicy] = dict[str, InfraStructureVendorPolicy]()
        # Only these locations are allowed within this GZ (Datastore and Workspaces)
        self.hardVendorPolicies: dict[str, InfraHardVendorPolicy] = dict[str, InfraHardVendorPolicy]()
        self.locationPolicies: dict[str, InfraStructureLocationPolicy] = dict[str, InfraStructureLocationPolicy]()
        self.dataplatformPolicies: dict[str, DataPlatformPolicy] = dict[str, DataPlatformPolicy]()

        self.add(*args)

    def setEcosystem(self, eco: Ecosystem) -> None:
        """Sets the ecosystem for this zone and sets the zone for all teams"""
        self.key = GovernanceZoneKey(eco.key, self.name)

        self.add()

    def checkLocationIsAllowed(self, eco: 'Ecosystem', loc: InfrastructureLocation, tree: ValidationTree):
        """This checks that the provided location is allowed based on the vendor and location policies
        of the GZ, this allows a GZ to constrain where its data can come from or be used"""
        for locPolicy in self.locationPolicies.values():
            if not locPolicy.isCompatible(loc):
                tree.addRaw(ObjectNotCompatibleWithPolicy(loc, locPolicy, ProblemSeverity.ERROR))
        if (loc.key):
            v: InfrastructureVendor = eco.getVendorOrThrow(loc.key.ivName)
            for vendorPolicy in self.vendorPolicies.values():
                if not vendorPolicy.isCompatible(v):
                    tree.addRaw(ObjectNotCompatibleWithPolicy(v, vendorPolicy, ProblemSeverity.ERROR))
            for hardVendorPolicy in self.hardVendorPolicies.values():
                if (v.hardCloudVendor is None):
                    tree.addRaw(AttributeNotSet(f"{loc} No hard cloud vendor"))
                elif not hardVendorPolicy.isCompatible(v.hardCloudVendor):
                    tree.addRaw(ObjectNotCompatibleWithPolicy(v, hardVendorPolicy, ProblemSeverity.ERROR))
        else:
            tree.addRaw(AttributeNotSet("loc.key"))

    def add(self, *args: Union[InfraStructureVendorPolicy, InfraStructureLocationPolicy, StoragePolicy, DataClassificationPolicy,
                               TeamDeclaration, DataPlatformPolicy, Documentation, InfraHardVendorPolicy]) -> None:
        for arg in args:
            if (isinstance(arg, DataClassificationPolicy)):
                dcc: DataClassificationPolicy = arg
                self.classificationPolicies[dcc.name] = dcc
            elif (isinstance(arg, InfraStructureLocationPolicy)):
                self.locationPolicies[arg.name] = arg
            elif (isinstance(arg, InfraStructureVendorPolicy)):
                self.vendorPolicies[arg.name] = arg
            elif (isinstance(arg, StoragePolicy)):
                sp: StoragePolicy = arg
                if self.storagePolicies.get(sp.name) is not None:
                    raise Exception(f"Duplicate Storage Policy {sp.name}")
                self.storagePolicies[sp.name] = sp
            elif (type(arg) is TeamDeclaration):
                t: TeamDeclaration = arg
                self.teams.addAuthorization(t)
            elif (isinstance(arg, InfraHardVendorPolicy)):
                self.hardVendorPolicies[arg.name] = arg
            elif (isinstance(arg, DataPlatformPolicy)):
                self.dataplatformPolicies[arg. name] = arg
            elif (isinstance(arg, Documentation)):
                d: Documentation = arg
                self.documentation = d

        # Set softlink keys
        if (self.key):
            for sp in self.storagePolicies.values():
                sp.setGovernanceZone(self)
            for td in self.teams.authorizedNames.values():
                td.setGovernanceZone(self)

    def getTeam(self, name: str) -> Optional[Team]:
        return self.teams.getObject(name)

    def getTeamOrThrow(self, name: str) -> Team:
        t: Optional[Team] = self.getTeam(name)
        if (t):
            return t
        else:
            raise ObjectDoesntExistException(f"Unknown team {name}")

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, GovernanceZone):
            rc: bool = super().__eq__(__value)
            rc = rc and self.name == __value.name
            rc = rc and self.key == __value.key
            rc = rc and self.dataplatformPolicies == __value.dataplatformPolicies
            rc = rc and self.teams == __value.teams
            rc = rc and self.classificationPolicies == __value.classificationPolicies
            rc = rc and self.storagePolicies == __value.storagePolicies
            rc = rc and self.vendorPolicies == __value.vendorPolicies
            rc = rc and self.hardVendorPolicies == __value.hardVendorPolicies
            rc = rc and self.locationPolicies == __value.locationPolicies
            return rc
        return False

    def areTopLevelChangesAuthorized(self, proposed: GitControlledObject, changeSource: Repository, tree: ValidationTree) -> bool:
        """Just check the not git controlled attributes"""
        # If we're changed by an authorized source then it doesn't matter
        if (self.owningRepo == changeSource):
            return True
        if not (super().areTopLevelChangesAuthorized(proposed, changeSource, tree) and type(proposed) is GovernanceZone and self.name == proposed.name):
            return False
        if self.storagePolicies != proposed.storagePolicies:
            self.showDictChangesAsProblems(self.storagePolicies, proposed.storagePolicies, tree.createChild("StoragePolicies"))
            return False
        if self.dataplatformPolicies != proposed.dataplatformPolicies:
            self.showDictChangesAsProblems(self.dataplatformPolicies, proposed.dataplatformPolicies, tree.createChild("DataPlatformPolicies"))
            return False
        if self.vendorPolicies != proposed.vendorPolicies:
            self.showDictChangesAsProblems(self.vendorPolicies, proposed.vendorPolicies, tree.createChild("VendorPolicies"))
            return False
        if self.locationPolicies != proposed.locationPolicies:
            self.showDictChangesAsProblems(self.locationPolicies, proposed.locationPolicies, tree.createChild("LocationPolicies"))
            return False
        if not self.teams.areTopLevelChangesAuthorized(proposed.teams, changeSource, tree):
            return False
        return True

    def checkIfChangesAreAuthorized(self, proposed: GitControlledObject, changeSource: Repository, vTree: ValidationTree) -> None:
        proposedGZ: GovernanceZone = cast(GovernanceZone, proposed)

        """This checks if the governance zone has changed relative to the specified change source"""
        """This checks if any teams have been added or removed relative to e"""

        self.checkTopLevelAttributeChangesAreAuthorized(proposedGZ, changeSource, vTree)

        # Get the current teams from the change source
        self.teams.checkIfChangesAreAuthorized(proposedGZ.teams, changeSource, vTree)

    def lint(self, eco: Ecosystem, govTree: ValidationTree) -> None:
        """This validates a GovernanceZone and populates the teamcache with the zones teams"""

        # Make sure each Team is defined
        self.teams.defineAllObjects()

        for team in self.teams.authorizedObjects.values():
            td: TeamDeclaration = self.teams.authorizedNames[team.name]
            if (td.key is None):
                govTree.addRaw(AttributeNotSet(f"{td} has no key"))
            else:
                # Add Team to eco level cache, check for dup Teams and lint
                if (eco.teamCache.get(team.name) is not None):
                    govTree.addRaw(DuplicateObject(team, ProblemSeverity.ERROR))
                else:
                    eco.cache_addTeam(td, team)
                    teamTree: ValidationTree = govTree.createChild(team)
                    team.lint(eco, self, td, teamTree)
        self.superLint(govTree)
        self.teams.lint(govTree)
        if (self.key is None):
            govTree.addRaw(AttributeNotSet("Key not set"))

    def __str__(self) -> str:
        return f"GovernanceZone({self.name})"

    def isBackwardsCompatibleWith(self, originZone: 'GovernanceZone', tree: ValidationTree):
        """This checks if this zone is backwards compatible with the original zone. This means that the proposed zone
        can be used to replace this one without breaking any data pipelines"""

        # Check if the teams are compatible
        for team in self.teams.authorizedObjects.values():
            tTree: ValidationTree = tree.createChild(team)
            originTeam: Optional[Team] = originZone.getTeam(team.name)
            # if team exists in old zone then check it, otherwise, it's a new team and we don't care
            if originTeam:
                team.isBackwardsCompatibleWith(originTeam, tTree)

    def getDatasetStoragePolicies(self, dataset: Dataset) -> Sequence[StoragePolicy]:
        """Returns the storage policies for the specified dataset including mandatory ones"""
        rc: list[StoragePolicy] = []
        rc.extend(dataset.policies.values())
        for sp in self.storagePolicies.values():
            if (sp.mandatory == PolicyMandatedRule.MANDATED_WITHIN_ZONE):
                rc.append(sp)
        return rc


@dataclass
class WorkspaceEntitlement:
    pass


@dataclass
class EventSink:
    pass


@dataclass
class Deliverable:
    pass


class DataPlatform(ABC, Documentable):
    """This is a system which can interpret data flows in the metadata and realize those flows"""
    def __init__(self, name: str, doc: Documentation) -> None:
        Documentable.__init__(self, doc)
        self.name: str = name

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, DataPlatform) and self.name == __value.name

    def __hash__(self) -> int:
        return hash(self.name)

    @abstractmethod
    def getInternalDataContainers(self) -> set[DataContainer]:
        """A Data platform can have internal data containers which store ingested data or intermediate data"""
        pass

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    @abstractmethod
    def getSupportedVendors(self, eco: Ecosystem) -> set[InfrastructureVendor]:
        pass

    @abstractmethod
    def isContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        pass

    @abstractmethod
    def lint(self, eco: Ecosystem, tree: ValidationTree):
        if (self.documentation):
            self.documentation.lint(tree)


class DataLatency(Enum):
    """Specifies the acceptable latency range from a consumer"""
    SECONDS = 0
    """Up to 59 seconds"""
    MINUTES = 1
    """Up to 59 minutes"""
    HOURS = 3
    """Up to 24 hours"""
    DAYS = 4
    """A day or more"""


class DataRetentionPolicy(Enum):
    """Client indicates whether the data is live or forensic"""
    LIVE_ONLY = 0
    """Only the latest version of each record should be retained"""
    FORENSIC = 1
    """All versions of every record in every table used to produce the datasets should be retained and present in the consumer data tables"""
    LIVE_WITH_FORENSIC_HISTORY = 2
    """All versions of each record should be retained BUT only latest records are needed in the consumer data tables"""


# This needs to be keyed and rolled up to manage definitions centrally, there should
# be a common ESMA definition for example (5 years forensic)
class ConsumerRetentionRequirements:
    """Consumers specify the retention requirements for the data they consume. Platforms use this to backtrack
    retention requirements for data in the full inferred pipeline to manage that consumer"""
    def __init__(self, r: DataRetentionPolicy, latency: DataLatency, regulator: Optional[str],
                 minRetentionDurationIfNeeded: Optional[timedelta] = None) -> None:
        self.policy: DataRetentionPolicy = r
        self.latency: DataLatency = latency
        self.minRetentionTime: Optional[timedelta] = minRetentionDurationIfNeeded
        self.regulator: Optional[str] = regulator

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())


class DataPlatformChooser(ABC):
    """Subclasses of this choose a DataPlatform to render the pipeline for moving data from a producer to a Workspace possibly
    through intermediate Workspaces"""
    def __init__(self):
        pass

    @abstractmethod
    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class FixedDataPlatform(DataPlatformChooser):
    def __init__(self, dp: DataPlatform):
        self.fixedDataPlatform: DataPlatform = dp

    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        return self.fixedDataPlatform

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, FixedDataPlatform) and self.fixedDataPlatform == __value.fixedDataPlatform

    def __str__(self) -> str:
        return f"FixedDataPlatform({self.fixedDataPlatform})"


class WorkspacePlatformConfig(DataPlatformChooser):
    """This allows a Workspace to specify per pipeline hints for behavior, i.e.
    allowed latency and so on"""
    def __init__(self, hist: ConsumerRetentionRequirements) -> None:
        self.retention: ConsumerRetentionRequirements = hist

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        """For now, just return default"""
        # TODO This should evaluate the parameters provide and choose the 'best' DataPlatform
        return eco.getDefaultDataPlatform()

    def __str__(self) -> str:
        return f"WorkspacePlatformConfig({self.retention})"


class WorkspaceFixedDataPlatform(DataPlatformChooser):
    """This specifies a fixed DataPlatform for a Workspace"""
    def __init__(self, dp: DataPlatform):
        self.dataPlatform: DataPlatform = dp

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, WorkspaceFixedDataPlatform) and self.dataPlatform == o.dataPlatform

    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        return self.dataPlatform

    def __str__(self) -> str:
        return f"WorkspaceFixedDataPlatform({self.dataPlatform})"


class DeprecationsAllowed(Enum):
    """This specifies if deprecations are allowed for a specific dataset in a workspace dsg"""
    NEVER = 0
    """Deprecations are never allowed"""
    ALLOWED = 1
    """Deprecations are allowed but not will generate warnings"""


class DatasetSink(object):
    """This is a reference to a dataset in a Workspace"""
    def __init__(self, storeName: str, datasetName: str, deprecationsAllowed: DeprecationsAllowed = DeprecationsAllowed.NEVER) -> None:
        self.storeName: str = storeName
        self.datasetName: str = datasetName
        self.key = f"{self.storeName}:{self.datasetName}"
        self.deprecationsAllowed: DeprecationsAllowed = deprecationsAllowed

    def __eq__(self, __value: object) -> bool:
        if (type(__value) is DatasetSink):
            return self.key == __value.key and self.storeName == __value.storeName and self.datasetName == __value.datasetName
        else:
            return False

    def __hash__(self) -> int:
        return hash(f"{self.storeName}/{self.datasetName}")

    def lint(self, eco: Ecosystem, team: Team, ws: 'Workspace', tree: ValidationTree):
        """Check the DatasetSink meets all policy checks"""
        if not is_valid_sql_identifier(self.storeName):
            tree.addRaw(NameMustBeSQLIdentifier(f"DatasetSink store name {self.storeName}", ProblemSeverity.ERROR))
        if not is_valid_sql_identifier(self.datasetName):
            tree.addRaw(NameMustBeSQLIdentifier(f"DatasetSink dataset name {self.datasetName}", ProblemSeverity.ERROR))
        dataset: Optional[Dataset] = eco.cache_getDataset(self.storeName, self.datasetName)
        if (dataset is None):
            tree.addRaw(ConstraintViolation(f"Unknown dataset {self.storeName}:{self.datasetName}", ProblemSeverity.ERROR))
        else:
            storeI: Optional[DatastoreCacheEntry] = eco.datastoreCache.get(self.storeName)
            if storeI:
                store: Datastore = storeI.datastore
                # Check Workspace dataContainer locations are compatible with the Datastore gz policies
                if (store.key):
                    gzStore: GovernanceZone = eco.getZoneOrThrow(store.key.gzName)
                    if (ws.dataContainer):
                        ws.dataContainer.lint(eco, gzStore, team, tree)
                else:
                    tree.addRaw(AttributeNotSet(f"{store} key is None"))

                # Production data in non production or vice versa should be noted
                if (store.productionStatus != ws.productionStatus):
                    tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} is using a datastore with a different production status",
                                    ProblemSeverity.WARNING)
                if store.isDatasetDeprecated(dataset):
                    if self.deprecationsAllowed == DeprecationsAllowed.NEVER:
                        tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} is deprecated and deprecations are not allowed")
                    elif (self.deprecationsAllowed == DeprecationsAllowed.ALLOWED):
                        tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} is using deprecated dataset", ProblemSeverity.WARNING)
                dataset: Optional[Dataset] = store.datasets.get(self.datasetName)
                if (dataset is None):
                    tree.addRaw(UnknownObjectReference(f"Unknown dataset {self.storeName}:{self.datasetName}", ProblemSeverity.ERROR))
                else:
                    if (ws.classificationVerifier and not dataset.checkClassificationsAreOnly(ws.classificationVerifier)):
                        tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} has unexpected classifications")
            else:
                tree.addRaw(UnknownObjectReference(f"Datastore {self.storeName}", ProblemSeverity.ERROR))

    def __str__(self) -> str:
        return f"DatasetSink({self.storeName}:{self.datasetName})"


class DatasetGroup(ANSI_SQL_NamedObject, Documentable):
    """A collection of Datasets which are rendered with a specific pipeline spec in a Workspace. The name should be
    ANSI SQL compliant because it could be used as part of a SQL View/Table name in a Workspace database"""
    def __init__(self, name: str, *args: Union[DatasetSink, WorkspacePlatformConfig, Documentation]) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        self.platformMD: Optional[DataPlatformChooser] = None
        self.sinks: dict[str, DatasetSink] = OrderedDict[str, DatasetSink]()
        for arg in args:
            if (type(arg) is DatasetSink):
                sink: DatasetSink = arg
                if (self.sinks.get(sink.key) is not None):
                    raise ObjectAlreadyExistsException(f"Duplicate DatasetSink {sink.key}")
                self.sinks[sink.key] = sink
            elif (isinstance(arg, Documentation)):
                self.documentation = arg
            elif (isinstance(arg, WorkspacePlatformConfig)):
                if self.platformMD is None:
                    self.platformMD = arg
                else:
                    raise AttributeAlreadySetException("Platform")
            else:
                raise UnknownArgumentException(f"Unknown argument {type(arg)}")

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

    def lint(self, eco: Ecosystem, team: Team, ws: 'Workspace', tree: ValidationTree):
        super().nameLint(tree)
        if (self.documentation):
            self.documentation.lint(tree)
        if not is_valid_sql_identifier(self.name):
            tree.addRaw(NameMustBeSQLIdentifier(f"DatasetGroup name {self.name}", ProblemSeverity.ERROR))
        for sink in self.sinks.values():
            sinkTree: ValidationTree = tree.createChild(sink)
            sink.lint(eco, team, ws, sinkTree)
        if (self.platformMD):
            platform: Optional[DataPlatform] = self.platformMD.choooseDataPlatform(eco)
            if (platform is None):
                tree.addProblem("DSG doesnt choose a dataplatform")
        else:
            tree.addRaw(AttributeNotSet("DSG has no data platform chooser"))

    def __str__(self) -> str:
        return f"DatasetGroup({self.name})"


class TransformerTrigger:
    def __init__(self, name: str):
        self.name: str = name

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def __eq__(self, o: object) -> bool:
        return isinstance(o, TransformerTrigger) and self.name == o.name


class TimedTransformerTrigger(TransformerTrigger):
    def __init__(self, name: str, transformerTrigger: StepTrigger):
        super().__init__(name)
        self.trigger: StepTrigger = transformerTrigger

    def __eq__(self, o: object) -> bool:
        return isinstance(o, TimedTransformerTrigger) and self.trigger == o.trigger and super().__eq__(o)


class CodeArtifact(ABC):
    """This defines a piece of code which can be used to transform data in a workspace"""

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        pass

    def __eq__(self, o: object) -> bool:
        return isinstance(o, CodeArtifact)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"


class PythonCodeArtifact(CodeArtifact):
    """This describes a python job and its dependencies"""
    def __init__(self, requirements: list[str], envVars: dict[str, str], requiredVersion: str) -> None:
        self.requirements: list[str] = requirements
        self.envVars: dict[str, str] = envVars
        self.requiredVersion: str = requiredVersion

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        # TODO more
        pass

    def __eq__(self, o: object) -> bool:
        if isinstance(o, PythonCodeArtifact):
            rc: bool = self.requiredVersion == o.requiredVersion
            rc = rc and self.requirements == o.requirements
            rc = rc and self.envVars == o.envVars
            return rc
        else:
            return False


class CodeExecutionEnvironment(ABC):
    """This is an environment which can execute code, AWS Lambda, Azure Functions, Kubernetes, etc"""
    def __init__(self, loc: InfrastructureLocation):
        self.location: InfrastructureLocation = loc

    def __eq__(self, o: object) -> bool:
        return isinstance(o, CodeExecutionEnvironment) and self.location == o.location

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        self.location.lint(tree)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.location})"


class KubernetesEnvironment(CodeExecutionEnvironment):
    """This is a Kubernetes environment"""
    def __init__(self, hostName: str, cred: Credential, loc: InfrastructureLocation) -> None:
        super().__init__(loc)
        self.hostName: str = hostName
        """This is the hostname of the Kubernetes cluster"""
        self.credential: Credential = cred
        """This is the credential used to access the Kubernetes cluster"""

    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        super().lint(eco, tree)
        if not is_valid_hostname_or_ip(self.hostName):
            tree.addProblem(f"Invalid host name <{self.hostName}>")
        cTree: ValidationTree = tree.createChild(self.credential)
        self.credential.lint(eco, cTree)


class DataTransformer(ANSI_SQL_NamedObject, Documentable):
    """This allows new data to be produced from existing data. The inputs to the transformer are the
    datasets in the workspace and the output is a Datastore associated with the transformer. The transformer
    will be triggered using the specified trigger policy"""
    def __init__(self, name: str, store: Datastore, trigger: TransformerTrigger, code: CodeArtifact,
                 codeEnv: CodeExecutionEnvironment, doc: Optional[Documentation] = None) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        # This Datastore is defined here and has a CaptureMetaData automatically added. Do not specify a CMD in the Datastore
        self.outputDatastore: Datastore = store
        self.trigger: TransformerTrigger = trigger
        self.code: CodeArtifact = code
        self.codeEnv: CodeExecutionEnvironment = codeEnv
        self.documentation = doc

    def lint(self, eco: Ecosystem, ws: 'Workspace', tree: ValidationTree):
        ANSI_SQL_NamedObject.nameLint(self, tree)
        if (self.documentation):
            self.documentation.lint(tree)
        # Does store exist
        storeI: Optional[DatastoreCacheEntry] = eco.datastoreCache.get(self.outputDatastore.name)
        if (storeI is None):
            tree.addRaw(UnknownObjectReference(f"datastore {self.outputDatastore.name}", ProblemSeverity.ERROR))
        else:
            if (storeI.datastore.productionStatus != ws.productionStatus):
                tree.addRaw(ConstraintViolation(f"DataTransformer {self.name} is using a datastore with a different production status",
                                                ProblemSeverity.WARNING))

            workSpaceI: WorkspaceCacheEntry = eco.cache_getWorkspaceOrThrow(ws.name)
            if (workSpaceI.team != storeI.team):
                tree.addRaw(ConstraintViolation(f"DataTransformer {self.name} is using a datastore from a different team", ProblemSeverity.ERROR))
        codeEnvTree: ValidationTree = tree.createChild(self.codeEnv)
        self.codeEnv.lint(eco, codeEnvTree)
        codeTree: ValidationTree = tree.createChild(self.codeEnv)
        self.code.lint(eco, codeTree)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, DataTransformer) and self.name == o.name and self.outputDatastore == o.outputDatastore and \
            self.trigger == o.trigger and self.code == o.code and self.codeEnv == o.codeEnv


class Workspace(ANSI_SQL_NamedObject, Documentable):
    """A collection of datasets used by a consumer for a specific use case. This consists of one or more groups of datasets with each set using
    the correct pipeline spec.
    Specific datasets can be present in multiple groups. They will be named differently in each group. The name needs to be ANSI SQL because
    it could be used as part of a SQL View/Table name in a Workspace database. Workspaces must have ecosystem unique names"""
    def __init__(self, name: str, *args: Union[DatasetGroup, DataContainer, Documentation, DataClassificationPolicy, ProductionStatus,
                                               DeprecationInfo, DataTransformer]) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        self.dsgs: dict[str, DatasetGroup] = OrderedDict[str, DatasetGroup]()
        self.dataContainer: Optional[DataContainer] = None
        self.productionStatus: ProductionStatus = ProductionStatus.NOT_PRODUCTION
        self.deprecationStatus: DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)
        self.dataTransformer: Optional[DataTransformer] = None
        # This is the set of classifications expected in the Workspace. Linting fails
        # if any datsets/attributes found with classifications different than these
        self.classificationVerifier: Optional[DataClassificationPolicy] = None
        self.key: Optional[WorkspaceKey] = None
        """This workspace is the input to a data transformer if set"""
        self.add(*args)

    def setTeam(self, key: TeamDeclarationKey):
        self.key = WorkspaceKey(key, self.name)

    def add(self, *args: Union[DatasetGroup, DataContainer, Documentation, DataClassificationPolicy, ProductionStatus, DeprecationInfo, DataTransformer]):
        for arg in args:
            if (isinstance(arg, DatasetGroup)):
                if (self.dsgs.get(arg.name) is not None):
                    raise ObjectAlreadyExistsException(f"Duplicate DatasetGroup {arg.name}")
                self.dsgs[arg.name] = arg
            elif (isinstance(arg, DataClassificationPolicy)):
                self.classificationVerifier = arg
            elif (isinstance(arg, DataContainer)):
                if (self.dataContainer is not None and self.dataContainer != arg):
                    raise AttributeAlreadySetException("dataContainer")
                self.dataContainer = arg
            elif (isinstance(arg, ProductionStatus)):
                self.productionStatus = arg
            elif (isinstance(arg, DeprecationInfo)):
                self.deprecationStatus = arg
            elif (isinstance(arg, Documentation)):
                if (self.documentation is not None and self.documentation != arg):
                    raise AttributeAlreadySetException("Documentation")
                self.documentation = arg
            else:
                if (self.dataTransformer is not None and self.dataTransformer != arg):
                    raise AttributeAlreadySetException("DataTransformer")
                self.dataTransformer = arg

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

    def isDatastoreUsed(self, store: Datastore) -> bool:
        """Returns true if the specified datastore is used by this workspace"""
        for dsg in self.dsgs.values():
            for sink in dsg.sinks.values():
                if sink.storeName == store.name:
                    return True
        return False

    def lint(self, eco: Ecosystem, gz: GovernanceZone, t: Team, tree: ValidationTree):
        super().nameLint(tree)

        if (self.key is None):
            tree.addRaw(AttributeNotSet("Workspace key is none"))

        # Check Workspaces in this gz are on dataContainers compatible with vendor
        # and location policies for this GZ
        if (self.dataContainer):
            cntTree: ValidationTree = tree.createChild(self.dataContainer)
            self.dataContainer.lint(eco, gz, t, cntTree)

        # Check production status of workspace matches all datasets in use
        # Check deprecation status of workspace generates warnings for all datasets in use
        # Lint the DSGs
        for dsg in self.dsgs.values():
            dsgTree: ValidationTree = tree.createChild(dsg)
            dsg.lint(eco, t, self, dsgTree)

        # Link the transformer if present
        if self.dataTransformer:
            dtTree: ValidationTree = tree.createChild(self.dataTransformer)
            self.dataTransformer.lint(eco, self, dtTree)

    def __str__(self) -> str:
        return f"Workspace({self.name})"


class PlatformStyle(Enum):
    OLTP = 0
    OLAP = 1
    COLUMNAR = 2
    OBJECT = 3
