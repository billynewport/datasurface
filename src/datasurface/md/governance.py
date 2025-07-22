"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from dataclasses import dataclass
from collections import OrderedDict
import os
import tempfile
from typing import Any, Callable, Optional, Sequence, Type, TypeVar, Union, cast
from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from typing import Generic
from datasurface.md.types import DataType

from datasurface.md.exceptions import AttributeAlreadySetException, ObjectAlreadyExistsException, ObjectDoesntExistException
from datasurface.md.lint import AttributeNotSet, ConstraintViolation, DataTransformerMissing, DuplicateObject, NameHasBadSynthax, NameMustBeSQLIdentifier, \
        ObjectIsDeprecated, ObjectMissing, ObjectNotCompatibleWithPolicy, ObjectWrongType, ProductionDatastoreMustHaveClassifications, \
        UnauthorizedAttributeChange, ProblemSeverity, UnknownChangeSource, UnknownObjectReference, ValidationProblem, ValidationTree, UserDSLObject, \
        InternalLintableObject, ANSI_SQL_NamedObject, UnexpectedExceptionProblem, ObjectNotSupportedByDataPlatform
from datasurface.md.json import JSONable
import hashlib
from datasurface.md.utils import is_valid_sql_identifier, is_valid_hostname_or_ip, validate_cron_string
from datasurface.md.documentation import Documentation, Documentable
from datasurface.md.repo import Repository, GitControlledObject
from datasurface.md.policy import Policy, AllowDisallowPolicy, DataClassification, DataClassificationPolicy, Literal
from datasurface.md.schema import Schema
from datasurface.md.keys import StoragePolicyKey, EcosystemKey, TeamDeclarationKey, WorkspaceKey, \
    DatastoreKey, GovernanceZoneKey, DataPlatformKey, LocationKey
from datasurface.md.vendor import CloudVendor, InfrastructureVendor, InfrastructureLocation, convertCloudVendorItems, \
    UnknownLocationProblem, UnknownVendorProblem
from datasurface.md.credential import Credential, CredentialStore
from datasurface.md.keys import InvalidLocationStringProblem


class ProductionStatus(Enum):
    """This indicates whether the team is in production or not"""
    PRODUCTION = 0
    NOT_PRODUCTION = 1


class DeprecationStatus(Enum):
    """This indicates whether the team is deprecated or not"""
    NOT_DEPRECATED = 0
    DEPRECATED = 1


class DeprecationInfo(Documentable, JSONable):
    """This is the deprecation information for an object"""
    def __init__(self, status: DeprecationStatus, reason: Optional[Documentation] = None) -> None:
        Documentable.__init__(self, reason)
        JSONable.__init__(self)
        self.status: DeprecationStatus = status
        """If it deprecated or not"""
        """If deprecated then this explains why and what an existing user should do, alternative dataset for example"""

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and \
            isinstance(other, DeprecationInfo) and self.status == other.status

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "status": self.status.name})
        if self.documentation:
            rc.update({"reason": self.documentation.to_json()})
        return rc


def handleUnsupportedObjectsToJson(obj: object) -> str:
    if isinstance(obj, Enum):
        return obj.name
    elif isinstance(obj, DataType):
        return str(obj.to_json())
    raise Exception(f"Unsupported object {obj} in to_json")


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

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, StoragePolicy) and self.name == other.name and self.mandatory == other.mandatory and \
            self.key == other.key and self.deprecationStatus == other.deprecationStatus

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

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is StoragePolicyAllowAnyContainer and \
            self.name == other.name and self.mandatory == other.mandatory


class InfraHardVendorPolicy(AllowDisallowPolicy[Literal[CloudVendor]]):
    """Allows a GZ to police which vendors can be used with datastore or workspaces within itself"""
    def __init__(self, name: str, doc: Documentation, allowed: Optional[set[CloudVendor]] = None,
                 notAllowed: Optional[set[CloudVendor]] = None):
        super().__init__(name, doc, convertCloudVendorItems(allowed), convertCloudVendorItems(notAllowed))

    def __str__(self):
        return f"InfraStructureVendorPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        return super().__eq__(v) and isinstance(v, InfraStructureVendorPolicy) and self.allowed == v.allowed and self.notAllowed == v.notAllowed

    def __hash__(self) -> int:
        return super().__hash__()

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc


class DataPlatformPolicy(AllowDisallowPolicy['DataPlatformKey']):
    def __init__(self, name: str, doc: Optional[Documentation], allowed: Optional[set['DataPlatformKey']] = None,
                 notAllowed: Optional[set['DataPlatformKey']] = None):
        super().__init__(name, doc, allowed, notAllowed)

    def __str__(self):
        return f"DataPlatformPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        return super().__eq__(v) and isinstance(v, DataPlatformPolicy) and self.allowed == v.allowed and \
            self.notAllowed == v.notAllowed and self.name == v.name

    def __hash__(self) -> int:
        return super().__hash__()

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc


class EncryptionSystem(JSONable):
    """This describes"""
    def __init__(self) -> None:
        JSONable.__init__(self)
        self.name: Optional[str] = None
        self.keyContainer: Optional['DataContainer'] = None
        """Are keys stored on site or at a third party?"""
        self.hasThirdPartySuperUser: bool = False

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, EncryptionSystem) and self.name == __value.name and \
            self.keyContainer == __value.keyContainer and self.hasThirdPartySuperUser == __value.hasThirdPartySuperUser

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        rc.update({"name": self.name})
        rc.update({"keyContainer": self.keyContainer.to_json() if self.keyContainer else None})
        rc.update({"hasThirdPartySuperUser": self.hasThirdPartySuperUser})
        return rc


class SchemaProjector(ABC):
    """This class takes a Schema and projects it to a Schema compatible with an underlying DataContainer"""
    def __init__(self, eco: 'Ecosystem', dp: 'DataPlatform'):
        self.eco: 'Ecosystem' = eco
        self.dp: 'DataPlatform' = dp

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, SchemaProjector) and self.eco == __value.eco and self.dp == __value.dp

    def getSchemaTypes(self) -> set[str]:
        """This returns the types of schemas that this projector can project. Examples could be MERGE or STAGING."""
        return set()

    @abstractmethod
    def computeSchema(self, dataset: 'Dataset', schemaType: str) -> 'Dataset':
        """This returns the actual Dataset in use for that Dataset in the Workspace on this DataPlatform.
        The schemaType is used to determine the type of schema to project."""
        pass


class CaseSensitiveEnum(Enum):
    CASE_SENSITIVE = 0
    """This is a case sensitive enum"""
    CASE_INSENSITIVE = 1


class DataContainerNamingMapper:
    """This is an interface for mapping dataset names and attributes to the underlying data container. This is used to map
    the name of model elements to concrete data container names which may have different standards for naming. Given, consumers
    should be able to use any producer data, this name mapping must succeed. This may require character substitution or even
    truncation of names possibly with an additional hash on the end of the name to ensure uniqueness"""
    def __init__(self, maxLen: int = 255, caseSensitive: CaseSensitiveEnum = CaseSensitiveEnum.CASE_SENSITIVE, allowQuotes: Optional[str] = None) -> None:
        self.maxLen = maxLen
        self.caseSensitive = caseSensitive
        self.allowQuotes = allowQuotes

    def formatIdentifier(self, s: str) -> str:
        if self.caseSensitive == CaseSensitiveEnum.CASE_INSENSITIVE:
            s = s.upper()
        if self.allowQuotes is not None:
            s = f'{self.allowQuotes}{s}{self.allowQuotes}'
        return s

    def truncateIdentifier(self, s: str, maxLen: int) -> str:
        """This truncates the string to the maximum length. This truncation will add a 3 digit hex hash
        to the end of the string. This, the string may be truncated to maxLen - 4 and then the hash is added
        with an underscore seperator."""

        if len(s) > maxLen:
            truncated = s[:maxLen - 4]
            hash: str = hashlib.sha1(s.encode()).hexdigest()[:3]  # Get the first 3 characters of the hash
            return f"{truncated}_{hash}"
        else:
            return s

    @abstractmethod
    def mapRawDatasetName(self, dp: 'DataPlatform', w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        """This maps the data set name to a physical table which may be then shared by views for each Workspace using
        that dataset for a data platform. This name should not be exposed for use by consumers. They should use the view
        instead."""
        return self.formatIdentifier(f"{dp.name}_{w.name}_{dsg.name}_{store.name}_{ds.name}")

    @abstractmethod
    def mapRawDatasetView(self, dp: 'DataPlatform', w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        """This names the workspace view name for a dataset used in a DSG. This is the actual name used by
        consumers, the view, not the underlying table holding the data"""
        return self.formatIdentifier(f"{dp.name}_{w.name}_{dsg.name}_{store.name}_{ds.name}")

    @abstractmethod
    def mapAttributeName(self, w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset', attributeName: str) -> str:
        """This maps the model attribute name in a schema to the physical attribute/column name allowed by a data container"""
        return self.formatIdentifier(attributeName)


class DefaultDataContainerNamingMapper(DataContainerNamingMapper):
    """This is a default naming adapter which maps the dataset name to the dataset name and the attribute name to the attribute name"""
    def __init__(self) -> None:
        super().__init__()

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, DefaultDataContainerNamingMapper)

    def mapRawDatasetName(self, dp: 'DataPlatform', w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        """The table which data is materialized in. This is the raw table name containing data"""
        return super().mapRawDatasetName(dp, w, dsg, store, ds)

    def mapRawDatasetView(self, dp: 'DataPlatform', w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset') -> str:
        """This is the view name which consumers should use to access the data."""
        return super().mapRawDatasetView(dp, w, dsg, store, ds)

    def mapAttributeName(self, w: 'Workspace', dsg: 'DatasetGroup', store: 'Datastore', ds: 'Dataset', attributeName: str) -> str:
        return super().mapAttributeName(w, dsg, store, ds, attributeName)


class Dataset(ANSI_SQL_NamedObject, Documentable, JSONable):
    """This is a single collection of homogeneous records with a primary key"""
    def __init__(self, name: str, *args: Union[Schema, StoragePolicy, Documentation, DeprecationInfo, DataClassification],
                 schema: Optional[Schema] = None,
                 storage_policies: Optional[list[StoragePolicy]] = None,
                 documentation: Optional[Documentation] = None,
                 deprecation_info: Optional[DeprecationInfo] = None,
                 classifications: Optional[list[DataClassification]] = None) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, documentation)
        JSONable.__init__(self)
        self.originalSchema: Optional[Schema] = None
        # Explicit policies, note these need to be added to mandatory policies for the owning GZ
        self.policies: dict[str, StoragePolicy] = OrderedDict()
        self.dataClassificationOverride: Optional[list[DataClassification]] = None
        """This is the classification of the data in the dataset. The overrides any classifications on the schema"""
        self.deprecationStatus: DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)

        # Handle new optimized parameters first if no args provided
        if not args and (schema is not None or storage_policies is not None or documentation is not None or
                         deprecation_info is not None or classifications is not None):
            # Use optimized path with named parameters
            if schema is not None:
                self.originalSchema = schema
            if storage_policies is not None:
                for policy in storage_policies:
                    if self.policies.get(policy.name) is not None:
                        raise Exception(f"Duplicate policy {policy.name}")
                    self.policies[policy.name] = policy
            if documentation is not None:
                self.documentation = documentation
            if deprecation_info is not None:
                self.deprecationStatus = deprecation_info
            if classifications is not None:
                self.dataClassificationOverride = classifications
        else:
            # Use legacy path with positional arguments
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

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Dataset):
            return ANSI_SQL_NamedObject.__eq__(self, other) and Documentable.__eq__(self, other) and \
                self.name == other.name and self.originalSchema == other.originalSchema and \
                self.policies == other.policies and \
                self.deprecationStatus == other.deprecationStatus and self.dataClassificationOverride == other.dataClassificationOverride
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

    def checkForBackwardsCompatibility(self, other: object, vTree: ValidationTree) -> bool:
        """This checks if the dataset is backwards compatible with the other dataset. This means that the other dataset
        can be used in place of this dataset. This is used to check if a dataset can be replaced by another dataset
        when a new version is released"""
        if (not isinstance(other, Dataset)):
            vTree.addRaw(ObjectWrongType(other, Dataset, ProblemSeverity.ERROR))
            return False
        super().checkForBackwardsCompatibility(other, vTree)
        if (self.originalSchema is None):
            vTree.addRaw(AttributeNotSet(f"Original schema not set for {self.name}"))
        elif (other.originalSchema is None):
            vTree.addRaw(AttributeNotSet(f"Original schema not set for {other.name}"))
        else:
            self.originalSchema.checkForBackwardsCompatibility(other.originalSchema, vTree)
        return not vTree.hasErrors()

    def __str__(self) -> str:
        return f"Dataset({self.name})"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = ANSI_SQL_NamedObject.to_json(self)
        rc.update(Documentable.to_json(self))
        rc.update({"_type": self.__class__.__name__})
        rc.update({"originalSchema": self.originalSchema.to_json() if self.originalSchema else None})
        rc.update({"policies": {k: v.to_json() for k, v in self.policies.items()}})
        rc.update({"dataClassificationOverride": [dc.to_json() for dc in self.dataClassificationOverride] if self.dataClassificationOverride else None})
        rc.update({"deprecationStatus": self.deprecationStatus.to_json()})
        return rc

    def hasClassifications(self) -> bool:
        """This returns true if the dataset has classifications for everything"""
        if (self.dataClassificationOverride):
            return True
        if (self.originalSchema and self.originalSchema.hasDataClassifications()):
            return True
        return False


class CaptureType(Enum):
    SNAPSHOT = 0
    INCREMENTAL = 1


class IngestionConsistencyType(Enum):
    """This determines whether data is ingested in consistent groups across multiple datasets or
    whether each dataset is ingested independently"""
    SINGLE_DATASET = 0
    MULTI_DATASET = 1


class StepTrigger(UserDSLObject):
    """A step such as ingestion is driven in pulses triggered by these."""
    def __init__(self, name: str):
        super().__init__()
        self.name: str = name

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc

    def __eq__(self, o: object) -> bool:
        return isinstance(o, StepTrigger) and self.name == o.name

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        pass


class CronTrigger(StepTrigger):
    """This allows the ingestion pulses to be specified using a cron string"""
    def __init__(self, name: str, cron: str):
        super().__init__(name)
        self.cron: str = cron

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        rc.update({"cron": self.cron})
        return rc

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, CronTrigger) and self.cron == o.cron

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if not validate_cron_string(self.cron):
            tree.addProblem(f"Invalid cron string <{self.cron}>")


class ExternallyTriggered(StepTrigger):
    """This is a step trigger that is triggered by an external event"""
    def __init__(self, name: str):
        super().__init__(name)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, ExternallyTriggered)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        pass


class DataContainer(Documentable, JSONable):
    """This is a container for data. It's a logical container. The data can be physically stored in
    one or more locations through replication or fault tolerance measures. It is owned by a data platform
    and is used to determine whether a dataset is compatible with the container by a governancezone."""
    def __init__(self, name: str, *args: Union[set['LocationKey'], Documentation]) -> None:
        Documentable.__init__(self, None)
        JSONable.__init__(self)
        self.locations: set[LocationKey] = set()
        self.name: str = name
        self.serverSideEncryptionKeys: Optional[EncryptionSystem] = None
        """This is the vendor ecnryption system providing the container. For example, if a cloud vendor
        hosts the container, do they have access to the container data?"""
        self.clientSideEncryptionKeys: Optional[EncryptionSystem] = None
        """This is the encryption system used by the client to encrypt data before sending to the container. This could be used
        to encrypt data before sending to a cloud vendor for example"""
        self.isReadOnly: bool = False
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = Documentable.to_json(self)
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        rc.update({"locations": [loc.to_json() for loc in self.locations]})
        rc.update({"serverSideEncryptionKeys": self.serverSideEncryptionKeys.to_json() if self.serverSideEncryptionKeys else None})
        rc.update({"clientSideEncryptionKeys": self.clientSideEncryptionKeys.to_json() if self.clientSideEncryptionKeys else None})
        rc.update({"isReadOnly": self.isReadOnly})
        if (self.documentation):
            rc.update({"documentation": self.documentation.to_json()})
        return rc

    def add(self, *args: Union[set['LocationKey'], Documentation]) -> None:
        for arg in args:
            if (isinstance(arg, set)):
                for loc in arg:
                    if (loc in self.locations):
                        raise Exception(f"Duplicate Location {loc}")
                    self.locations.add(loc)
            else:
                self.documentation = arg

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DataContainer):
            return self.name == other.name and self.locations == other.locations and \
                self.serverSideEncryptionKeys == other.serverSideEncryptionKeys and \
                self.clientSideEncryptionKeys == other.clientSideEncryptionKeys and \
                self.isReadOnly == other.isReadOnly
        else:
            return False

    def getName(self) -> str:
        """Returns the name of the container"""
        return self.name

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if (self.documentation):
            dTree: ValidationTree = tree.addSubTree(self.documentation)
            self.documentation.lint(dTree)

        for loc in self.locations:
            ltree: ValidationTree = tree.addSubTree(loc)
            loc.lint(ltree)
            eco.lintLocationKey(loc, ltree)

    def __hash__(self) -> int:
        return hash(self.name)

    def areLocationsOwnedByTheseVendors(self, eco: 'Ecosystem', vendors: set[CloudVendor]) -> bool:
        """Returns true if the container only uses locations managed by the provided set of cloud vendors"""
        for lkey in self.locations:
            loc: Optional[InfrastructureLocation] = eco.getAsInfraLocation(lkey)
            if (loc is None or loc.key is None):
                return False
            v: InfrastructureVendor = eco.getVendorOrThrow(loc.key.ivName)
            if v.hardCloudVendor not in vendors:
                return False
        return True

    def areAllLocationsInLocations(self, locations: set['LocationKey']) -> bool:
        """Returns true if all locations are in the provided set of locations"""
        for lkey in self.locations:
            if lkey not in locations:
                return False
        return True

    @abstractmethod
    def getNamingAdapter(self) -> Optional[DataContainerNamingMapper]:
        """This returns a naming adapter which can be used to map dataset names and attributes to the underlying data container"""
        return None


class DataPlatformManagedDataContainer(DataContainer):
    """This is a data container that is managed by a data platform. This is used on Workspaces to specify a DataContainer that is provided by
    the DataPlatform assigned to the Workspace. Some DataPlatforms may only support this type of container if they do not support pushing data
    to different explicit data containers for Workspaces, for example, a consumer wants the data pushed to an existing database where they want
    the data along with other data they have in that database."""
    def __init__(self, name: str) -> None:
        super().__init__(name, set())

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, DataPlatformManagedDataContainer)):
            return super().__eq__(other)
        return False

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return f"DataPlatformManagedDataContainer({self.name})"

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        return

    def getNamingAdapter(self) -> Optional[DataContainerNamingMapper]:
        return None


class SQLDatabase(DataContainer):
    """A generic SQL Database data container"""
    def __init__(self, name: str, locations: set['LocationKey'], databaseName: str) -> None:
        super().__init__(name, locations)
        self.databaseName: str = databaseName

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "databaseName": self.databaseName})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, SQLDatabase)):
            return super().__eq__(other) and self.databaseName == other.databaseName
        return False

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)

    def getNamingAdapter(self) -> Optional[DataContainerNamingMapper]:
        return DefaultDataContainerNamingMapper()


class URLSQLDatabase(SQLDatabase):
    """This is a SQL database with a URL"""
    def __init__(self, name: str, locations: set['LocationKey'], url: str, databaseName: str) -> None:
        super().__init__(name, locations, databaseName)
        self.url: str = url

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "url": self.url})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, URLSQLDatabase)):
            return super().__eq__(other) and self.url == other.url
        return False

    def __hash__(self) -> int:
        return hash(self.name)


class HostPortPair(UserDSLObject):
    """This represents a host and port pair"""
    def __init__(self, hostName: str, port: int) -> None:
        UserDSLObject.__init__(self)
        self.hostName: str = hostName
        self.port: int = port

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "hostName": self.hostName, "port": self.port})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, HostPortPair)):
            return self.hostName == other.hostName and self.port == other.port
        return False

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return f"{self.hostName}:{self.port}"

    def lint(self, tree: ValidationTree) -> None:
        if not is_valid_hostname_or_ip(self.hostName):
            tree.addRaw(NameHasBadSynthax(f"Host '{self.hostName}' is not a valid hostname or IP address"))
        if self.port < 0 or self.port > 65535:
            tree.addProblem(f"Port {self.port} is not a valid port number")


class HostPortPairList(UserDSLObject):
    """This is a list of host port pairs"""
    def __init__(self, pairs: list[HostPortPair]) -> None:
        UserDSLObject.__init__(self)
        self.pairs: list[HostPortPair] = pairs

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "pairs": [p.to_json() for p in self.pairs]})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, HostPortPairList)):
            return self.pairs == other.pairs
        return False

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return ", ".join([str(p) for p in self.pairs])

    def lint(self, tree: ValidationTree) -> None:
        for pair in self.pairs:
            pair.lint(tree.addSubTree(pair))


class HostPortSQLDatabase(SQLDatabase):
    """This is a SQL database with a host and port"""
    def __init__(self, name: str, locations: set['LocationKey'], hostPort: HostPortPair, databaseName: str) -> None:
        super().__init__(name, locations, databaseName)
        self.hostPortPair: HostPortPair = hostPort

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "hostPort": self.hostPortPair.to_json()})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, HostPortSQLDatabase)):
            return super().__eq__(other) and self.hostPortPair == other.hostPortPair
        return False

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        self.hostPortPair.lint(tree.addSubTree(self.hostPortPair))


class PostgresDatabase(HostPortSQLDatabase):
    """This is a Postgres database"""
    def __init__(self, name: str, hostPort: HostPortPair, locations: set['LocationKey'], databaseName: str) -> None:
        super().__init__(name, locations, hostPort, databaseName)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, PostgresDatabase)):
            return super().__eq__(other)
        return False

    def __hash__(self) -> int:
        return hash(self.name)


class MySQLDatabase(HostPortSQLDatabase):
    """This is a MySQL database"""
    def __init__(self, name: str, hostPort: HostPortPair, locations: set['LocationKey'], databaseName: str) -> None:
        super().__init__(name, locations, hostPort, databaseName)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, MySQLDatabase)):
            return super().__eq__(other)
        return False

    def __hash__(self) -> int:
        return hash(self.name)


class OracleDatabase(HostPortSQLDatabase):
    """This is an Oracle database"""
    def __init__(self, name: str, hostPort: HostPortPair, locations: set['LocationKey'], databaseName: str) -> None:
        super().__init__(name, locations, hostPort, databaseName)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, OracleDatabase)):
            return super().__eq__(other)
        return False

    def __hash__(self) -> int:
        return hash(self.name)


class SQLServerDatabase(HostPortSQLDatabase):
    """This is a SQL Server database"""
    def __init__(self, name: str, hostPort: HostPortPair, locations: set['LocationKey'], databaseName: str) -> None:
        super().__init__(name, locations, hostPort, databaseName)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, SQLServerDatabase)):
            return super().__eq__(other)
        return False

    def __hash__(self) -> int:
        return hash(self.name)


class DB2Database(HostPortSQLDatabase):
    """This is a DB2 database"""
    def __init__(self, name: str, hostPort: HostPortPair, locations: set['LocationKey'], databaseName: str) -> None:
        super().__init__(name, locations, hostPort, databaseName)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, DB2Database)):
            return super().__eq__(other)
        return False

    def __hash__(self) -> int:
        return hash(self.name)


class ObjectStorage(DataContainer):
    """Generic Object storage service. Flat file storage"""
    def __init__(self, name: str, locs: set['LocationKey'], endPointURI: Optional[str], bucketName: str, prefix: Optional[str]):
        super().__init__(name, locs)
        self.endPointURI: Optional[str] = endPointURI
        self.bucketName: str = bucketName
        self.prefix: Optional[str] = prefix

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "endPointURI": self.endPointURI, "bucketName": self.bucketName, "prefix": self.prefix})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, ObjectStorage)):
            return super().__eq__(other) and self.endPointURI == other.endPointURI and self.bucketName == other.bucketName and self.prefix == other.prefix
        return False


class PyOdbcSourceInfo(SQLDatabase):
    """This describes how to connect to a database using pyodbc"""
    def __init__(self, name: str, locs: set['LocationKey'], serverHost: str, databaseName: str, driver: str, connectionStringTemplate: str) -> None:
        super().__init__(name, locs, databaseName)
        self.serverHost: str = serverHost
        self.driver: str = driver
        self.connectionStringTemplate: str = connectionStringTemplate

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "serverHost": self.serverHost,
                   "driver": self.driver, "connectionStringTemplate": self.connectionStringTemplate})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is PyOdbcSourceInfo and self.serverHost == other.serverHost and \
            self.databaseName == other.databaseName and self.driver == other.driver and self.connectionStringTemplate == other.connectionStringTemplate

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        super().lint(eco, tree)
# TODO validate the server string, its not just a host name
#        if (not is_valid_hostname_or_ip(self.serverHost)):
#            tree.addProblem(f"Server host {self.serverHost} is not a valid hostname or IP address")

    def __str__(self) -> str:
        return f"PyOdbcSourceInfo({self.serverHost})"


class CaptureMetaData(UserDSLObject):
    """This describes how a platform can pull data for a Datastore"""

    def __init__(self, *args: Union[StepTrigger, DataContainer, IngestionConsistencyType]):
        UserDSLObject.__init__(self)
        self.singleOrMultiDatasetIngestion: Optional[IngestionConsistencyType] = None
        self.stepTrigger: Optional[StepTrigger] = None
        self.dataContainer: Optional[DataContainer] = None
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        rc.update({"singleOrMultiDatasetIngestion": self.singleOrMultiDatasetIngestion.value if self.singleOrMultiDatasetIngestion else None})
        rc.update({"stepTrigger": self.stepTrigger.to_json() if self.stepTrigger else None})
        rc.update({"dataContainer": self.dataContainer.to_json() if self.dataContainer else None})
        return rc

    def add(self, *args: Union[StepTrigger, DataContainer, IngestionConsistencyType]) -> None:
        for arg in args:
            if (isinstance(arg, StepTrigger)):
                if (self.stepTrigger is not None):
                    raise AttributeAlreadySetException("CaptureTrigger already set")
                self.stepTrigger = arg
            elif (isinstance(arg, IngestionConsistencyType)):
                if (self.singleOrMultiDatasetIngestion is not None):
                    raise AttributeAlreadySetException("SingleOrMultiDatasetIngestion already set")
                self.singleOrMultiDatasetIngestion = arg
            else:
                if (self.dataContainer is not None):
                    raise AttributeAlreadySetException("Container already set")
                self.dataContainer = arg

    @abstractmethod
    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        if (self.singleOrMultiDatasetIngestion is None):
            tree.addRaw(AttributeNotSet("Single Or Multi ingestion not specified"))

        if (self.dataContainer is None):
            # The container is implicit when its a DataTransformer (same as the Workspace container)
            if (not isinstance(self, DataTransformerOutput)):
                tree.addRaw(AttributeNotSet("Container not specified"))
            tree.addRaw(AttributeNotSet("Container not specified"))
        else:
            cTree: ValidationTree = tree.addSubTree(self.dataContainer)
            self.dataContainer.lint(eco, cTree)

        if (self.stepTrigger):
            self.stepTrigger.lint(eco, tree)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, CaptureMetaData) and self.singleOrMultiDatasetIngestion == other.singleOrMultiDatasetIngestion and \
            self.stepTrigger == other.stepTrigger and self.dataContainer == other.dataContainer

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

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DataTransformerOutput) and self.workSpaceName == other.workSpaceName


class IngestionMetadata(CaptureMetaData):
    """Producers use these to describe HOW to snapshot and pull deltas from a data source in to
    data pipelines. The ingestion service interprets these to allow code free ingestion from
    supported sources and handle operation pipelines."""
    def __init__(self, *args: Union[DataContainer, Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__()
        self.credential: Optional[Credential] = None
        self.add(*args)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "credential": self.credential.to_json() if self.credential else None})
        return rc

    def add(self, *args: Union[Credential, DataContainer, StepTrigger, IngestionConsistencyType]) -> None:
        for arg in args:
            if (isinstance(arg, Credential)):
                c: Credential = arg
                if (self.credential is not None):
                    raise AttributeAlreadySetException("Credential already set")
                self.credential = c
            else:
                super().add(arg)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, IngestionMetadata):
            return super().__eq__(other) and self.credential == other.credential
        return False

    @abstractmethod
    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if (self.dataContainer):
            capTree: ValidationTree = tree.addSubTree(self.dataContainer)
            self.dataContainer.lint(eco, capTree)
        # Credential is needed for a platform connect to a datacontainer and ingest data
        # But, Credentials are linted by the DataPlatform using its CredentialStore
        super().lint(eco, gz, t, d, tree)


class CDCCaptureIngestion(IngestionMetadata):
    """This indicates CDC can be used to capture deltas from the source"""
    def __init__(self, dc: DataContainer, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(dc, *args)

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, d, tree)

    def __str__(self) -> str:
        return "CDCCaptureIngestion()"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is CDCCaptureIngestion


class SQLIngestion(IngestionMetadata):
    """This is an abstract class for SQL ingestion. It allows a dataset to table name mapping to be specified.
    If its not specified then the dataset name is used as the table name"""
    def __init__(self, db: SQLDatabase, *args: Union[Credential, StepTrigger, IngestionConsistencyType, dict[str, str]]) -> None:
        super().__init__(db, *[arg for arg in args if not isinstance(arg, dict)])
        self.tableForDataset: dict[str, str] = {}

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "tableForDataset": self.tableForDataset})
        return rc

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SQLIngestion):
            return super().__eq__(other) and self.tableForDataset == other.tableForDataset
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, d, tree)
        # Check every dataset in the datastore has a table name specified if any mappings are specified, all or none
        if len(self.tableForDataset) > 0:
            # Check all values in the mapping are valid SQL table names
            for table in self.tableForDataset.values():
                if not is_valid_sql_identifier(table):
                    tree.addRaw(NameMustBeSQLIdentifier(table, ProblemSeverity.ERROR))

            for dataset in d.datasets.values():
                if dataset.name not in self.tableForDataset:
                    tree.addRaw(AttributeNotSet(f"Dataset {dataset.name} has no table name specified"))

    def __str__(self) -> str:
        return "SQLIngestion()"


class SQLSnapshotIngestion(SQLIngestion):
    """This is an SQL ingestion which does a select * from each table every batch."""
    def __init__(self, db: SQLDatabase, *args: Union[Credential, StepTrigger, IngestionConsistencyType, dict[str, str]]) -> None:
        super().__init__(db, *args)

    def __str__(self) -> str:
        return "SQLSnapshotIngestion()"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, SQLSnapshotIngestion)

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, d, tree)


class SQLSnapshotDeltaIngestion(SQLIngestion):
    """This IMD describes how to pull a snapshot 'dump' from each dataset and then persist
    state variables which are used to next pull a delta per dataset and then persist the state
    again so that another delta can be pulled on the next pass and so on"""
    def __init__(self, db: SQLDatabase, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(db, *args)
        self.variableNames: list[str] = []
        """The names of state variables produced by snapshot and delta sql strings"""
        self.snapshotSQL: dict[str, str] = OrderedDict()
        """A SQL string per dataset which pulls a per table snapshot"""
        self.deltaSQL: dict[str, str] = OrderedDict()
        """A SQL string per dataset which pulls all rows which changed since last time for a table"""

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "variableNames": self.variableNames, "snapshotSQL": self.snapshotSQL, "deltaSQL": self.deltaSQL})
        return rc

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SQLSnapshotDeltaIngestion):
            return super().__eq__(other) and self.variableNames == other.variableNames and \
                self.snapshotSQL == other.snapshotSQL and self.deltaSQL == other.deltaSQL
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, d, tree)
        # It would be nice to check the SQL strings are valid but that's a lot of work and we're not doing it yet.
        # We'll just check the variable names are valid SQL identifiers
        for var in self.variableNames:
            if not is_valid_sql_identifier(var):
                tree.addRaw(NameMustBeSQLIdentifier(var, ProblemSeverity.ERROR))

    def __str__(self) -> str:
        return "SQLSnapshotDeltaIngestion()"


class StreamingIngestion(IngestionMetadata):
    """This is an abstract class for streaming data sources. It allows a dataset to topic name mapping to be specified.
    If its not specified then the dataset name is used as the topic name"""
    def __init__(self, dc: DataContainer, *args: Union[Credential, StepTrigger, IngestionConsistencyType, dict[str, str]]) -> None:
        # Pass all args except any which are dict[str, str]
        super().__init__(dc, *[arg for arg in args if not isinstance(arg, dict)])
        self.topicForDataset: dict[str, str] = {}
        for arg in args:
            if isinstance(arg, dict):
                self.topicForDataset.update(arg)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "topicForDataset": self.topicForDataset})
        return rc

    def __eq__(self, other: object) -> bool:
        if isinstance(other, StreamingIngestion):
            return super().__eq__(other) and self.topicForDataset == other.topicForDataset
        return False


class KafkaServer(DataContainer):
    """This represents a connection to a Kafka Server."""
    def __init__(self, name: str, locs: set['LocationKey'], bootstrapServers: HostPortPairList,
                 groupID: Optional[str] = None, caCert: Optional[Credential] = None) -> None:
        super().__init__(name, locs)
        self.bootstrapServers: HostPortPairList = bootstrapServers
        self.groupID: Optional[str] = groupID
        self.caCertificate: Optional[Credential] = caCert

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__,
                   "bootstrapServers": self.bootstrapServers.to_json(),
                   "groupID": self.groupID,
                   "caCertificate": self.caCertificate.to_json() if self.caCertificate else None})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, KafkaServer) and self.bootstrapServers == other.bootstrapServers and \
            self.caCertificate == other.caCertificate and self.groupID == other.groupID

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        super().lint(eco, tree)
        self.bootstrapServers.lint(tree.addSubTree(self.bootstrapServers))

    def __str__(self) -> str:
        return f"KafkaServer({self.bootstrapServers})"

    def getNamingAdapter(self) -> Optional[DataContainerNamingMapper]:
        """This returns a naming adapter which can be used to map dataset names and attributes to the underlying data container"""
        return None


class KafkaIngestion(StreamingIngestion):
    """This allows a topic and a schema format to be specified for a source publishing messages to a Kafka topic"""
    def __init__(self, kafkaServer: KafkaServer, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(kafkaServer, *args)
        self.kafkaServer: KafkaServer = kafkaServer

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, KafkaIngestion) and self.kafkaServer == other.kafkaServer

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', d: 'Datastore', tree: ValidationTree) -> None:
        super().lint(eco, gz, t, d, tree)
        kTree: ValidationTree = tree.addSubTree(self.kafkaServer)
        self.kafkaServer.lint(eco, kTree)


class DatasetPerTopicKafkaIngestion(KafkaIngestion):
    """This is a KafkaIngestion which uses a separate topic for each dataset"""
    def __init__(self, kafkaServer: KafkaServer, *args: Union[Credential, StepTrigger, IngestionConsistencyType]) -> None:
        super().__init__(kafkaServer, *args)


class DatasetDSGApproval(UserDSLObject):
    """This is simply a record that a Workspace/DSG was approved to use a specific dataset in a datastore."""
    def __init__(self, workspace: str, dsg: str, datasetName: str) -> None:
        UserDSLObject.__init__(self)
        self.workspace: str = workspace
        self.dsg: str = dsg
        self.datasetName: str = datasetName

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "workspace": self.workspace, "dsg": self.dsg, "datasetName": self.datasetName}

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DatasetDSGApproval):
            return self.workspace == other.workspace and self.dsg == other.dsg and self.datasetName == other.datasetName
        return False

    def __str__(self) -> str:
        return f"DatasetDSGApproval({self.workspace}, {self.dsg}, {self.datasetName})"

    def __hash__(self) -> int:
        return hash(f"{self.workspace}#{self.dsg}#{self.datasetName}")

    def lint(self, eco: 'Ecosystem', store: 'Datastore', tree: ValidationTree) -> None:
        # Nothing to do, approvals can be added before the Workspace/DSG/Dataset exists. We run the risk
        # here of typos causing delays with approvals but this can't be avoided as team A using repo A may
        # own the datastore and team B using a different repo may want to use it. Permissions on Ecosystem
        # model updates mean the approval has to be added in a commit before the Workspace/DSG/Dataset exists.
        pass


class Datastore(ANSI_SQL_NamedObject, Documentable, JSONable):

    """This is a named group of datasets. It describes how to capture the data and make it available for processing"""
    def __init__(self, name: str,
                 *args: Union[Dataset, CaptureMetaData, Documentation, ProductionStatus, DeprecationInfo, DatasetDSGApproval],
                 datasets: Optional[list[Dataset]] = None,
                 capture_metadata: Optional[CaptureMetaData] = None,
                 documentation: Optional[Documentation] = None,
                 production_status: Optional[ProductionStatus] = None,
                 deprecation_info: Optional[DeprecationInfo] = None,
                 datasetDSGApprovals: Optional[set[DatasetDSGApproval]] = None) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, documentation)
        JSONable.__init__(self)
        self.datasets: dict[str, Dataset] = OrderedDict()

        # If none then approval are not required. If even an empty list then approval is required.
        self.datasetDSGApprovals: Optional[set[DatasetDSGApproval]] = datasetDSGApprovals
        self.key: Optional[DatastoreKey] = None
        self.cmd: Optional[CaptureMetaData] = capture_metadata
        self.productionStatus: ProductionStatus = production_status if production_status is not None else ProductionStatus.NOT_PRODUCTION
        self.deprecationStatus: DeprecationInfo = deprecation_info if deprecation_info is not None else DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)
        """Deprecating a store deprecates all datasets in the store regardless of their deprecation status"""

        # Process named parameters first
        if datasets is not None:
            for dataset in datasets:
                if self.datasets.get(dataset.name) is not None:
                    raise ObjectAlreadyExistsException(f"Duplicate Dataset {dataset.name}")
                self.datasets[dataset.name] = dataset

        if capture_metadata is not None:
            self.cmd = capture_metadata

        if documentation is not None:
            self.documentation = documentation

        if production_status is not None:
            self.productionStatus = production_status

        if deprecation_info is not None:
            self.deprecationStatus = deprecation_info

        # Process *args using existing add method
        self.add(*args)

    def isDatasetDSGApproved(self, workspace: str, dsg: str, datasetName: str) -> bool:
        """Returns true if the dataset/dsg is approved"""
        # If approvals are not required, allow all access
        if self.datasetDSGApprovals is None:
            return True

        # For better performance with large approval lists, we could maintain a set
        # For now, keeping the simple implementation but with proper iteration
        key: DatasetDSGApproval = DatasetDSGApproval(workspace, dsg, datasetName)
        return key in self.datasetDSGApprovals

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = ANSI_SQL_NamedObject.to_json(self)
        rc.update(Documentable.to_json(self))
        rc.update({"_type": self.__class__.__name__})
        rc.update({"datasets": {k: v.to_json() for k, v in self.datasets.items()}})
        if (self.key is not None):
            rc.update({"team": self.key.tdName, "governance_zone": self.key.gzName})
        rc.update({"cmd": self.cmd.to_json() if self.cmd else None})
        if (self.documentation):
            rc.update({"doc": self.documentation.to_json()})
        rc.update({"productionStatus": self.productionStatus.name})
        rc.update({"deprecationStatus": self.deprecationStatus.to_json()})
        if (self.datasetDSGApprovals is not None):
            rc.update({"datasetDSGApprovals": [a.to_json() for a in self.datasetDSGApprovals]})
        return rc

    def setTeam(self, tdKey: TeamDeclarationKey):
        self.key = DatastoreKey(tdKey, self.name)

    def add(self, *args: Union[Dataset, CaptureMetaData, Documentation, ProductionStatus, DeprecationInfo, DatasetDSGApproval]) -> None:
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
            elif (isinstance(arg, ProductionStatus)):
                self.productionStatus = arg
            elif (isinstance(arg, DeprecationInfo)):
                self.deprecationStatus = arg
            elif (isinstance(arg, DatasetDSGApproval)):
                # Initialize approvals list if it doesn't exist
                if self.datasetDSGApprovals is None:
                    self.datasetDSGApprovals = set()
                self.datasetDSGApprovals.add(arg)
            elif (isinstance(arg, Documentation)):
                doc: Documentation = arg
                self.documentation = doc

    def isDatasetDeprecated(self, dataset: Dataset) -> bool:
        """Returns true if the datastore is deprecated OR dataset is deprecated"""
        return self.deprecationStatus.status == DeprecationStatus.DEPRECATED or dataset.deprecationStatus.status == DeprecationStatus.DEPRECATED

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Datastore):
            return ANSI_SQL_NamedObject.__eq__(self, other) and Documentable.__eq__(self, other) and \
                self.datasets == other.datasets and self.cmd == other.cmd and \
                self.productionStatus == other.productionStatus and self.deprecationStatus == other.deprecationStatus and \
                self.key == other.key and self.datasetDSGApprovals == other.datasetDSGApprovals
        return False

    def lint(self, eco: 'Ecosystem', gz: 'GovernanceZone', t: 'Team', storeTree: ValidationTree) -> None:
        self.nameLint(storeTree)
        if (self.key is None):
            storeTree.addRaw(AttributeNotSet(f"{self} has no key"))
        if (self.documentation):
            self.documentation.lint(storeTree)
        for dataset in self.datasets.values():
            dTree: ValidationTree = storeTree.addSubTree(dataset)
            dataset.lint(eco, gz, t, self, dTree)
            if (self.productionStatus == ProductionStatus.PRODUCTION):
                if (not dataset.hasClassifications()):
                    dTree.addRaw(ProductionDatastoreMustHaveClassifications(self, dataset))

        if (self.cmd):
            cmdTree: ValidationTree = storeTree.addSubTree(self.cmd)
            self.cmd.lint(eco, gz, t, self, cmdTree)
        else:
            storeTree.addRaw(AttributeNotSet("CaptureMetaData not set"))
        if (len(self.datasets) == 0):
            storeTree.addRaw(AttributeNotSet("No datasets in store"))
        if (self.datasetDSGApprovals is not None):
            for approval in self.datasetDSGApprovals:
                approval.lint(eco, self, storeTree.addSubTree(approval))

    def checkForBackwardsCompatibility(self, other: object, vTree: ValidationTree) -> bool:
        """This checks if the other datastore is backwards compatible with this one. This means that the other datastore
        can be used to replace this one without breaking any data pipelines"""

        if (not isinstance(other, Datastore)):
            vTree.addRaw(ObjectWrongType(other, Datastore, ProblemSeverity.ERROR))
            return False
        super().checkForBackwardsCompatibility(other, vTree)
        # Check if the datasets are compatible
        for dataset in self.datasets.values():
            dTree: ValidationTree = vTree.addSubTree(dataset)
            otherDataset: Optional[Dataset] = other.datasets.get(dataset.name)
            if (otherDataset):
                dataset.checkForBackwardsCompatibility(otherDataset, dTree)
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

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, TeamCacheEntry)):
            return self.team == other.team and self.declaration == other.declaration
        return False


class WorkspaceCacheEntry:
    """This is used by Ecosystem to cache workspaces"""
    def __init__(self, w: 'Workspace', t: 'Team') -> None:
        self.workspace: Workspace = w
        self.team: Team = t

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, WorkspaceCacheEntry)):
            return self.workspace == other.workspace and self.team == other.team
        return False


class DatastoreCacheEntry:
    """This is used by Ecosystem to cache datastores"""
    def __init__(self, d: 'Datastore', t: 'Team') -> None:
        self.datastore: Datastore = d
        self.team: Team = t

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, DatastoreCacheEntry)):
            return self.datastore == other.datastore and self.team == other.team
        return False


class DependentWorkspaces(JSONable):
    """This tracks a Workspaces dependent on a datastore"""
    def __init__(self, workSpace: 'Workspace'):
        JSONable.__init__(self)
        self.workspace: Workspace = workSpace
        self.dependencies: set[DependentWorkspaces] = set()

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "workspaceName": self.workspace.name, "dependencies": [dep.to_json() for dep in self.dependencies]}

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
            return super().__eq__(__value) and self.workspace.name == __value.workspace.name and self.dependencies == __value.dependencies
        else:
            return False


class DefaultDataPlatform(UserDSLObject):
    def __init__(self, p: 'DataPlatformKey'):
        UserDSLObject.__init__(self)
        self.defaultPlatform: 'DataPlatformKey' = p

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "defaultPlatform": self.defaultPlatform.name})
        return rc

    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        """Lint just checks the platform exists, the platform will be linted seperatedly"""
        if (eco.getDataPlatform(self.defaultPlatform.name) is None):
            tree.addRaw(AttributeNotSet("Default Data Platform not set"))
        else:
            dp: Optional['DataPlatform'] = eco.getDataPlatform(self.defaultPlatform.name)
            if dp is None:
                tree.addRaw(AttributeNotSet("Default Data Platform not set"))

    def get(self, eco: 'Ecosystem') -> 'DataPlatform':
        """This returns the default DataPlatform or throws an Exception if it has not been specified"""
        return eco.getDataPlatformOrThrow(self.defaultPlatform.name)

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, DefaultDataPlatform)):
            return self.defaultPlatform == other.defaultPlatform
        return False


class PlatformService(JSONable):
    def __init__(self, name: str):
        JSONable.__init__(self)
        self.name: str = name

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "name": self.name}


class PlatformServicesProvider(UserDSLObject):
    def __init__(self, name: str, credStore: CredentialStore):
        UserDSLObject.__init__(self)
        self.name: str = name
        self.credStore: CredentialStore = credStore

    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__, "name": self.name}

    def render(self):
        pass

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree):
        self.credStore.lint(tree.addSubTree(self.credStore))
        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)
        graph.lint(self.credStore, tree.addSubTree(graph))

    @abstractmethod
    def mergeHandler(self, eco: 'Ecosystem'):
        """This is the merge handler implementation."""
#        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)
        pass


# Add regulators here with their named retention policies for reference in Workspaces
# Feels like regulators are across GovernanceZones
class Ecosystem(GitControlledObject, JSONable):

    def createGZone(self, name: str, repo: Repository) -> 'GovernanceZone':
        gz: GovernanceZone = GovernanceZone(name, repo)
        gz.setEcosystem(self)
        return gz

    def __init__(self, name: str, repo: Repository,
                 *args: Union[PlatformServicesProvider,
                              'DataPlatform', Documentation, DefaultDataPlatform,
                              InfrastructureVendor, 'GovernanceZoneDeclaration'],
                 platform_services_provider: Optional[PlatformServicesProvider] = None,
                 data_platforms: Optional[list['DataPlatform']] = None,
                 documentation: Optional[Documentation] = None,
                 default_data_platform: Optional[DefaultDataPlatform] = None,
                 infrastructure_vendors: Optional[list[InfrastructureVendor]] = None,
                 governance_zone_declarations: Optional[list['GovernanceZoneDeclaration']] = None) -> None:
        GitControlledObject.__init__(self, repo)
        JSONable.__init__(self)
        self.name: str = name
        self.key: EcosystemKey = EcosystemKey(self.name)

        self.zones: AuthorizedObjectManager[GovernanceZone, GovernanceZoneDeclaration] = \
            AuthorizedObjectManager[GovernanceZone, GovernanceZoneDeclaration]("zones", lambda name, repo: self.createGZone(name, repo), repo)
        """This is the authorative list of governance zones within the ecosystem"""

        self.vendors: dict[str, InfrastructureVendor] = OrderedDict[str, InfrastructureVendor]()
        self.dataPlatforms: dict[str, DataPlatform] = OrderedDict[str, DataPlatform]()
        self.defaultDataPlatform: Optional[DefaultDataPlatform] = None
        self.platformServicesProvider: Optional[PlatformServicesProvider] = None
        self.dsgPlatformMappings: dict[str, DatasetGroupDataPlatformAssignments] = dict[str, DatasetGroupDataPlatformAssignments]()
        self.resetCaches()

        # Handle backward compatibility: if *args are provided, parse them the old way
        if args:
            # Legacy mode: parse *args (slower but compatible)
            self.add(*args)
        else:
            # New mode: use named parameters directly (faster!)
            if platform_services_provider:
                self.platformServicesProvider = platform_services_provider

            if data_platforms:
                for platform in data_platforms:
                    if self.dataPlatforms.get(platform.name) is not None:
                        raise ObjectAlreadyExistsException(f"Duplicate DataPlatform {platform.name}")
                    self.dataPlatforms[platform.name] = platform

            if documentation:
                self.documentation = documentation

            if default_data_platform:
                self.defaultDataPlatform = default_data_platform

            if infrastructure_vendors:
                for vendor in infrastructure_vendors:
                    if self.vendors.get(vendor.name) is not None:
                        raise ObjectAlreadyExistsException(f"Duplicate Vendor {vendor.name}")
                    self.vendors[vendor.name] = vendor

            if governance_zone_declarations:
                for zone_declaration in governance_zone_declarations:
                    self.zones.addAuthorization(zone_declaration)
                    zone_declaration.key = GovernanceZoneKey(self.key, zone_declaration.name)

        self.resetKey()

    def checkObjectIsSupported(self, obj: object, types: list[type], tree: ValidationTree) -> None:
        """This checks that the object is one of the specified types only."""
        if not isinstance(obj, tuple(types)):
            tree.addRaw(ObjectNotSupportedByDataPlatform(obj, types, ProblemSeverity.ERROR))

    def checkAllRepositoriesInEcosystem(self, tree: ValidationTree, types: list[type]) -> None:
        """This checks that all repositories in the ecosystem are one of the specified types only."""
        self.checkObjectIsSupported(self.owningRepo, types, tree)

        # Check each GovernanceZone
        zone: GovernanceZone
        for zone in self.zones.defineAllObjects():
            self.checkObjectIsSupported(zone.owningRepo, types, tree.addSubTree(zone))

            # Check each Team
            team: Team
            for team in zone.teams.defineAllObjects():
                self.checkObjectIsSupported(team.owningRepo, types, tree.addSubTree(team))

    def hydrateDSGDataPlatformMappings(self, jsonFile: str, tree: ValidationTree) -> None:
        """This uses the file dsg_platform_mapping.json to hydrate the dsgPlatformMappings set"""
        import json
        from datasurface.md.documentation import PlainTextDocumentation

        # If the file doesn't exist, there is no mapping.
        if not os.path.exists(jsonFile):
            return

        # If there is an exception during the load then add a raw error to the tree
        try:
            with open(jsonFile, "r") as f:
                mappings: list[dict[str, Any]] = json.load(f)
                for dsg_mapping in mappings:
                    # Extract required fields
                    dsg_name: str = dsg_mapping["dsgName"]
                    workspace: str = dsg_mapping.get("workspace", "default_workspace")
                    assignments: list[Any] = dsg_mapping.get("assignments", [])
                    dsg_assignments: list[DSGDataPlatformAssignment] = []
                    for assignment in assignments:
                        dsg_assignment = DSGDataPlatformAssignment(
                            workspace=workspace,
                            dsgName=dsg_name,
                            dp=DataPlatformKey(assignment["dataPlatform"]),
                            doc=PlainTextDocumentation(assignment["documentation"]),
                            productionStatus=ProductionStatus[assignment["productionStatus"]],
                            deprecationsAllowed=DeprecationsAllowed[assignment["deprecationsAllowed"]],
                            status=DatasetGroupDataPlatformMappingStatus[assignment["status"]]
                        )
                        dsg_assignments.append(dsg_assignment)

                    # Create the assignments container

                    self.dsgPlatformMappings[f"{workspace}#{dsg_name}"] = DatasetGroupDataPlatformAssignments(
                        workspace=workspace,
                        dsgName=dsg_name,
                        assignments=dsg_assignments
                    )
            # Now lint the dsg platform mappings
            if self.dsgPlatformMappings:
                for dsg_mapping in self.dsgPlatformMappings.values():
                    dsg_mapping.lint(self, tree.addSubTree(dsg_mapping))
        except Exception as e:
            tree.addRaw(UnexpectedExceptionProblem(e))
            self.dsgPlatformMappings.clear()

    def getDSGPlatformMapping(self, workspaceName: str, dsgName: str) -> Optional['DatasetGroupDataPlatformAssignments']:
        return self.dsgPlatformMappings.get(f"{workspaceName}#{dsgName}")

    @classmethod
    def create_legacy(cls, name: str, repo: Repository,
                      *args: Union[PlatformServicesProvider,
                                   'DataPlatform', Documentation, DefaultDataPlatform,
                                   InfrastructureVendor, 'GovernanceZoneDeclaration']) -> 'Ecosystem':
        """Legacy factory method for backward compatibility with old *args pattern.
        Use this temporarily during migration, then switch to named parameters for better performance."""
        platform_services_provider: Optional[PlatformServicesProvider] = None
        data_platforms: list['DataPlatform'] = []
        documentation: Optional[Documentation] = None
        default_data_platform: Optional[DefaultDataPlatform] = None
        infrastructure_vendors: list[InfrastructureVendor] = []
        governance_zone_declarations: list['GovernanceZoneDeclaration'] = []

        for arg in args:
            if isinstance(arg, InfrastructureVendor):
                infrastructure_vendors.append(arg)
            elif isinstance(arg, PlatformServicesProvider):
                platform_services_provider = arg
            elif isinstance(arg, Documentation):
                documentation = arg
            elif isinstance(arg, DataPlatform):
                data_platforms.append(arg)
            elif isinstance(arg, DefaultDataPlatform):
                default_data_platform = arg
            else:
                # GovernanceZoneDeclaration
                governance_zone_declarations.append(arg)

        return cls(
            name=name,
            repo=repo,
            platform_services_provider=platform_services_provider,
            data_platforms=data_platforms if data_platforms else None,
            documentation=documentation,
            default_data_platform=default_data_platform,
            infrastructure_vendors=infrastructure_vendors if infrastructure_vendors else None,
            governance_zone_declarations=governance_zone_declarations if governance_zone_declarations else None
        )

    def getAsInfraLocation(self, loc: 'LocationKey') -> Optional[InfrastructureLocation]:
        # The string is in the format vendor:location1/location2/location3
        vendor, locationParts = loc.parseToVendorAndLocations()
        rc: Optional[InfrastructureLocation] = self.getLocation(vendor, locationParts)
        return rc

    def lintLocationKey(self, locKey: 'LocationKey', tree: ValidationTree) -> None:
        """This lints a location key making sure it points to a valid location"""
        if locKey in self.validLocationsSeen:
            return
        locTree: ValidationTree = tree.addSubTree(locKey)
        locKey.lint(locTree)
        if not locTree.hasErrors():
            vendorStr, locationParts = locKey.parseToVendorAndLocations()
            if len(locationParts) == 0:
                locTree.addRaw(InvalidLocationStringProblem("Location string must contain at least one location", locKey.locStr, ProblemSeverity.ERROR))
            else:
                vendor: Optional[InfrastructureVendor] = self.getVendor(vendorStr)
                if vendor is None:
                    locTree.addRaw(UnknownVendorProblem(vendorStr, ProblemSeverity.ERROR))
                else:
                    loc: Optional[InfrastructureLocation] = vendor.getLocation(locationParts[0])
                    if loc is None:
                        locTree.addRaw(UnknownLocationProblem(locationParts[0], ProblemSeverity.ERROR))
                    else:
                        for locIdx in range(1, len(locationParts)):
                            loc = loc.getLocation(locationParts[locIdx])
                            if loc is None:
                                locTree.addRaw(UnknownLocationProblem(locationParts[locIdx], ProblemSeverity.ERROR))
                                break

        """If the location key is valid then add it to the set of valid locations"""
        if not locTree.hasErrors():
            self.validLocationsSeen.add(locKey)

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "name": self.name,
            "zones": {k: k.name for k in self.zones.defineAllObjects()},
            "vendors": {k: k.to_json() for k in self.vendors.values()},
            "dataPlatforms": {k: k.to_json() for k in self.dataPlatforms.values()},
            "renderEngine": self.platformServicesProvider.to_json() if self.platformServicesProvider else None
        }

    def resetCaches(self) -> None:
        """Empties the caches"""
        self.validLocationsSeen: set[LocationKey] = set()
        self.datastoreCache: dict[str, DatastoreCacheEntry] = {}
        """This is a cache of all data stores in the ecosystem"""
        self.workSpaceCache: dict[str, WorkspaceCacheEntry] = {}
        """This is a cache of all workspaces in the ecosystem"""
        self.teamCache: dict[str, TeamCacheEntry] = {}
        """This is a cache of all team declarations in the ecosystem"""

    def generateAllBootstrapArtifacts(self, folderRoot: str, ringLevel: int):
        """This generates the bootstrap artifacts for all the data platforms in the ecosystem. It will create a folder for each data platform, call the
        platform and then create a file named after the key and write the value to the file. The caller should provide the location of the volume mounted
        to expose the files to"""

        for dp in self.dataPlatforms.values():
            self.generateBootstrapArtifacts(folderRoot, dp, ringLevel)

    def generateBootstrapArtifacts(self, folderRoot: str, dp: 'DataPlatform', ringLevel: int):
        """This generates the bootstrap artifacts for all the data platforms in the ecosystem. It will create a folder for each data platform, call the
        platform and then create a file named after the key and write the value to the file. The caller should provide the location of the volume mounted
        to expose the files to"""

        name: str = dp.name
        folder: str = f"bootstrap_{name}"
        os.makedirs(folder, exist_ok=True)
        files: dict[str, str] = dp.generateBootstrapArtifacts(self, ringLevel)
        for key, value in files.items():
            with open(os.path.join(folder, key), "w") as f:
                f.write(value)

    def add(self, *args: Union[PlatformServicesProvider, 'DataPlatform', DefaultDataPlatform,
                               Documentation, InfrastructureVendor, 'GovernanceZoneDeclaration']) -> None:
        for arg in args:
            if isinstance(arg, InfrastructureVendor):
                if self.vendors.get(arg.name) is not None:
                    raise ObjectAlreadyExistsException(f"Duplicate Vendor {arg.name}")
                self.vendors[arg.name] = arg
            elif isinstance(arg, PlatformServicesProvider):
                self.platformServicesProvider = arg
            elif isinstance(arg, Documentation):
                self.documentation = arg
            elif isinstance(arg, DataPlatform):
                self.dataPlatforms[arg.name] = arg
            elif isinstance(arg, DefaultDataPlatform):
                if (self.defaultDataPlatform is not None):
                    raise AttributeAlreadySetException("Default DataPlatform already specified")
                else:
                    self.defaultDataPlatform = arg
            else:
                self.zones.addAuthorization(arg)
                arg.key = GovernanceZoneKey(self.key, arg.name)
        self.resetKey()

    def resetKey(self) -> None:
        for vendor in self.vendors.values():
            vendor.setEcosystem(self.key)

    def getDefaultDataPlatform(self) -> 'DataPlatform':
        """This returns the default DataPlatform or throws an Exception if it has not been specified"""
        if (self.defaultDataPlatform):
            return self.defaultDataPlatform.get(self)
        else:
            raise Exception("No default data platform specified")

    def getVendor(self, name: str) -> Optional[InfrastructureVendor]:
        return self.vendors.get(name)

    def checkDataPlatformExists(self, d: 'DataPlatform') -> bool:
        """This checks if the data platform exists in the ecosystem and is equal to the one in the ecosystem
        with the same name"""
        if (d.name in self.dataPlatforms):
            return self.dataPlatforms[d.name] == d
        return False

    def getVendorOrThrow(self, name: str) -> InfrastructureVendor:
        v: Optional[InfrastructureVendor] = self.getVendor(name)
        if (v):
            if (v.key is None):
                v.setEcosystem(self.key)
            return v
        else:
            raise ObjectDoesntExistException(f"Unknown vendor {name}")

    def getDataPlatform(self, name: str) -> Optional['DataPlatform']:
        return self.dataPlatforms.get(name)

    def getDataPlatformOrThrow(self, name: str) -> 'DataPlatform':
        p: Optional['DataPlatform'] = self.getDataPlatform(name)
        if (p is None):
            raise ObjectDoesntExistException(f"Unknown data platform {name}")
        return p

    def getLocation(self, vendorName: str, locKey: list[str]) -> Optional[InfrastructureLocation]:
        vendor: Optional[InfrastructureVendor] = self.getVendor(vendorName)
        loc: Optional[InfrastructureLocation] = None
        if vendor:
            loc = vendor.findLocationUsingKey(locKey)
        return loc

    def getLocationOrThrow(self, vendorName: str, locKey: list[str]) -> InfrastructureLocation:
        vendor: InfrastructureVendor = self.getVendorOrThrow(vendorName)
        loc: Optional[InfrastructureLocation] = vendor.findLocationUsingKey(locKey)
        if loc is None:
            raise ObjectDoesntExistException(f"Unknown location {locKey} in vendor {vendorName}")
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
        if (w is None):
            raise ObjectDoesntExistException(f"Unknown workspace {work}")
        return w

    def cache_getWorkspace(self, work: str) -> Optional[WorkspaceCacheEntry]:
        """This returns the named workspace if it exists"""
        w: Optional[WorkspaceCacheEntry] = self.workSpaceCache.get(work)
        return w

    def cache_getDatastore(self, store: str) -> Optional[DatastoreCacheEntry]:
        """This returns the named datastore if it exists"""
        s: Optional[DatastoreCacheEntry] = self.datastoreCache.get(store)
        return s

    def cache_getDatastoreOrThrow(self, store: str) -> DatastoreCacheEntry:
        s: Optional[DatastoreCacheEntry] = self.datastoreCache.get(store)
        if s is None:
            raise ObjectDoesntExistException(f"Unknown datastore {store}")
        return s

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
            govTree: ValidationTree = ecoTree.addSubTree(gz)
            gz.lint(self, govTree)

        """All caches should now be populated"""

        for vendor in self.vendors.values():
            vTree: ValidationTree = ecoTree.addSubTree(vendor)
            vendor.lint(vTree)

        for pl in self.dataPlatforms.values():
            platTree: ValidationTree = ecoTree.addSubTree(pl)
            pl.lint(self, platTree)

        # Now lint the workspaces
        for workSpaceCacheEntry in self.workSpaceCache.values():
            workSpace = workSpaceCacheEntry.workspace
            wsTree: ValidationTree = ecoTree.addSubTree(workSpace)
            if (workSpace.key):
                gz: GovernanceZone = self.getZoneOrThrow(workSpace.key.gzName)
                workSpace.lint(self, gz, workSpaceCacheEntry.team, wsTree)
        self.superLint(ecoTree)
        self.zones.lint(ecoTree)
        if (self.documentation):
            self.documentation.lint(ecoTree)

        if (self.defaultDataPlatform is not None):
            self.defaultDataPlatform.lint(self, ecoTree)

        # If there are no errors at this point then
        # Generate pipeline graphs and lint them.
        # This will ask each DataPlatform to verify that it
        # can generate a pipeline graph for its assigned DAG subset. This
        # can fail for a variety of reasons such as the DataPlatform does not
        # support certain DataContainers or even schema mapping issues to underlying
        # infrastructure.

        if not ecoTree.hasErrors():
            try:
                if self.platformServicesProvider is not None:
                    # Lint the renderEngine which lints the intentions graphs
                    self.platformServicesProvider.lint(self, ecoTree.addSubTree(self.platformServicesProvider))
            except Exception as e:
                ecoTree.addProblem(f"Error generating pipeline graph {e}", ProblemSeverity.ERROR)

        # Prune the tree to remove objects that have no problems
        ecoTree.prune()
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

        zTree: ValidationTree = vTree.addSubTree(self.zones)
        self.zones.checkIfChangesAreAuthorized(prop_eco.zones, changeSource, zTree)

    def __eq__(self, proposed: object) -> bool:
        if super().__eq__(proposed) and isinstance(proposed, Ecosystem):
            rc = self.name == proposed.name
            rc = rc and self.zones == proposed.zones
            rc = rc and self.key == proposed.key
            rc = rc and self.vendors == proposed.vendors
            rc = rc and self.dataPlatforms == proposed.dataPlatforms
            rc = rc and self.defaultDataPlatform == proposed.defaultDataPlatform
            rc = rc and self.platformServicesProvider == proposed.platformServicesProvider
            rc = rc and self.dsgPlatformMappings == proposed.dsgPlatformMappings
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
                zTree: ValidationTree = tree.addSubTree(self.zones)
                if not self.zones.areTopLevelChangesAuthorized(proposed.zones, changeSource, zTree):
                    rc = False
                if self._check_dict_changes(self.vendors, proposed.vendors, tree, "Vendors"):
                    rc = False
                if self._check_dict_changes(self.dataPlatforms, proposed.dataPlatforms, tree, "DataPlatforms"):
                    rc = False
                if self.dsgPlatformMappings != proposed.dsgPlatformMappings:
                    tree.addRaw(UnauthorizedAttributeChange("dsgPlatformMappings", self.dsgPlatformMappings,
                                                            proposed.dsgPlatformMappings, ProblemSeverity.ERROR))
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
        if (z is None):
            raise ObjectDoesntExistException(f"Unknown zone {gz}")
        return z

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
        if (t is None):
            raise ObjectDoesntExistException(f"Unknown team {teamName} in zone {gz}")
        return t

    def __str__(self) -> str:
        return f"Ecosystem({self.name})"

    def checkIfChangesAreBackwardsCompatibleWith(self, originEco: 'Ecosystem', vTree: ValidationTree) -> None:
        """This checks if the proposed ecosystem is backwards compatible with the current ecosystem"""
        # Check if the zones are compatible
        for zone in self.zones.authorizedObjects.values():
            zTree: ValidationTree = vTree.addSubTree(zone)
            originZone: Optional[GovernanceZone] = originEco.getZone(zone.name)
            if originZone:
                zone.checkForBackwardsCompatiblity(originZone, zTree)

    # Check that the changeSource is one of the authorized sources
    def checkIfChangeSourceIsUsed(self, changeSource: Repository, tree: ValidationTree) -> None:
        # First, gather all the repositories used by the parts in a set
        allSources: set[Repository] = set()
        allSources.add(self.owningRepo)
        # All declared zones
        for zone in self.zones.authorizedNames.values():
            allSources.add(zone.owningRepo)
        # Any teams for defined zones.
        for zone in self.zones.authorizedObjects.values():
            for team in zone.teams.authorizedObjects.values():
                allSources.add(team.owningRepo)
        # Now, just check if the changeSource is in the set
        if changeSource not in allSources:
            tree.addRaw(UnknownChangeSource(changeSource, ProblemSeverity.ERROR))

    def checkIfChangesCanBeMerged(self, proposed: 'Ecosystem', source: Repository) -> ValidationTree:
        """This is called to check if the proposed changes can be merged in to the current ecosystem. It returns a ValidationTree with issues if not
        or an empty ValidationTree if allowed."""

        # First, the incoming ecosystem must be consistent and pass lint checks
        eTree: ValidationTree = proposed.lintAndHydrateCaches()

        # Any errors make us fail immediately
        # But we want warnings and infos to accumulate for the caller
        if eTree.hasErrors():
            return eTree

        # Check if the changeSource is one of the authorized sources
        self.checkIfChangeSourceIsUsed(source, eTree)
        if eTree.hasErrors():
            return eTree

        # Check if the proposed changes being made by an authorized repository
        self.checkIfChangesAreAuthorized(proposed, source, eTree)
        if eTree.hasErrors():
            return eTree

        # Check if the proposed changes are backwards compatible this object
        proposed.checkIfChangesAreBackwardsCompatibleWith(self, eTree)
        return eTree


class VendorKey(UserDSLObject):
    """This is used to reference a vendor during DSL construction"""
    def __init__(self, vendor: str) -> None:
        UserDSLObject.__init__(self)
        self.vendorString: str = vendor
        self.vendor: Optional[InfrastructureVendor] = None

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "vendorString": self.vendorString, "vendor": self.vendor.to_json() if self.vendor else None})
        return rc

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, VendorKey)):
            return self.vendorString == other.vendorString and self.vendor == other.vendor
        return False

    def getAsInfraVendor(self, eco: Ecosystem) -> Optional[InfrastructureVendor]:
        if self.vendor is not None:
            return self.vendor
        vendor: Optional[InfrastructureVendor] = eco.getVendor(self.vendorString)
        return vendor

    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        if (self.getAsInfraVendor(eco) is None):
            tree.addRaw(UnknownVendorProblem(self.vendorString, ProblemSeverity.ERROR))

    def __str__(self) -> str:
        return f"VendorKey({self.vendorString})"

    def __hash__(self) -> int:
        return hash(self.vendorString)


class Team(GitControlledObject, JSONable):
    """This is the authoritive definition of a team within a goverance zone. All teams must have
    a corresponding TeamDeclaration in the owning GovernanceZone"""
    def __init__(self, name: str, repo: Repository,
                 *args: Union[Datastore, 'Workspace', Documentation, DataContainer],
                 datastores: Optional[list[Datastore]] = None,
                 workspaces: Optional[list['Workspace']] = None,
                 documentation: Optional[Documentation] = None,
                 containers: Optional[list[DataContainer]] = None) -> None:
        GitControlledObject.__init__(self, repo)
        JSONable.__init__(self)
        self.name: str = name
        self.workspaces: dict[str, Workspace] = OrderedDict()
        self.dataStores: dict[str, Datastore] = OrderedDict()
        self.containers: dict[str, DataContainer] = OrderedDict()

        # Handle backward compatibility: if *args are provided, parse them the old way
        if args:
            # Legacy mode: parse *args (slower but compatible)
            self.add(*args)
        else:
            # New mode: use named parameters directly (faster!)
            if datastores is not None:
                for datastore in datastores:
                    self.addStore(datastore)

            if workspaces is not None:
                for workspace in workspaces:
                    self.addWorkspace(workspace)

            if documentation is not None:
                self.documentation = documentation

            if containers is not None:
                for container in containers:
                    if self.containers.get(container.name) is not None:
                        raise ObjectAlreadyExistsException(f"Duplicate DataContainer {container.name}")
                    self.containers[container.name] = container

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        rc.update({"name": self.name})
        rc.update({"dataStores": {k: v.name for k, v in self.dataStores.items()}})
        rc.update({"workspaces": {k: v.name for k, v in self.workspaces.items()}})
        rc.update({"containers": {k: v.to_json() for k, v in self.containers.items()}})
        return rc

    def add(self, *args: Union[Datastore, 'Workspace', Documentation, DataContainer]) -> None:
        """Adds a workspace, datastore or gitrepository to the team"""
        for arg in args:
            if (isinstance(arg, Datastore)):
                s: Datastore = arg
                self.addStore(s)
            elif (isinstance(arg, Workspace)):
                w: Workspace = arg
                self.addWorkspace(w)
            elif (isinstance(arg, DataContainer)):
                dc: DataContainer = arg
                if self.containers.get(dc.name) is not None:
                    raise ObjectAlreadyExistsException(f"Duplicate DataContainer {dc.name}")
                self.containers[dc.name]
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
            cmd: CaptureMetaData = DataTransformerOutput(w.name)
            # the transformer will capture data from the Workspace container
            # when the transformer finishes running
            if (w.dataContainer):
                cmd.add(w.dataContainer)
            oStore.add(cmd)
            self.addStore(w.dataTransformer.outputDatastore)

    def __eq__(self, other: object) -> bool:
        if super().__eq__(other) and isinstance(other, Team):
            rc: bool = self.name == other.name
            rc = rc and self.workspaces == other.workspaces
            rc = rc and self.dataStores == other.dataStores
            rc = rc and self.containers == other.containers
            return rc
        return False

    def getStoreOrThrow(self, storeName: str) -> Datastore:
        rc: Optional[Datastore] = self.dataStores.get(storeName)
        if rc is None:
            raise ObjectDoesntExistException(f"Unknown datastore {storeName}")
        return rc

    def getDataContainerOrThrow(self, containerName: str) -> DataContainer:
        """Returns the named data container or throws an exception if it does not exist"""
        rc: Optional[DataContainer] = self.containers.get(containerName)
        if rc is None:
            raise ObjectDoesntExistException(f"Unknown data container {containerName}")
        return rc

    def areTopLevelChangesAuthorized(self, proposed: GitControlledObject, changeSource: Repository, tree: ValidationTree) -> bool:
        """This is a shallow equality check for the top level team object"""
        # If we are being changed by an authorized source then it doesnt matter
        if (self.owningRepo == changeSource):
            return True
        if not super().areTopLevelChangesAuthorized(proposed, changeSource, tree):
            return False
        if not isinstance(proposed, Team):
            return False
        if self._check_dict_changes(self.dataStores, proposed.dataStores, tree, "DataStores"):
            return False
        if self._check_dict_changes(self.workspaces, proposed.workspaces, tree, "Workspaces"):
            return False
        if self._check_dict_changes(self.containers, proposed.containers, tree, "Containers"):
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
                storeTree: ValidationTree = teamTree.addSubTree(s)
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
                wTree: ValidationTree = teamTree.addSubTree(w)

                # Check all classification allows policies from gz are satisfied on every sink
                for dccPolicy in gz.classificationPolicies.values():
                    for dsg in w.dsgs.values():
                        for sink in dsg.sinks.values():
                            store: Datastore = eco.cache_getDatastoreOrThrow(sink.storeName).datastore
                            dataset: Dataset = store.datasets[sink.datasetName]
                            if (not dataset.checkClassificationsAreOnly(dccPolicy)):
                                wTree.addRaw(ObjectNotCompatibleWithPolicy(sink, dccPolicy, ProblemSeverity.ERROR))

        # Iterate over DataContainers linting as we go
        for c in self.containers.values():
            cTree: ValidationTree = teamTree.addSubTree(c)
            c.lint(eco, cTree)
        self.superLint(teamTree)

    def __str__(self) -> str:
        return f"Team({self.name})"

    def checkForBackwardsCompatibility(self, originTeam: 'Team', vTree: ValidationTree):
        """This checks if the current team is backwards compatible with the origin team"""
        # Check if the datasets are compatible
        for store in self.dataStores.values():
            sTree: ValidationTree = vTree.addSubTree(store)
            originStore: Optional[Datastore] = originTeam.dataStores.get(store.name)
            if (originStore):
                store.checkForBackwardsCompatibility(originStore, sTree)


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


class AuthorizedObjectManager(GitControlledObject, Generic[G, N]):
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

    def defineAllObjects(self) -> list[G]:
        """This 'defines' all declared objects"""
        values: list[G] = list()
        for n in self.authorizedNames.values():
            v: Optional[G] = self.getObject(n.name)
            if (v is not None):
                values.append(v)
        return values

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

    def __eq__(self, other: object) -> bool:
        if (super().__eq__(other) and isinstance(other, AuthorizedObjectManager)):
            a: AuthorizedObjectManager[G, N] = cast(AuthorizedObjectManager[G, N], other)
            rc: bool = self.authorizedNames == a.authorizedNames
            rc = rc and self.name == a.name
            rc = rc and self.authorizedObjects == a.authorizedObjects
            # Cannot test factory for equality
            # rc = rc and self.factory is a.factory
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


class GovernanceZone(GitControlledObject, JSONable):
    """This declares the existence of a specific GovernanceZone and defines the teams it manages, the storage policies
    and which repos can be used to pull changes for various metadata"""
    def __init__(self, name: str, ownerRepo: Repository, *args: Union['InfraStructureLocationPolicy', 'InfraStructureVendorPolicy',
                                                                      StoragePolicy, DataClassificationPolicy, TeamDeclaration,
                                                                      Documentation, DataPlatformPolicy, InfraHardVendorPolicy]) -> None:
        GitControlledObject.__init__(self, ownerRepo)
        JSONable.__init__(self)
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

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        teamKeys: list[Team] = self.teams.defineAllObjects()
        rc.update({
            "name": self.name,
            "teams": {k: k.name for k in teamKeys},
            "storagePolicies": {k: v.to_json() for k, v in self.storagePolicies.items()},
            "classificationPolicies": {k: v.to_json() for k, v in self.classificationPolicies.items()},
            "vendorPolicies": {k: v.to_json() for k, v in self.vendorPolicies.items()},
            "hardVendorPolicies": {k: v.to_json() for k, v in self.hardVendorPolicies.items()},
            "locationPolicies": {k: v.to_json() for k, v in self.locationPolicies.items()},
            "dataplatformPolicies": {k: v.to_json() for k, v in self.dataplatformPolicies.items()},
        })
        return rc

    def setEcosystem(self, eco: Ecosystem) -> None:
        """Sets the ecosystem for this zone and sets the zone for all teams"""
        self.key = GovernanceZoneKey(eco.key, self.name)

        self.add()

    def checkLocationIsAllowed(self, eco: 'Ecosystem', location: LocationKey, tree: ValidationTree):
        """This checks that the provided location is allowed based on the vendor and location policies
        of the GZ, this allows a GZ to constrain where its data can come from or be used"""
        loc: Optional[InfrastructureLocation] = eco.getAsInfraLocation(location)
        if (loc is None):
            tree.addRaw(UnknownLocationProblem(str(location), ProblemSeverity.ERROR))
            return
        for locPolicy in self.locationPolicies.values():
            if not locPolicy.isCompatible(location):
                tree.addRaw(ObjectNotCompatibleWithPolicy(loc, locPolicy, ProblemSeverity.ERROR))
        if (loc.key):
            v: InfrastructureVendor = eco.getVendorOrThrow(loc.key.ivName)
            for vendorPolicy in self.vendorPolicies.values():
                if not vendorPolicy.isCompatible(VendorKey(v.name)):
                    tree.addRaw(ObjectNotCompatibleWithPolicy(v, vendorPolicy, ProblemSeverity.ERROR))
            for hardVendorPolicy in self.hardVendorPolicies.values():
                if (v.hardCloudVendor is None):
                    tree.addRaw(AttributeNotSet(f"{loc} No hard cloud vendor"))
                elif not hardVendorPolicy.isCompatible(Literal(v.hardCloudVendor)):
                    tree.addRaw(ObjectNotCompatibleWithPolicy(v, hardVendorPolicy, ProblemSeverity.ERROR))
        else:
            tree.addRaw(AttributeNotSet("loc.key"))

    def add(self, *args: Union['InfraStructureVendorPolicy', 'InfraStructureLocationPolicy', StoragePolicy, DataClassificationPolicy,
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
        if (t is None):
            raise ObjectDoesntExistException(f"Unknown team {name}")
        return t

    def __eq__(self, other: object) -> bool:
        if isinstance(other, GovernanceZone):
            rc: bool = super().__eq__(other)
            rc = rc and self.name == other.name
            rc = rc and self.key == other.key
            rc = rc and self.dataplatformPolicies == other.dataplatformPolicies
            rc = rc and self.teams == other.teams
            rc = rc and self.classificationPolicies == other.classificationPolicies
            rc = rc and self.storagePolicies == other.storagePolicies
            rc = rc and self.vendorPolicies == other.vendorPolicies
            rc = rc and self.hardVendorPolicies == other.hardVendorPolicies
            rc = rc and self.locationPolicies == other.locationPolicies
            return rc
        return False

    def areTopLevelChangesAuthorized(self, proposed: GitControlledObject, changeSource: Repository, tree: ValidationTree) -> bool:
        """Just check the not git controlled attributes"""
        # If we're changed by an authorized source then it doesn't matter
        if (self.owningRepo == changeSource):
            return True
        if not (super().areTopLevelChangesAuthorized(proposed, changeSource, tree) and type(proposed) is GovernanceZone and self.name == proposed.name):
            return False
        if self._check_dict_changes(self.storagePolicies, proposed.storagePolicies, tree, "StoragePolicies"):
            return False
        if self._check_dict_changes(self.dataplatformPolicies, proposed.dataplatformPolicies, tree, "DataPlatformPolicies"):
            return False
        if self._check_dict_changes(self.vendorPolicies, proposed.vendorPolicies, tree, "VendorPolicies"):
            return False
        if self._check_dict_changes(self.hardVendorPolicies, proposed.hardVendorPolicies, tree, "HardVendorPolicies"):
            return False
        if self._check_dict_changes(self.classificationPolicies, proposed.classificationPolicies, tree, "ClassificationPolicies"):
            return False
        if self._check_dict_changes(self.locationPolicies, proposed.locationPolicies, tree, "LocationPolicies"):
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
                    teamTree: ValidationTree = govTree.addSubTree(team)
                    team.lint(eco, self, td, teamTree)
        self.superLint(govTree)
        self.teams.lint(govTree)
        if (self.key is None):
            govTree.addRaw(AttributeNotSet("Key not set"))

    def __str__(self) -> str:
        return f"GovernanceZone({self.name})"

    def checkForBackwardsCompatiblity(self, originZone: 'GovernanceZone', tree: ValidationTree):
        """This checks if this zone is backwards compatible with the original zone. This means that the proposed zone
        can be used to replace this one without breaking any data pipelines"""

        # Check if the teams are compatible
        for team in self.teams.authorizedObjects.values():
            tTree: ValidationTree = tree.addSubTree(team)
            originTeam: Optional[Team] = originZone.getTeam(team.name)
            # if team exists in old zone then check it, otherwise, it's a new team and we don't care
            if originTeam:
                team.checkForBackwardsCompatibility(originTeam, tTree)

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


class DockerContainer:
    """This is a docker container which can be used to run some code such as a DataPlatform"""
    def __init__(self, name: str, image: str, version: str, cmd: str) -> None:
        self.name: str = name
        self.image: str = image
        self.version: str = version
        self.cmd: str = cmd

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, DockerContainer) and self.name == __value.name and self.image == __value.image and \
            self.version == __value.version and self.cmd == __value.cmd

    def __str__(self) -> str:
        return f"DockerContainer({self.name})"


class DataPlatformExecutor(InternalLintableObject, JSONable):
    """This specifies how a DataPlatform should execute"""
    def __init__(self) -> None:
        InternalLintableObject.__init__(self)
        JSONable.__init__(self)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DataPlatformExecutor)

    def __str__(self) -> str:
        return "DataPlatformExecutor()"

    @abstractmethod
    def lint(self, eco: Ecosystem, tree: ValidationTree):
        pass


class DataPlatformCICDExecutor(DataPlatformExecutor):
    """This is a DataPlatformExecutor for DataPlatforms which generate an IaC representation of the
    DataSurface intention graph and stores the generated IaC in a git repository. The IaC platform
    such as terraform or AWS cdk then checks for events on that repository and applies then changes
    from the repository when they are detected."""
    def __init__(self, repo: Repository) -> None:
        super().__init__()
        self.iacRepo: Repository = repo

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DataPlatformCICDExecutor) and self.iacRepo == other.iacRepo

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)
        self.iacRepo.lint(tree.addSubTree(self.iacRepo))

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "iacRepo": self.iacRepo.to_json()})
        return rc


T = TypeVar('T')


class DataPlatform(Documentable, JSONable):
    """This is a system which can interpret data flows in the metadata and realize those flows"""
    def __init__(self, name: str,
                 *args: Union[DataPlatformExecutor, Documentation],
                 executor: Optional[DataPlatformExecutor] = None,
                 documentation: Optional[Documentation] = None) -> None:
        Documentable.__init__(self, documentation)
        JSONable.__init__(self)
        self.name: str = name

        # Handle backward compatibility: if *args are provided, parse them the old way
        if args:
            # Legacy mode: parse *args (slower but compatible)
            parsed_executor: Optional[DataPlatformExecutor] = executor
            parsed_documentation: Optional[Documentation] = documentation

            for arg in args:
                if isinstance(arg, DataPlatformExecutor):
                    parsed_executor = arg
                else:
                    # Remaining argument should be Documentation
                    parsed_documentation = arg

            # Use parsed values
            if parsed_executor is None:
                raise ObjectDoesntExistException(f"Could not find object of type {DataPlatformExecutor}")
            self.executor: DataPlatformExecutor = parsed_executor

            # Initialize Documentable with parsed documentation
            self.documentation = parsed_documentation
        else:
            # New mode: use named parameters directly (faster!)
            if executor is None:
                raise ObjectDoesntExistException(f"Could not find object of type {DataPlatformExecutor}")
            self.executor: DataPlatformExecutor = executor

    @abstractmethod
    def getCredentialStore(self) -> CredentialStore:
        """This returns the credential store for the data platform"""
        raise NotImplementedError("getCredentialStore not implemented")

    @classmethod
    def create_legacy(cls, name: str, *args: Union[DataPlatformExecutor, Documentation]) -> 'DataPlatform':
        """Legacy factory method for backward compatibility with old *args pattern.
        Use this temporarily during migration, then switch to named parameters for better performance."""
        executor: Optional[DataPlatformExecutor] = None
        documentation: Optional[Documentation] = None

        for arg in args:
            if isinstance(arg, DataPlatformExecutor):
                executor = arg
            else:
                # Remaining argument should be Documentation
                documentation = arg

        return cls(
            name=name,
            executor=executor,
            documentation=documentation
        )

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc

    def add(self, *args: Union[DataPlatformExecutor, Documentation]) -> None:
        """Add executor or documentation. Provided for backward compatibility."""
        for arg in args:
            if isinstance(arg, DataPlatformExecutor):
                if self.executor != arg:
                    raise ObjectAlreadyExistsException("Executor already set")
                self.executor = arg
            else:
                self.documentation = arg

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DataPlatform) and self.name == other.name and self.executor == other.executor and Documentable.__eq__(self, other)

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    @abstractmethod
    def getSupportedVendors(self, eco: Ecosystem) -> set[CloudVendor]:
        pass

    @abstractmethod
    def isWorkspaceDataContainerSupported(self, eco: Ecosystem, dc: DataContainer) -> bool:
        # This is called to check if a DataContainer specified by a Workspace is supported by the DataPlatform assigned to it.
        pass

    @abstractmethod
    def lint(self, eco: Ecosystem, tree: ValidationTree):
        if (self.documentation):
            self.documentation.lint(tree)
        self.executor.lint(eco, tree.addSubTree(self.executor))
        if (not eco.checkDataPlatformExists(self)):
            tree.addRaw(ValidationProblem(f"DataPlatform {self} not found in ecosystem {eco}", ProblemSeverity.ERROR))

    @abstractmethod
    def createGraphHandler(self, graph: 'PlatformPipelineGraph') -> 'DataPlatformGraphHandler':
        """This is typically called in response to a merge event on a repository. This provides the DataPlatform with the ingestion graph assigned to it. This
        is used to either lint the graph and check the DataPlatform can actually execute the pipeline described in the graph as well as create or modify an
        existing pipeline infrastructure to execute the ingestion graph provided."""
        pass

    @abstractmethod
    def generateBootstrapArtifacts(self, eco: Ecosystem, ringLevel: int) -> dict[str, str]:
        """This generates the bootstrap artifacts from the data platform. The ecosystem is needed to get the eco reposistory among other things"""
        pass

    @abstractmethod
    def createSchemaProjector(self, eco: Ecosystem) -> SchemaProjector:
        """This returns a schema projector which can be used to project the dataset schema to a schema compatible with the container"""
        raise NotImplementedError("createSchemaProjector not implemented")

    @abstractmethod
    def lintWorkspace(self, eco: Ecosystem, tree: ValidationTree, ws: 'Workspace', dsgName: str):
        raise NotImplementedError("lintWorkspace not implemented")

    @abstractmethod
    def resetBatchState(self, eco: Ecosystem, storeName: str, datasetName: Optional[str] = None) -> str:
        """This resets the batch state for a datastore"""
        raise NotImplementedError("resetBatchState not implemented")


class UnsupportedIngestionType(ValidationProblem):
    """This indicates an ingestion type is not supported by a data platform"""
    def __init__(self, store: Datastore, dp: DataPlatform, sev: ProblemSeverity) -> None:
        super().__init__(f"Ingestion type {store.cmd} is not supported by {dp}", sev)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, UnsupportedIngestionType)


class DatasetConsistencyNotSupported(ValidationProblem):
    """This indicates a dataset consistency type is not supported by a data platform"""
    def __init__(self, store: Datastore, type: IngestionConsistencyType, dp: DataPlatform, sev: ProblemSeverity) -> None:
        super().__init__(f"Store: {store.name} Dataset consistency type {type} is not supported by {dp}", sev)


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


class DataMilestoningStrategy(Enum):
    """Client indicates whether the data is live or forensic"""
    LIVE_ONLY = 0
    """Only the latest version of each record should be retained"""
    FORENSIC = 1
    """All versions of every record in every table used to produce the datasets should be retained and present in the consumer data tables"""
    LIVE_WITH_FORENSIC_HISTORY = 2
    """All versions of each record should be retained BUT only latest records are needed in the consumer data tables"""


# This needs to be keyed and rolled up to manage definitions centrally, there should
# be a common ESMA definition for example (5 years forensic)
class ConsumerRetentionRequirements(UserDSLObject):
    """Consumers specify the retention requirements for the data they consume. Platforms use this to backtrack
    retention requirements for data in the full inferred pipeline to manage that consumer"""
    def __init__(self, r: DataMilestoningStrategy, latency: DataLatency, regulator: Optional[str],
                 minRetentionDurationIfNeeded: Optional[timedelta] = None) -> None:
        self.milestoningStrategy: DataMilestoningStrategy = r
        self.latency: DataLatency = latency
        self.minRetentionTime: Optional[timedelta] = minRetentionDurationIfNeeded
        self.regulator: Optional[str] = regulator

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, ConsumerRetentionRequirements)):
            return super().__eq__(other) and self.milestoningStrategy == other.milestoningStrategy and self.latency == other.latency and \
                self.minRetentionTime == other.minRetentionTime and self.regulator == other.regulator
        return False

    def __hash__(self) -> int:
        return hash((self.milestoningStrategy, self.latency, self.minRetentionTime, self.regulator))

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "milestoningStrategy": self.milestoningStrategy.name, "latency": self.latency.name,
                   "minRetentionTime": self.minRetentionTime, "regulator": self.regulator})
        return rc


class DataPlatformChooser(UserDSLObject):
    """Subclasses of this choose a DataPlatform to render the pipeline for moving data from a producer to a Workspace possibly
    through intermediate Workspaces"""
    def __init__(self):
        UserDSLObject.__init__(self)

    @abstractmethod
    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        return {"_type": self.__class__.__name__}


class WorkspacePlatformConfig(DataPlatformChooser):
    """This allows a Workspace to specify per pipeline hints for behavior, i.e.
    allowed latency and so on"""
    def __init__(self, hist: ConsumerRetentionRequirements) -> None:
        self.retention: ConsumerRetentionRequirements = hist

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, WorkspacePlatformConfig)):
            return super().__eq__(other) and self.retention == other.retention
        return False

    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        """For now, just return default"""
        # TODO This should evaluate the parameters provide and choose the 'best' DataPlatform
        return eco.getDefaultDataPlatform()

    def __str__(self) -> str:
        return f"WorkspacePlatformConfig({self.retention})"

    def __hash__(self) -> int:
        return hash(self.retention)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        rc.update({"retention": self.retention.to_json()})
        return rc


class WorkspaceFixedDataPlatform(DataPlatformChooser):
    """This specifies a fixed DataPlatform for a Workspace"""
    def __init__(self, dp: DataPlatformKey):
        self.dataPlatform: DataPlatformKey = dp

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, WorkspaceFixedDataPlatform) and self.dataPlatform == o.dataPlatform

    def choooseDataPlatform(self, eco: Ecosystem) -> Optional[DataPlatform]:
        return eco.getDataPlatform(self.dataPlatform.name)

    def __str__(self) -> str:
        return f"WorkspaceFixedDataPlatform({self.dataPlatform})"

    def __hash__(self) -> int:
        return hash(self.dataPlatform)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        rc.update({"dataPlatform": self.dataPlatform.name})
        return rc


class DeprecationsAllowed(Enum):
    """This specifies if deprecations are allowed for a specific dataset in a workspace dsg"""
    NEVER = 0
    """Deprecations are never allowed"""
    ALLOWED = 1
    """Deprecations are allowed but not will generate warnings"""


class DatasetSink(UserDSLObject):

    @staticmethod
    def calculateKey(storeName: str, datasetName: str) -> str:
        return f"{storeName}:{datasetName}"

    """This is a reference to a dataset in a Workspace"""
    def __init__(self, storeName: str, datasetName: str, deprecationsAllowed: DeprecationsAllowed = DeprecationsAllowed.NEVER) -> None:
        UserDSLObject.__init__(self)
        self.storeName: str = storeName
        self.datasetName: str = datasetName
        self.key = DatasetSink.calculateKey(self.storeName, self.datasetName)
        self.deprecationsAllowed: DeprecationsAllowed = deprecationsAllowed

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({
            "_type": self.__class__.__name__,
            "storeName": self.storeName,
            "datasetName": self.datasetName,
            "deprecationsAllowed": self.deprecationsAllowed.name
        })
        return rc

    def __eq__(self, other: object) -> bool:
        if (type(other) is DatasetSink):
            return super().__eq__(other) and self.key == other.key and self.storeName == other.storeName and self.datasetName == other.datasetName and \
                self.deprecationsAllowed == other.deprecationsAllowed
        else:
            return False

    def __hash__(self) -> int:
        return hash(f"{self.storeName}/{self.datasetName}")

    def lint(self, eco: Ecosystem, team: Team, ws: 'Workspace', dsg: 'DatasetGroup', tree: ValidationTree):
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
                # If approvals are required, check this DSG is in the store approval list
                if (not store.isDatasetDSGApproved(ws.name, dsg.name, self.datasetName)):
                    tree.addRaw(ConstraintViolation(
                        f"Dataset {self.storeName}:{self.datasetName} is not approved for use in {ws.name}#{dsg.name}", ProblemSeverity.ERROR))
                # Check Workspace dataContainer locations are compatible with the Datastore gz policies
                if (store.key):
                    gzStore: GovernanceZone = eco.getZoneOrThrow(store.key.gzName)
                    if (ws.dataContainer):
                        ws.dataContainer.lint(eco, tree)
                        for loc in ws.dataContainer.locations:
                            gzStore.checkLocationIsAllowed(eco, loc, tree)
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
                        tree.addRaw(ObjectNotCompatibleWithPolicy(self, ws.classificationVerifier, ProblemSeverity.ERROR))
            else:
                tree.addRaw(UnknownObjectReference(f"Datastore {self.storeName}", ProblemSeverity.ERROR))

    def __str__(self) -> str:
        return f"DatasetSink({self.storeName}:{self.datasetName})"


class DatasetGroupDataPlatformMappingStatus(Enum):
    """This indicates the status of a DataPlatform assignment to a DatasetGroup"""
    PROVISIONING = 0
    """The DataPlatform is being provisioned"""
    PROVISIONED = 1
    """The DataPlatform is provisioned and ready to use"""
    DECOMMISSIONING = 2
    """The DataPlatform is being decommissioned"""
    DECOMMISSIONED = 3
    """The DataPlatform is decommissioned and no longer used"""


class DSGDataPlatformAssignment(UserDSLObject):
    """This is a reference to a DataPlatform which is assigned to a DatasetGroup"""
    def __init__(self, workspace: str, dsgName: str, dp: DataPlatformKey, doc: Documentation, productionStatus: ProductionStatus = ProductionStatus.PRODUCTION,
                 deprecationsAllowed: DeprecationsAllowed = DeprecationsAllowed.NEVER,
                 status: DatasetGroupDataPlatformMappingStatus = DatasetGroupDataPlatformMappingStatus.PROVISIONING) -> None:
        UserDSLObject.__init__(self)
        self.workspace: str = workspace
        self.dsgName: str = dsgName
        self.dataPlatform: DataPlatformKey = dp
        self.documentation: Documentation = doc
        self.productionStatus: ProductionStatus = productionStatus
        self.deprecationsAllowed: DeprecationsAllowed = deprecationsAllowed
        self.status: DatasetGroupDataPlatformMappingStatus = DatasetGroupDataPlatformMappingStatus.PROVISIONING

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({
            "_type": self.__class__.__name__,
            "workspace": self.workspace,
            "dsgName": self.dsgName,
            "dataPlatform": self.dataPlatform.name,
            "documentation": self.documentation.to_json(),
            "productionStatus": self.productionStatus.name,
            "deprecationsAllowed": self.deprecationsAllowed.name,
            "status": self.status.name
        })
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DSGDataPlatformAssignment) and self.workspace == other.workspace and \
            self.dsgName == other.dsgName and self.dataPlatform == other.dataPlatform and self.documentation == other.documentation and \
            self.productionStatus == other.productionStatus and self.deprecationsAllowed == other.deprecationsAllowed and \
            self.status == other.status

    def __hash__(self) -> int:
        return hash((self.workspace, self.dsgName, self.dataPlatform, self.documentation, self.productionStatus, self.deprecationsAllowed, self.status))

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        # Make sure the workspace and dsg exist
        w: Optional[WorkspaceCacheEntry] = eco.cache_getWorkspace(self.workspace)
        if w is None:
            tree.addRaw(UnknownObjectReference(f"Unknown workspace {self.workspace}", ProblemSeverity.ERROR))
        else:
            dp: Optional[DataPlatform] = eco.getDataPlatform(self.dataPlatform.name)
            if dp is None:
                tree.addRaw(UnknownObjectReference(f"Unknown data platform {self.dataPlatform.name}", ProblemSeverity.ERROR))
            else:
                dp.lintWorkspace(eco, tree.addSubTree(dp), w.workspace, self.dsgName)


class DatasetGroupDataPlatformAssignments(UserDSLObject):
    """This is a reference to a DataPlatform which is assigned to a Workspace"""
    def __init__(self, workspace: str, dsgName: str, assignments: list[DSGDataPlatformAssignment]) -> None:
        UserDSLObject.__init__(self)
        self.workspace: str = workspace
        self.dsgName: str = dsgName
        self.assignments: list[DSGDataPlatformAssignment] = assignments

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "assignments": [assignment.to_json() for assignment in self.assignments]})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, DatasetGroupDataPlatformAssignments) and self.assignments == other.assignments and \
            self.workspace == other.workspace and self.dsgName == other.dsgName

    def __hash__(self) -> int:
        return hash((self.workspace, self.dsgName, tuple(self.assignments)))

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        # Make sure the workspace and dsg exist
        w: Optional[WorkspaceCacheEntry] = eco.cache_getWorkspace(self.workspace)
        if w is None:
            tree.addRaw(UnknownObjectReference(f"Unknown workspace {self.workspace}", ProblemSeverity.ERROR))
        else:
            # Make sure the dsg exists
            dsg: Optional[DatasetGroup] = w.workspace.dsgs.get(self.dsgName)
            if dsg is None:
                tree.addRaw(UnknownObjectReference(f"Unknown dataset group {self.workspace}:{self.dsgName}", ProblemSeverity.ERROR))
            else:
                # Make sure the DSG has a platformMD
                if dsg.platformMD is None:
                    tree.addRaw(ConstraintViolation(f"DSG {self.workspace}:{self.dsgName} must have a platformMD",
                                                    ProblemSeverity.ERROR))

        for assignment in self.assignments:
            assignment.lint(eco, tree.addSubTree(assignment))


class DatasetGroup(ANSI_SQL_NamedObject, Documentable):
    """A collection of Datasets which are rendered with a specific pipeline spec in a Workspace. The name should be
    ANSI SQL compliant because it could be used as part of a SQL View/Table name in a Workspace database"""
    def __init__(self, name: str,
                 *args: Union[DatasetSink, DataPlatformChooser, Documentation],
                 sinks: Optional[list[DatasetSink]] = None,
                 platform_chooser: Optional[DataPlatformChooser] = None,
                 documentation: Optional[Documentation] = None) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        self.platformMD: Optional[DataPlatformChooser] = platform_chooser
        self.sinks: dict[str, DatasetSink] = OrderedDict[str, DatasetSink]()

        if args:
            # legacy mode: parse *args
            parsed_platformMD: Optional[DataPlatformChooser] = platform_chooser
            parsed_documentation: Optional[Documentation] = documentation
            parsed_sinks: dict[str, DatasetSink] = OrderedDict[str, DatasetSink]()

            for arg in args:
                if isinstance(arg, DatasetSink):
                    sink: DatasetSink = arg
                    if (parsed_sinks.get(sink.key) is not None):
                        raise ObjectAlreadyExistsException(f"Duplicate DatasetSink {sink.key}")
                    parsed_sinks[sink.key] = sink
                elif isinstance(arg, Documentation):
                    parsed_documentation = arg
                else:
                    parsed_platformMD = arg

            # Use parsed values
            self.platformMD: Optional[DataPlatformChooser] = parsed_platformMD
            self.documentation: Optional[Documentation] = parsed_documentation
            self.sinks: dict[str, DatasetSink] = parsed_sinks
        else:
            # new mode: use named parameters directly (faster!)
            self.platformMD: Optional[DataPlatformChooser] = platform_chooser
            self.documentation: Optional[Documentation] = documentation

            if sinks is not None:
                for sink in sinks:
                    if self.sinks.get(sink.key) is not None:
                        raise ObjectAlreadyExistsException(f"Duplicate DatasetSink {sink.key}")
                    self.sinks[sink.key] = sink

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = ANSI_SQL_NamedObject.to_json(self)
        rc.update(Documentable.to_json(self))
        rc.update({
            "_type": self.__class__.__name__,
            "sinks": {sink.key: sink.to_json() for sink in self.sinks.values()},
            "platformMD": self.platformMD.to_json() if self.platformMD else None
        })
        return rc

    def __eq__(self, other: object) -> bool:
        return ANSI_SQL_NamedObject.__eq__(self, other) and Documentable.__eq__(self, other) and \
            isinstance(other, DatasetGroup) and self.platformMD == other.platformMD and \
            self.sinks == other.sinks

    def __hash__(self) -> int:
        return hash((self.name, tuple(self.sinks.items()), self.platformMD))

    def lint(self, eco: Ecosystem, team: Team, ws: 'Workspace', tree: ValidationTree):
        super().nameLint(tree)
        if (self.documentation):
            self.documentation.lint(tree)
        if not is_valid_sql_identifier(self.name):
            tree.addRaw(NameMustBeSQLIdentifier(f"DatasetGroup name {self.name}", ProblemSeverity.ERROR))
        for sink in self.sinks.values():
            sinkTree: ValidationTree = tree.addSubTree(sink)
            sink.lint(eco, team, ws, self, sinkTree)

        # If a DSG has a platformMD, it must choose a platform and that platform must perfectly match the same named platform in the Ecosystem
        if (self.platformMD):
            # PlatformChooser needs to choose a platform and that platform must perfectly match the same named platform in the Ecosystem
            platform: Optional[DataPlatform] = self.platformMD.choooseDataPlatform(eco)
            if (platform is None):
                tree.addProblem("DSG doesnt choose a dataplatform")
            else:
                ecoPlat: Optional[DataPlatform] = eco.dataPlatforms.get(platform.name)
                if (ecoPlat is None):
                    tree.addRaw(UnknownObjectReference(f"DataPlatform {platform.name} is not in the Ecosystem", ProblemSeverity.ERROR))
                else:
                    if (ecoPlat != platform):
                        tree.addProblem("DSG chooses a platform which is different from the same named platform in the Ecosystem", ProblemSeverity.ERROR)
                # Now check the platform is happy with the containers for the Workspace
                if (ws.dataContainer):
                    ws.dataContainer.lint(eco, tree)
                    if (not platform.isWorkspaceDataContainerSupported(eco, ws.dataContainer)):
                        tree.addProblem(f"DataPlatform {platform.name} does not support the Workspace data container {ws.dataContainer.name}",
                                        ProblemSeverity.ERROR)
        if (len(self.sinks) == 0):
            tree.addRaw(AttributeNotSet("No datasetsinks in group"))

    def __str__(self) -> str:
        return f"DatasetGroup({self.name})"


class TransformerTrigger(JSONable):
    def __init__(self, name: str):
        JSONable.__init__(self)
        self.name: str = name

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc

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

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "trigger": self.trigger.to_json()})
        return rc


class CodeArtifact(UserDSLObject):
    """This defines a piece of code which can be used to transform data in a workspace"""

    def __init__(self):
        UserDSLObject.__init__(self)

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

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
        super().__init__()
        self.requirements: list[str] = requirements
        self.envVars: dict[str, str] = envVars
        self.requiredVersion: str = requiredVersion

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update(
            {
                "_type": self.__class__.__name__,
                "requirements": self.requirements,
                "envVars": self.envVars,
                "requiredVersion": self.requiredVersion
            }
        )
        return rc

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


class CodeExecutionEnvironment(PlatformService, JSONable):
    """This is an environment which can execute code, Spark/Flink/MR Jobs etc. The RenderEngine
    needs to support this CEE if a DataTransformer needs it to execute a CodeArtifact."""
    def __init__(self, name: str, loc: set[LocationKey]):
        PlatformService.__init__(self, name)
        JSONable.__init__(self)
        self.location: set[LocationKey] = loc

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = PlatformService.to_json(self)
        rc.update({"_type": self.__class__.__name__})
        rc.update({"locations": [loc.to_json() for loc in self.location]})
        return rc

    def __eq__(self, o: object) -> bool:
        return isinstance(o, CodeExecutionEnvironment) and self.location == o.location

    @abstractmethod
    def lint(self, eco: 'Ecosystem', tree: ValidationTree) -> None:
        for loc in self.location:
            ltree: ValidationTree = tree.addSubTree(loc)
            loc.lint(ltree)
            eco.lintLocationKey(loc, ltree)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.location})"

    @abstractmethod
    def isCodeArtifactSupported(self, eco: 'Ecosystem', ca: CodeArtifact) -> bool:
        """This checks if the code artifact can be run in this environment"""
        return False

    @abstractmethod
    def submitJob(self, job: CodeArtifact) -> dict[str, Any]:
        """This submits a job to this execution environment. It's typically called by a DataPlatform to execute a job. The job is
        described by the CodeArtifact"""
        pass


class DataTransformer(ANSI_SQL_NamedObject, Documentable, JSONable):
    """This allows new data to be produced from existing data. The inputs to the transformer are the
    datasets in the workspace and the output is a Datastore associated with the transformer. The transformer
    will be triggered using the specified trigger policy"""
    def __init__(self, name: str, store: Datastore, code: CodeArtifact,
                 doc: Optional[Documentation] = None, trigger: Optional[StepTrigger] = None) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        JSONable.__init__(self)
        # This Datastore is defined here and has a CaptureMetaData automatically added. Do not specify a CMD in the Datastore
        # This is done in the Team.addWorkspace method
        self.outputDatastore: Datastore = store
        self.code: CodeArtifact = code
        self.documentation = doc
        self.trigger: Optional[StepTrigger] = trigger

    def to_json(self) -> dict[str, Any]:
        json_dict: dict[str, Any] = ANSI_SQL_NamedObject.to_json(self)
        json_dict.update(Documentable.to_json(self))
        json_dict.update({"_type": self.__class__.__name__})
        json_dict.update({"outputDatastore": self.outputDatastore.to_json()})
        json_dict.update({"code": self.code.to_json()})
        if self.trigger is not None:
            json_dict.update({"trigger": self.trigger.to_json()})
        if self.documentation:
            json_dict["documentation"] = self.documentation.to_json()
        return json_dict

    def lint(self, eco: Ecosystem, ws: 'Workspace', tree: ValidationTree):
        ANSI_SQL_NamedObject.nameLint(self, tree)
        if (self.documentation):
            self.documentation.lint(tree)

        # The DSGs owned by the Workspace with a DataTransformer must not have a platformMD
        for dsg in ws.dsgs.values():
            if dsg.platformMD:
                tree.addRaw(
                    ConstraintViolation(
                        f"Workspace {ws.name} has a DataTransformer which is not allowed for a Workspace with a DSG {dsg.name}with a platformMD",
                        ProblemSeverity.ERROR))

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
            self.code.lint(eco, tree.addSubTree(self.code))
            # Output datastores must have a cmd which is a DataTransformerOutput
            if not isinstance(self.outputDatastore.cmd, DataTransformerOutput):
                tree.addRaw(ObjectNotSupportedByDataPlatform(self.outputDatastore.cmd, [DataTransformerOutput], ProblemSeverity.ERROR))
            if self.trigger:
                self.trigger.lint(eco, tree.addSubTree(self.trigger))

    def __eq__(self, o: object) -> bool:
        return ANSI_SQL_NamedObject.__eq__(self, o) and Documentable.__eq__(self, o) and \
            isinstance(o, DataTransformer) and self.outputDatastore == o.outputDatastore and self.code == o.code and \
            self.trigger == o.trigger


class WorkloadTier(Enum):
    """This is a relative priority of a Workspace against other Workspaces. This priority propogates backwards to producers whose data a Workspace
    uses. Thus, producers don't set the priority of their data, it's determined by the priority of whose is using it."""
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3
    UNKNOWN = 4


class WorkspacePriority(UserDSLObject):
    """This is a relative priority of a Workspace against other Workspaces. This priority propogates backwards to producers whose data a Workspace
    uses. Thus, producers don't set the priority of their data, it's determined by the priority of whose is using it."""
    def __init__(self):
        super().__init__()

    def to_json(self) -> dict[str, Any]:
        """Base implementation that subclasses can extend"""
        return {"_type": self.__class__.__name__}

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

    @abstractmethod
    def isMoreImportantThan(self, other: 'WorkspacePriority') -> bool:
        """This checks if this priority is more important than the other priority"""
        pass


class PrioritizedWorkloadTier(WorkspacePriority):
    """This uses a simple enum to determine priority"""
    def __init__(self, priority: WorkloadTier):
        super().__init__()
        self.priority: WorkloadTier = priority

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "priority": self.priority.name})
        return rc

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.priority})"

    def isMoreImportantThan(self, other: 'WorkspacePriority') -> bool:
        if isinstance(other, PrioritizedWorkloadTier):
            return self.priority.value < other.priority.value
        else:
            return super().isMoreImportantThan(other)


class Workspace(ANSI_SQL_NamedObject, Documentable, JSONable):
    """A collection of datasets used by a consumer for a specific use case. This consists of one or more groups of datasets with each set using
    the correct pipeline spec.
    Specific datasets can be present in multiple groups. They will be named differently in each group. The name needs to be ANSI SQL because
    it could be used as part of a SQL View/Table name in a Workspace database. Workspaces must have ecosystem unique names"""
    def __init__(self, name: str, *args: Union[DatasetGroup, DataContainer, Documentation, DataClassificationPolicy, ProductionStatus,
                                               DeprecationInfo, DataTransformer, WorkspacePriority]) -> None:
        ANSI_SQL_NamedObject.__init__(self, name)
        Documentable.__init__(self, None)
        JSONable.__init__(self)
        self.priority: WorkspacePriority = PrioritizedWorkloadTier(WorkloadTier.UNKNOWN)
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

    def to_json(self) -> dict[str, Any]:
        json_dict: dict[str, Any] = ANSI_SQL_NamedObject.to_json(self)
        json_dict.update(Documentable.to_json(self))
        json_dict.update({
            "_type": self.__class__.__name__,
            "datasetGroups": {name: dsg.to_json() for name, dsg in self.dsgs.items()},
            "productionStatus": self.productionStatus.name,
            "deprecationStatus": self.deprecationStatus.to_json(),
            "priority": self.priority.to_json()
        })
        if self.dataContainer:
            json_dict["dataContainer"] = self.dataContainer.to_json()
        if self.dataTransformer:
            json_dict["dataTransformer"] = self.dataTransformer.to_json()
        return json_dict

    def setTeam(self, key: TeamDeclarationKey):
        self.key = WorkspaceKey(key, self.name)

    def add(self, *args: Union[DatasetGroup, DataContainer, Documentation, DataClassificationPolicy, ProductionStatus,
                               DeprecationInfo, DataTransformer, WorkspacePriority]):
        for arg in args:
            if (isinstance(arg, WorkspacePriority)):
                self.priority = arg
            elif (isinstance(arg, DatasetGroup)):
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

    def __eq__(self, other: object) -> bool:
        return ANSI_SQL_NamedObject.__eq__(self, other) and Documentable.__eq__(self, other) and \
            isinstance(other, Workspace) and \
            self.priority == other.priority and self.dsgs == other.dsgs and self.dataContainer == other.dataContainer and \
            self.productionStatus == other.productionStatus and self.deprecationStatus == other.deprecationStatus and \
            self.dataTransformer == other.dataTransformer and self.classificationVerifier == other.classificationVerifier and \
            self.key == other.key

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
            cntTree: ValidationTree = tree.addSubTree(self.dataContainer)
            self.dataContainer.lint(eco, cntTree)

        # Check production status of workspace matches all datasets in use
        # Check deprecation status of workspace generates warnings for all datasets in use
        # Lint the DSGs
        for dsg in self.dsgs.values():
            dsgTree: ValidationTree = tree.addSubTree(dsg)
            dsg.lint(eco, t, self, dsgTree)

        # Link the transformer if present
        if self.dataTransformer:
            dtTree: ValidationTree = tree.addSubTree(self.dataTransformer)
            self.dataTransformer.lint(eco, self, dtTree)

    def __str__(self) -> str:
        return f"Workspace({self.name})"


class PlatformStyle(Enum):
    OLTP = 0
    OLAP = 1
    COLUMNAR = 2
    OBJECT = 3


class PipelineNode(InternalLintableObject, JSONable):
    """This is a named node in the pipeline graph. It stores node common information and which nodes this node depends on and those that depend on this node"""
    def __init__(self, name: str, platform: DataPlatform):
        InternalLintableObject.__init__(self)
        JSONable.__init__(self)
        self.name: str = name
        self.platform: DataPlatform = platform
        # This node depends on this set of nodes
        self.leftHandNodes: dict[str, PipelineNode] = dict()
        # This set of nodes depend on this node
        self.rightHandNodes: dict[str, PipelineNode] = dict()
        self.priority: Optional[WorkspacePriority] = None

    def __str__(self) -> str:
        return f"{self.__class__.__name__}/{self.name}"

    def __eq__(self, o: object) -> bool:
        return InternalLintableObject.__eq__(self, o) and JSONable.__eq__(self, o) and isinstance(o, PipelineNode) and \
            self.name == o.name and self.leftHandNodes == o.leftHandNodes and \
            self.rightHandNodes == o.rightHandNodes and self.priority == o.priority and self.platform == o.platform

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = dict()
        rc.update({"_type": self.__class__.__name__,
                   "name": self.name,
                   "platform": self.platform.name,
                   "leftHandNodes": {str(node): node.to_json() for node in self.leftHandNodes.values()},
                   "rightHandNodes": {str(node): node.to_json() for node in self.rightHandNodes.values()},
                   "priority": self.priority.to_json() if self.priority else None})
        return rc

    def addRightHandNode(self, rhNode: 'PipelineNode'):
        """This records a node that depends on this node"""
        self.rightHandNodes[str(rhNode)] = rhNode
        rhNode.leftHandNodes[str(self)] = self

    def setPriority(self, proposedPriority: Optional[WorkspacePriority]):
        """This sets the priority of this node. If the proposed priority is more important than the current priority then it is set."""
        if proposedPriority is None:
            self.priority = None
        elif (self.priority is not None):
            if (not self.priority.isMoreImportantThan(proposedPriority)):
                self.priority = proposedPriority
        else:
            self.priority = proposedPriority


class ExportNode(PipelineNode):
    """This is a node which represents the export of a dataset from a Datastore to a DataContainer. The dataset data is then
    available to the consumer which owns the Workspace associated with the DataContainer."""
    def __init__(self, platform: DataPlatform, dataContainer: DataContainer, storeName: str, datasetName: str):
        super().__init__(f"Export/{platform.name}/{dataContainer.name}/{storeName}/{datasetName}", platform)
        self.dataContainer: DataContainer = dataContainer
        self.storeName: str = storeName
        self.datasetName: str = datasetName

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, ExportNode) and self.dataContainer == o.dataContainer and \
            self.storeName == o.storeName and self.datasetName == o.datasetName

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__,
                   "dataContainer": self.dataContainer.to_json(),
                   "storeName": self.storeName,
                   "datasetName": self.datasetName})
        return rc


class IngestionNode(PipelineNode):
    """This is a super class node for ingestion nodes. It represents an ingestion stream source for a pipeline."""
    def __init__(self, name: str, platform: DataPlatform, storeName: str, captureTrigger: Optional[StepTrigger]):
        super().__init__(name, platform)
        self.storeName: str = storeName
        self.captureTrigger: Optional[StepTrigger] = captureTrigger

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, IngestionNode) and self.storeName == o.storeName and self.captureTrigger == o.captureTrigger

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__,
                   "storeName": self.storeName,
                   "captureTrigger": self.captureTrigger.to_json() if self.captureTrigger else None})
        return rc


class IngestionMultiNode(IngestionNode):
    """This is a node which represents the ingestion of multiple datasets from a Datastore. Such as Datastore might have N datasets and
    all N datasets are ingested together, transactionally in to a pipeline graph."""
    def __init__(self, platform: DataPlatform, storeName: str, captureTrigger: Optional[StepTrigger]):
        super().__init__(f"Ingest/{platform.name}/{storeName}", platform, storeName, captureTrigger)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, IngestionMultiNode)

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc


class IngestionSingleNode(IngestionNode):
    """This is a node which represents the ingestion of a single dataset from a Datastore. Such as Datastore might have N datasets
    and each of the datasets is ingested independently in to the pipeline. This node represents the ingestion of a single dataset"""
    def __init__(self, platform: DataPlatform, storeName: str, dataset: str, captureTrigger: Optional[StepTrigger]):
        super().__init__(f"Ingest/{platform.name}/{storeName}/{dataset}", platform, storeName, captureTrigger)
        self.datasetName: str = dataset

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, IngestionSingleNode) and self.datasetName == o.datasetName

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__,
                   "datasetName": self.datasetName})
        return rc


class TriggerNode(PipelineNode):
    """This is a node which represents the trigger for a DataTransformer. The trigger is a join on all the exports to a single Workspace."""
    def __init__(self, w: Workspace, platform: DataPlatform):
        super().__init__(f"Trigger/{platform.name}/{w.name}", platform)
        self.workspace: Workspace = w

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, TriggerNode) and self.workspace == o.workspace

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__,
                   "workspace": self.workspace.name})
        return rc


class DataTransformerNode(PipelineNode):
    """This is a node which represents the execution of a DataTransformer in the pipeline graph. The Datatransformer
    should 'execute' and its outputs can be found in the output Datastore for the datatransformer."""
    def __init__(self, ws: Workspace, platform: DataPlatform):
        super().__init__(f"DataTransformer/{platform.name}/{ws.name}", platform)
        self.workspace: Workspace = ws

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, DataTransformerNode) and self.workspace == o.workspace

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__,
                   "workspace": self.workspace.name})
        return rc


class DSGRootNode(JSONable):
    """This represents a target for a DataPlatform. A DataPlatforms purpose is to hydrated and maintain
    the datasets for a DatasetGroup. A Workspace owns one or more DatasetGroups and all datasets used in
    its DatasetGroups must be exported to the DataContainer used by the Workspace."""
    def __init__(self, w: Workspace, dsg: DatasetGroup):
        self.workspace: Workspace = w
        self.dsg: DatasetGroup = dsg

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, o: object) -> bool:
        return isinstance(o, DSGRootNode) and self.workspace == o.workspace and self.dsg == o.dsg

    def __str__(self) -> str:
        return f"{self.workspace.name}/{self.dsg.name}"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "workspace": self.workspace.name, "dsg": self.dsg.name})
        return rc


class PlatformPipelineGraph(InternalLintableObject):
    """This should be all the information a DataPlatform needs to render the processing pipeline graph. This would include
    provisioning Workspace views, provisioning dataContainer tables. Exporting data to dataContainer tables. Ingesting data from datastores,
    executing data transformers. We always build the graph starting on the right hand side, the consumer side which is typically a
    Workspace. We work left-wards towards data producers using the datasets used in DatasetGroups in the Workspace. Any datasets
    used by a Workspace need to have exports to the Workspace for those datasets. All datasets exported must also have been
    ingested. So we add an ingestion step. If a dataset is produced by a DataTransformer then we need to have the ingestion
    triggered by the execution of the DataTransformer. The DataTransformer is triggered itself by exports to the Workspace which
    owns it.
    Thus the right hand side of the graph should all be Exports to Workspaces. The left hand side should be
    all ingestion steps. Every ingestion should have a right hand side node which are exports to Workspaces.
    Exports will have a trigger for each dataset used by a DataTransformer. The trigger will be a join on all the exports to
    a single Workspace and will always trigger a DataTransformer node. The Datatransformer node will have an ingestion
    node for its output Datastore and then that Datastore should be exported to the Workspaces which use that Datastore.

    The graph is expected to be very large. BillyN has worked with graphs ingesting data from 12k datastores, millions of datasets, running thousands
    of transformers and having 2-3000 Workspaces. This resulted in the ingestion of data of about 30Tb per day of parquet compressed staging data.

    The priority of each node should be the highest priority of the Workspaces which depend on it."""

    def __init__(self, eco: Ecosystem, platform: DataPlatform):
        InternalLintableObject.__init__(self)
        self.platform: DataPlatform = platform
        self.eco: Ecosystem = eco
        self.workspaces: dict[str, Workspace] = dict()
        # All DSGs per Platform
        self.roots: set[DSGRootNode] = set()
        # This tracks which DatasetGroups are consumers of a DataContainer. This is necessary because
        # The DataPlatform may need to create view objects for each DatasetSink in the DataContainer
        # pointed at the underlying raw table
        self.dataContainerConsumers: dict[DataContainer, set[tuple[Workspace, DatasetGroup]]] = dict()

        # These are all the datastores used in the pipelinegraph for this platform. Note, this may be
        # a subset of the datastores in total in the ecosystem
        self.storesToIngest: set[str] = set()

        # This is the set of ALL nodes in this platforms pipeline graph
        self.nodes: dict[str, PipelineNode] = dict()

        # Cycle detection for DataTransformer self-references to prevent infinite recursion
        self._datatransformer_processing: set[str] = set()

    def __str__(self) -> str:
        return f"PlatformPipelineGraph({self.platform.name})"

    def generateGraph(self):
        """This generates the pipeline graph for the platform. This is a directed graph with nodes representing
        ingestion, export, trigger, and data transformation operations"""
        self.dataContainerConsumers = dict()
        self.storesToIngest = set()
        # Reset cycle detection state for fresh graph generation
        self._datatransformer_processing = set()

        # Split DSGs by Asset hosting Workspaces
        for dsg in self.roots:
            if dsg.workspace.dataContainer:
                dataContainer: DataContainer = dsg.workspace.dataContainer
                if self.dataContainerConsumers.get(dataContainer) is None:
                    self.dataContainerConsumers[dataContainer] = set()
                self.dataContainerConsumers[dataContainer].add((dsg.workspace, dsg.dsg))

        # Now collect stores to ingest per platform
        for consumers in self.dataContainerConsumers.values():
            for _, dsg in consumers:
                for sink in dsg.sinks.values():
                    self.storesToIngest.add(sink.storeName)

        # Make ingestion steps for every store used by platform
        for store in self.storesToIngest:
            self.createIngestionStep(store)

        # Now build pipeline graph backwards from workspaces used by platform and stores used by platform
        for dataContainer, consumers in self.dataContainerConsumers.items():
            for _, dsg in consumers:
                for sink in dsg.sinks.values():
                    exportStep: PipelineNode = ExportNode(self.platform, dataContainer, sink.storeName, sink.datasetName)
                    # If export doesn't already exist then create and add to ingestion job
                    if (self.nodes.get(str(exportStep)) is None):
                        self.nodes[str(exportStep)] = exportStep
                        self.addExportToPriorIngestion(exportStep)

    def findAllExportNodesForWorkspace(self, workspace: Workspace) -> set[ExportNode]:
        """This finds all the export nodes for a workspace"""
        dc: Optional[DataContainer] = workspace.dataContainer
        # No data container means no exports
        if dc is None:
            return set()
        # Find all the DatasetGroups that use this data container
        dsgSet: set[tuple[Workspace, DatasetGroup]] = self.dataContainerConsumers[dc]
        exportNodes: set[ExportNode] = set()
        for w, dsg in dsgSet:
            if w == workspace:
                for sink in dsg.sinks.values():
                    # Find the export node for this sink
                    exportNode: ExportNode = ExportNode(self.platform, dc, sink.storeName, sink.datasetName)
                    exportNodes.add(cast(ExportNode, self.nodes[str(exportNode)]))
        return exportNodes

    def findExistingOrCreateStep(self, step: PipelineNode) -> PipelineNode:
        """This finds an existing step or adds it to the set of steps in the graph"""
        if self.nodes.get(str(step)) is None:
            self.nodes[str(step)] = step
        else:
            step = self.nodes[str(step)]
        return step

    def createIngestionStep(self, storeName: str):
        """This creates a step to ingest data for a datastore. This results in either a single step for a multi-dataset store
        or one step per dataset in the single dataset stores"""
        store: Datastore = self.eco.cache_getDatastoreOrThrow(storeName).datastore

        if store.cmd:
            if (store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET):
                for datasetName in store.datasets.keys():
                    self.findExistingOrCreateStep(IngestionSingleNode(self.platform, storeName, datasetName, store.cmd.stepTrigger))
            else:  # MULTI_DATASET
                self.findExistingOrCreateStep(IngestionMultiNode(self.platform, storeName, store.cmd.stepTrigger))
        else:
            raise Exception(f"Store {storeName} cmd is None")

    def createIngestionStepForDataStore(self, store: Datastore, exportStep: ExportNode) -> PipelineNode:
        # Create a step for a single or multi dataset ingestion
        ingestionStep: Optional[PipelineNode] = None
        if store.cmd is None:
            raise Exception(f"Store {store.name} cmd is None")
        if (store.cmd.singleOrMultiDatasetIngestion == IngestionConsistencyType.SINGLE_DATASET):
            ingestionStep = IngestionSingleNode(exportStep.platform, exportStep.storeName, exportStep.datasetName, store.cmd.stepTrigger)
        else:  # MULTI_DATASET
            ingestionStep = IngestionMultiNode(exportStep.platform, exportStep.storeName, store.cmd.stepTrigger)
        ingestionStep = self.findExistingOrCreateStep(ingestionStep)
        return ingestionStep

    def createGraphForDataTransformer(self, dt: DataTransformerOutput, exportStep: ExportNode) -> None:
        """If a store is the output for a DataTransformer then we need to ingest it from the Workspace
        which defines the DataTransformer."""

        # Cycle detection: prevent infinite recursion for DataTransformer self-references
        transformer_key = f"{dt.workSpaceName}:{exportStep.storeName}"
        if transformer_key in self._datatransformer_processing:
            return  # Skip to avoid infinite recursion

        self._datatransformer_processing.add(transformer_key)
        try:
            w: Workspace = self.eco.cache_getWorkspaceOrThrow(dt.workSpaceName).workspace
            if w.dataContainer:
                # Find/Create Trigger, this is a join on all incoming exports needed for the transformer
                dtStep: DataTransformerNode = cast(DataTransformerNode, self.findExistingOrCreateStep(DataTransformerNode(w, self.platform)))
                triggerStep: TriggerNode = cast(TriggerNode, self.findExistingOrCreateStep(TriggerNode(w, self.platform)))
                # Add ingestion for transfomer
                dtIngestStep: PipelineNode = self.findExistingOrCreateStep(IngestionMultiNode(self.platform, exportStep.storeName, None))
                dtStep.addRightHandNode(dtIngestStep)
                # Ingesting Transformer causes Export
                dtIngestStep.addRightHandNode(exportStep)
                # Trigger calls Transformer Step
                triggerStep.addRightHandNode(dtStep)
                # Add Exports to call trigger
                for dsgR in self.roots:
                    if dsgR.workspace == w:
                        for sink in dsgR.dsg.sinks.values():
                            dsrExportStep: ExportNode = cast(ExportNode, self.findExistingOrCreateStep(
                                    ExportNode(self.platform, w.dataContainer, sink.storeName, sink.datasetName)))
                            self.addExportToPriorIngestion(dsrExportStep)
                            # Add Trigger for DT after export
                            dsrExportStep.addRightHandNode(triggerStep)
        finally:
            # Clean up cycle detection state
            self._datatransformer_processing.discard(transformer_key)

    def addExportToPriorIngestion(self, exportStep: ExportNode):
        """This makes sure the ingestion steps for a the datasets in an export step exist"""
        assert (self.nodes.get(str(exportStep)) is not None)
        """Work backwards from export step. The normal chain is INGEST -> EXPORT. In the case of exporting a store from
        a transformer then it is INGEST -> EXPORT -> TRIGGER -> TRANSFORM -> INGEST -> EXPORT"""
        store: Datastore = self.eco.cache_getDatastoreOrThrow(exportStep.storeName).datastore
        if (store.cmd):
            # Create a step for a single or multi dataset ingestion
            ingestionStep: PipelineNode = self.createIngestionStepForDataStore(store, exportStep)
            ingestionStep.addRightHandNode(exportStep)
            # If this store is a transformer then we need to create the transformer job
            if isinstance(store.cmd, DataTransformerOutput):
                self.createGraphForDataTransformer(store.cmd, exportStep)

    def getLeftSideOfGraph(self) -> set[PipelineNode]:
        """This returns ingestions which don't depend on anything else, the left end of a pipeline"""
        rc: set[PipelineNode] = set()
        for step in self.nodes.values():
            if len(step.leftHandNodes) == 0:
                rc.add(step)
        return rc

    def getRightSideOfGraph(self) -> set[PipelineNode]:
        """This returns steps which does have other steps depending on them, the right end of a pipeline"""
        rc: set[PipelineNode] = set()
        for step in self.nodes.values():
            if len(step.rightHandNodes) == 0:
                rc.add(step)
        return rc

    def checkNextStepsForStepType(self, filterStep: Type[PipelineNode], targetStep: Type[PipelineNode]) -> bool:
        """This finds steps of a certain type and then checks that ALL follow on steps from it are a certain type"""
        for s in self.nodes:
            if isinstance(s, filterStep):
                for nextS in s.rightHandNodes:
                    if not isinstance(nextS, targetStep):
                        return False
        return True

    def graphToText(self) -> str:
        """This returns a string representation of the pipeline graph from left to right"""
        left_side = self.getLeftSideOfGraph()

        # Create a dictionary to keep track of the nodes we've visited
        visited = {node: False for node in self.nodes.values()}

        def dfs(node: PipelineNode, indent: str = '') -> str:
            """Depth-first search to traverse the graph and build the string representation"""
            if visited[node]:
                return str(node)
            visited[node] = True
            result = indent + str(node) + '\n'
            if node.rightHandNodes:
                result += ' -> ('
                result += ', '.join(dfs(n, indent + '  ') for n in node.rightHandNodes.values() if not visited[n])
                result += ')'
            return result

        # Start the traversal from each node on the left side
        graph_strs = [dfs(node) for node in left_side]

        return '\n'.join(graph_strs)

    def lintCredentials(self, credStore: CredentialStore, tree: ValidationTree) -> None:
        """This checks every Credential in the Graph is compatible with the CredentialStore"""
        for node in self.nodes.values():
            c: Optional[Credential] = None
            if isinstance(node, IngestionNode):
                store: Datastore = self.eco.cache_getDatastoreOrThrow(node.storeName).datastore
                if store.cmd is not None and isinstance(store.cmd, IngestionMetadata):
                    c = store.cmd.credential
            if c is not None:
                credStore.lintCredential(c, tree.addSubTree(node))

    def lint(self, credStore: CredentialStore, tree: ValidationTree) -> None:
        """This checks the pipeline graph for errors and warnings"""

        # Get the IaC renderer for the platform
        gHandler: DataPlatformGraphHandler = self.platform.createGraphHandler(self)

        # Lint the graph to check all nodes are valid with this platform
        # This checks for unsupported databases, vendors, transformers and so on
        gHandler.lintGraph(self.eco, credStore, tree.addSubTree(gHandler))
        # Iterate over every ingestion node and check the credentials are compatible with
        # the credential store
        self.lintCredentials(credStore, tree)

    def propagateWorkspacePriorities(self):
        """Propagates workspace priorities through the pipeline graph.
        Each node's priority will be set to the highest priority of any workspace that depends on it,
        either directly or indirectly through the dependency chain.

        The priority of a node is the highest priority of the right hand side nodes. Ingestion node priorities
        are set to the highest priority of the Workspaces that have export nodes that depend on it. Transformer
        node priorities dont depend on their associate Workspace but instead depend on the priority of the Workspaces
        that depend on data produced by the transformer.

        Some sample metrics on this graph. It can have 8000 datastores with a combined 12 million datasets. There
        could be 2500 Workspaces. There could be 5000 Transformers. Most Workspaces connect directly to a prime
        datastore. Some workspaces will use datasets from Transformer output Datastores. The longest chain of
        Transformers could be 2 or 3 in a row. So, while the left and right sides of the graph can be large, the
        width of the graph between left and right is under 20 nodes.

        The number of DataContainers is expected to be in the low hundreds with possible 50k datasets exported
        to each DataContainer.

        Implementation optimized for large scale (millions of nodes):
        1. Reset all node priorities
        2. Sort workspaces by priority (highest first)
        3. Set initial priorities on export nodes from highest priority workspaces first
        4. Set the priority of all export nodes and the propagate it to all left hand node if it is higher
        """
        # 1. Reset all node priorities
        for node in self.nodes.values():
            node.setPriority(None)

        # 2. Sort workspaces by priority (highest first)
        sorted_workspaces = sorted(
            self.workspaces.values(),
            key=lambda w: cast(PrioritizedWorkloadTier, w.priority).priority.value
        )

        def setLeftNodesPriority(node: PipelineNode, priority: WorkspacePriority, visited_nodes: Optional[set[str]] = None):
            """This sets the priority of a node and then recursively sets the priority of all left hand nodes"""
            if visited_nodes is None:
                visited_nodes = set()

            node_key = str(node)
            if node_key in visited_nodes:
                return  # Cycle detected - avoid infinite recursion

            visited_nodes.add(node_key)
            node.setPriority(priority)
            for left_node in node.leftHandNodes.values():
                # if left node priority is none or lower than the current priority then set it
                if left_node.priority is None or not left_node.priority.isMoreImportantThan(priority):
                    setLeftNodesPriority(left_node, priority, visited_nodes)

        # 3. Set initial priorities on export nodes, starting with highest priority workspaces
        for workspace in sorted_workspaces:
            export_nodes = self.findAllExportNodesForWorkspace(workspace)
            for export_node in export_nodes:
                # Set the priority of the export node
                self.nodes[str(export_node)].setPriority(workspace.priority)
                # Set the priority of all left hand nodes
                setLeftNodesPriority(self.nodes[str(export_node)], workspace.priority)


class EcosystemPipelineGraph(InternalLintableObject):
    """This is the total graph for an Ecosystem. It's a list of graphs keyed by DataPlatforms in use. One graph per DataPlatform"""
    def __init__(self, eco: Ecosystem):
        InternalLintableObject.__init__(self)
        self.eco: Ecosystem = eco

        # Store for each DP, the set of DSGRootNodes
        self.roots: dict[str, PlatformPipelineGraph] = dict()

        # Scan workspaces/dsg pairs, split by DataPlatform
        for w in eco.workSpaceCache.values():
            for dsg in w.workspace.dsgs.values():
                assignment: Optional[DatasetGroupDataPlatformAssignments] = eco.getDSGPlatformMapping(w.workspace.name, dsg.name)
                dpList: list[DataPlatform] = []
                if assignment is not None:
                    for assignee in assignment.assignments:
                        if assignee.status != DatasetGroupDataPlatformMappingStatus.DECOMMISSIONED:
                            dpList.append(eco.getDataPlatformOrThrow(assignee.dataPlatform.name))

                for p in dpList:
                    root: DSGRootNode = DSGRootNode(w.workspace, dsg)
                    if self.roots.get(p.name) is None:
                        self.roots[p.name] = PlatformPipelineGraph(eco, p)
                    self.roots[p.name].roots.add(root)
                    # Collect Workspaces using the platform
                    if (self.roots[p.name].workspaces.get(w.workspace.name) is None):
                        self.roots[p.name].workspaces[w.workspace.name] = w.workspace

        # Recursively auto-include DataTransformer workspaces and their dependencies
        for platform_name, platform_graph in self.roots.items():
            visited_workspaces: set[str] = set()
            workspace_queue: list[str] = []

            # Start with all currently assigned workspaces
            for root in list(platform_graph.roots):
                if root.workspace.name not in visited_workspaces:
                    workspace_queue.append(root.workspace.name)

            # Process queue until empty (handles recursive dependencies and cycles)
            while workspace_queue:
                current_workspace_name = workspace_queue.pop(0)
                if current_workspace_name in visited_workspaces:
                    continue  # Skip already processed workspaces (cycle detection)

                visited_workspaces.add(current_workspace_name)
                current_workspace_entry: Optional[WorkspaceCacheEntry] = eco.cache_getWorkspace(current_workspace_name)
                if current_workspace_entry is None:
                    continue  # Skip if workspace doesn't exist

                current_workspace: Workspace = current_workspace_entry.workspace

                # Check all sinks in this workspace for DataTransformer dependencies
                for dsg in current_workspace.dsgs.values():
                    for sink in dsg.sinks.values():
                        storeEntry: Optional[DatastoreCacheEntry] = eco.datastoreCache.get(sink.storeName)
                        if storeEntry is not None:
                            store: Datastore = storeEntry.datastore
                            if isinstance(store.cmd, DataTransformerOutput):
                                # This is a DataTransformer output - auto-include the producer workspace
                                dt_workspace_name: str = store.cmd.workSpaceName
                                if dt_workspace_name not in visited_workspaces:
                                    # Add to queue for recursive processing
                                    workspace_queue.append(dt_workspace_name)

                                    # Add to platform graph immediately
                                    dt_workspace_entry: Optional[WorkspaceCacheEntry] = eco.cache_getWorkspace(dt_workspace_name)
                                    if dt_workspace_entry is not None:
                                        dt_workspace: Workspace = dt_workspace_entry.workspace
                                        # Add all DSGs from the DataTransformer workspace to this platform
                                        for dt_dsg in dt_workspace.dsgs.values():
                                            dt_root: DSGRootNode = DSGRootNode(dt_workspace, dt_dsg)
                                            platform_graph.roots.add(dt_root)
                                        # Add the workspace to the platform's workspace collection
                                        if platform_graph.workspaces.get(dt_workspace.name) is None:
                                            platform_graph.workspaces[dt_workspace.name] = dt_workspace

        # Now track DSGs per dataContainer
        # For each platform what DSGs need to be exported to a given dataContainer
        for platform in self.roots.keys():
            pinfo = self.roots[platform]
            pinfo.generateGraph()
            pinfo.propagateWorkspacePriorities()

    def lint(self, credStore: CredentialStore, tree: ValidationTree) -> None:
        for p in self.roots.values():
            p.lint(credStore, tree.addSubTree(p))

    def __str__(self) -> str:
        return f"EcosystemPipelineGraph({self.eco.name})"


class IaCFragmentManager(Documentable):
    """This is a fragment manager for IaC. It is used to store fragments of IaC code which are generated for a pipeline
    graph."""
    def __init__(self, name: str, doc: Documentation):
        Documentable.__init__(self, doc)
        self.name: str = name

    @abstractmethod
    def preRender(self):
        """This is called before the rendering of the fragments. It can be used to set up the fragment manager."""
        pass

    @abstractmethod
    def postRender(self):
        """This is called after the rendering of the fragments. It can be used to clean up the fragment manager."""
        pass

    @abstractmethod
    def addFragment(self, node: PipelineNode, fragment: str):
        """Add a fragment to the fragment manager"""
        pass


class CombineToStringFragmentManager(IaCFragmentManager):
    def __init__(self, name: str, doc: Documentation):
        super().__init__(name, doc)
        # This is a dictionary of dictionaries. The first key is the group, the second key is the name, and the value is the fragment
        self.fragments: dict[Type[PipelineNode], dict[PipelineNode, str]] = {}

    def addFragment(self, node: PipelineNode, fragment: str):
        nodeType: Type[PipelineNode] = node.__class__
        if nodeType not in self.fragments:
            self.fragments[nodeType] = {}
        self.fragments[nodeType][node] = fragment

    def __str__(self) -> str:
        rc: str = ""
        for group in self.fragments:
            rc += f"Group: {group}\n"
            for name in self.fragments[group]:
                rc += f"  {name}: {self.fragments[group][name]}\n"
        return rc


def defaultPipelineNodeFileName(node: PipelineNode) -> str:
    """Calculate simple file names for pipeline nodes. These are used with an file extension for the particular
    IaC provider, e.g. .tf for Terraform or .yaml for Kubernetes. The file name is unique for each node."""
    if (isinstance(node, IngestionMultiNode)):
        return f"{node.__class__.__name__}_{node.storeName}"
    elif (isinstance(node, IngestionSingleNode)):
        return f"{node.__class__.__name__}_{node.storeName}_{node.datasetName}"
    elif (isinstance(node, ExportNode)):
        return f"{node.__class__.__name__}_{node.storeName}_{node.datasetName}"
    elif (isinstance(node, TriggerNode)):
        return f"{node.__class__.__name__}_{node.name}_{node.workspace.name}"
    elif (isinstance(node, DataTransformerNode)):
        return f"{node.__class__.__name__}_{node.workspace.name}_{node.name}"
    raise Exception(f"Unknown node type {node}")


class FileBasedFragmentManager(IaCFragmentManager):
    """This is a file based fragment manager. It writes the fragments to a temporary directory. The fragments are stored in files with the name of the node.
    The name of the node is determined by the fnGetFileNameForNode function. This function should return a unique name for each node.
    The fragments are stored in the rootDir directory."""
    def __init__(self, name: str, doc: Documentation, fnGetFileNameForNode: Callable[[PipelineNode], str]):
        super().__init__(name, doc)
        self.rootDir: str = tempfile.mkdtemp()
        self.fnGetFileNameForNode: Callable[[PipelineNode], str] = fnGetFileNameForNode

    def addFragment(self, node: PipelineNode, fragment: str):
        name: str = self.fnGetFileNameForNode(node)
        with open(f"{self.rootDir}/{name}", "w") as file:
            file.write(fragment)

    def addStaticFile(self, folder: str, name: str, fragment: str):
        """Add a static file to the fragment manager. This is useful for adding files like
        provider.tf or variables.tf which are not associated with a particular node."""
        # Create the folder if it does not exist
        full_folder_path: str = f"{self.rootDir}/{folder}" if len(folder) > 0 else self.rootDir
        if not os.path.exists(full_folder_path):
            os.makedirs(full_folder_path)
        with open(f"{full_folder_path}/{name}", "w") as file:
            file.write(fragment)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, rootDir={self.rootDir})"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, FileBasedFragmentManager) and self.rootDir == other.rootDir and \
            self.fnGetFileNameForNode == other.fnGetFileNameForNode

    def __hash__(self) -> int:
        return hash(self.name)


class DataPlatformGraphHandler(InternalLintableObject):
    """This is a base class for DataPlatform code for handling a specific intention graph."""
    def __init__(self, graph: PlatformPipelineGraph):
        InternalLintableObject.__init__(self)
        self.graph: PlatformPipelineGraph = graph

    @abstractmethod
    def getInternalDataContainers(self) -> set[DataContainer]:
        """This returns all the internal DataContainers created by this DataPlatform to
        execute the pipelines for the indicated graph. These is meant for internal containers not
        for containers for Datastores or Workspaces"""
        pass

    @abstractmethod
    def lintGraph(self, eco: Ecosystem, credStore: CredentialStore, tree: ValidationTree):
        """This checks the pipeline graph for errors and warnings. It should also check that the platform
        can handle every node in the pipeline graph. Nodes may fail because there is no supported. The
        CredentialStore is provided so Credentials can check they are supported."""
        pass

    @abstractmethod
    def renderGraph(self, credStore: CredentialStore, issueTree: ValidationTree) -> dict[str, str]:
        """This is called by the RenderEngine to instruct a DataPlatform to render the
        intention graph that it manages. It returns a dictionary of file names and their contents."""
        pass


class PlatformPipelineGraphLinter(ABC):
    """This is a base class for linting a pipeline graph"""
    def __init__(self, graph: PlatformPipelineGraph):
        self.graph: PlatformPipelineGraph = graph

    @abstractmethod
    def lintIngestionSingleNode(self, eco: Ecosystem, node: IngestionSingleNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintIngestionMultiNode(self, eco: Ecosystem, node: IngestionMultiNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintExportNode(self, eco: Ecosystem, node: ExportNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintTriggerNode(self, eco: Ecosystem, node: TriggerNode, tree: ValidationTree) -> None:
        pass

    @abstractmethod
    def lintDataTransformerNode(self, eco: Ecosystem, node: DataTransformerNode, tree: ValidationTree) -> None:
        pass

    def lintGraph(self, eco: Ecosystem, credStore: CredentialStore, tree: ValidationTree):
        """Check that the platform can handle every node in the pipeline graph. Nodes may fail because there is no supported
        connector for an ingestion or export node or because there are missing parameters or because a certain type of
        trigger or data transformer is not supported. It may also fail because an infrastructure vendor or datacontainer is not supported"""
        for node in self.graph.nodes.values():
            if (isinstance(node, IngestionSingleNode)):
                self.lintIngestionSingleNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, IngestionMultiNode)):
                self.lintIngestionMultiNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, ExportNode)):
                self.lintExportNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, TriggerNode)):
                self.lintTriggerNode(eco, node, tree.addSubTree(node))
            elif (isinstance(node, DataTransformerNode)):
                self.lintDataTransformerNode(eco, node, tree.addSubTree(node))


class IaCDataPlatformRenderer(DataPlatformGraphHandler, PlatformPipelineGraphLinter):
    """This is intended to be a base class for IaC style DataPlatforms which render the intention graph
    to an IaC format. The various nodes in the graph are rendered as seperate files in a temporary folder
    which remains after the graph is rendered. The folder can then be committed to a CI/CD repository where
    it can be used by a platform like Terraform to effect the changes in the graph."""
    def __init__(self, executor: DataPlatformExecutor, graph: PlatformPipelineGraph):
        DataPlatformGraphHandler.__init__(self, graph)
        PlatformPipelineGraphLinter.__init__(self, graph)
        self.executor: DataPlatformExecutor = executor

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, IaCDataPlatformRenderer) and self.executor == other.executor and \
            self.graph == other.graph

    def renderIaC(self, fragments: IaCFragmentManager) -> IaCFragmentManager:
        fragments.preRender()
        """This renders the IaC for the given graph"""
        for node in self.graph.nodes.values():
            if isinstance(node, IngestionSingleNode):
                fragments.addFragment(node, self.renderIngestionSingle(node))
            elif isinstance(node, IngestionMultiNode):
                fragments.addFragment(node, self.renderIngestionMulti(node))
            elif isinstance(node, ExportNode):
                fragments.addFragment(node, self.renderExport(node))
            elif isinstance(node, TriggerNode):
                fragments.addFragment(node, self.renderTrigger(node))
            elif isinstance(node, DataTransformerNode):
                fragments.addFragment(node, self.renderDataTransformer(node))
            else:
                raise Exception(f"Unknown node type {node.__class__.__name__}")
        fragments.postRender()
        return fragments

    @abstractmethod
    def renderIngestionSingle(self, ingestNode: IngestionSingleNode) -> str:
        pass

    @abstractmethod
    def renderIngestionMulti(self, ingestNode: IngestionMultiNode) -> str:
        pass

    @abstractmethod
    def renderExport(self, exportNode: ExportNode) -> str:
        pass

    @abstractmethod
    def renderTrigger(self, triggerNode: TriggerNode) -> str:
        pass

    @abstractmethod
    def renderDataTransformer(self, dtNode: DataTransformerNode) -> str:
        pass

    def getDataContainerForDatastore(self, storeName: str) -> Optional[DataContainer]:
        """Get the data container for a given datastore"""
        storeEntry: DatastoreCacheEntry = self.graph.eco.cache_getDatastoreOrThrow(storeName)
        if (storeEntry.datastore.cmd is not None):
            return storeEntry.datastore.cmd.dataContainer
        else:
            return None

    def isDataContainerSupported(self, dc: DataContainer, allowedContainers: set[Type[DataContainer]]) -> bool:
        """Check if a data container is supported"""
        return dc.__class__ in allowedContainers


class IaCDataPlatformRendererShim(IaCDataPlatformRenderer):
    """This is a shim for during development. It does nothing and is used to test the IaCDataPlatformRenderer interface"""
    def __init__(self, executor: DataPlatformExecutor, graph: PlatformPipelineGraph):
        super().__init__(executor, graph)

    def renderIngestionSingle(self, ingestNode: IngestionSingleNode) -> str:
        return ""

    def renderIngestionMulti(self, ingestNode: IngestionMultiNode) -> str:
        return ""

    def renderExport(self, exportNode: ExportNode) -> str:
        return ""

    def renderTrigger(self, triggerNode: TriggerNode) -> str:
        return ""

    def renderDataTransformer(self, dtNode: DataTransformerNode) -> str:
        return ""

    def lintIngestionSingleNode(self, eco: Ecosystem, node: IngestionSingleNode, tree: ValidationTree) -> None:
        pass

    def lintIngestionMultiNode(self, eco: Ecosystem, node: IngestionMultiNode, tree: ValidationTree) -> None:
        pass

    def lintExportNode(self, eco: Ecosystem, node: ExportNode, tree: ValidationTree) -> None:
        pass

    def lintTriggerNode(self, eco: Ecosystem, node: TriggerNode, tree: ValidationTree) -> None:
        pass

    def lintDataTransformerNode(self, eco: Ecosystem, node: DataTransformerNode, tree: ValidationTree) -> None:
        pass

    def getInternalDataContainers(self) -> set[DataContainer]:
        raise NotImplementedError("This is a shim")


class UnsupportedDataContainer(ValidationProblem):
    def __init__(self, dc: DataContainer):
        super().__init__(f"DataContainer {dc} is not supported", ProblemSeverity.ERROR)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, UnsupportedDataContainer)

    def __hash__(self) -> int:
        return hash(self.description)


class InfraStructureLocationPolicy(AllowDisallowPolicy[LocationKey]):
    """Allows a GZ to police which locations can be used with datastores or workspaces within itself"""
    def __init__(self, name: str, doc: Documentation, allowed: Optional[set[LocationKey]] = None,
                 notAllowed: Optional[set[LocationKey]] = None):
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

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc


class InfraStructureVendorPolicy(AllowDisallowPolicy[VendorKey]):
    """Allows a GZ to police which vendors can be used with datastore or workspaces within itself"""
    def __init__(self, name: str, doc: Documentation, allowed: Optional[set[VendorKey]] = None,
                 notAllowed: Optional[set[VendorKey]] = None):
        super().__init__(name, doc, allowed, notAllowed)

    def __str__(self):
        return f"InfraStructureVendorPolicy({self.name})"

    def __eq__(self, v: object) -> bool:
        if not super().__eq__(v) or not isinstance(v, InfraStructureVendorPolicy):
            return False
        return self.allowed == v.allowed and self.notAllowed == v.notAllowed

    def __hash__(self) -> int:
        return super().__hash__()

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "name": self.name})
        return rc
