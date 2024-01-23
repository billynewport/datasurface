from dataclasses import dataclass
from collections import OrderedDict
import re
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence, TypeVar, Union, cast
from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from typing import Optional, TypeVar, Generic

from .Documentation import Documentation

from .utils import ANSI_SQL_NamedObject, Policy, is_valid_hostname_or_ip, is_valid_sql_identifier
from .Schema import DataClassification, DataClassificationPolicy, Schema
from .Exceptions import AttributeAlreadySetException, ObjectAlreadyExistsException, ObjectDoesntExistException, UnknownArgumentException, DatastoreDoesntExistException, AssetDoesntExistException, WorkspaceDoesntExistException
from .Lint import ProblemSeverity, ValidationTree

class ProductionStatus(Enum):
    """This indicates whether the team is in production or not"""
    PRODUCTION = 0
    NOT_PRODUCTION = 1

class DeprecationStatus(Enum):
    """This indicates whether the team is deprecated or not"""
    NOT_DEPRECATED = 0
    DEPRECATED = 1

class DeprecationInfo:
    """This is the deprecation information for an object"""
    def __init__(self, status : DeprecationStatus, reason : Optional[Documentation] = None) -> None:
        self.status : DeprecationStatus = status
        """If it deprecated or not"""
        self.reason : Optional[Documentation] = reason
        """If deprecated then this explains why and what an existing user should do, alternative dataset for example"""

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, DeprecationInfo) and self.status == __value.status and self.reason == __value.reason 

class GitControlledObject(ABC):
    """This is the base class for all objects which are controlled by a git repository"""
    def __init__(self, repo : 'Repository') -> None:
        self.owningRepo : Repository = repo
        """This is the repository which is authorized to make changes to this object"""

    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, GitControlledObject)):
            return self.owningRepo == __value.owningRepo
        else:
            return False
        
    @abstractmethod
    def eq_toplevel(self, proposed : 'GitControlledObject') -> bool:
        """This should compare attributes which are locally authorized only"""
        return True
    
    def superLint(self, tree : ValidationTree):
        rTree : ValidationTree = tree.createChild(self.owningRepo)
        self.owningRepo.lint(rTree)
    
    def checkTopLevelAttributeChangesAreAuthorized(self, proposed : 'GitControlledObject', changeSource : 'Repository', vTree : ValidationTree) -> None:
        """This checks if the local attributes of the object have been modified by the authorized change source"""
        # Check if the ecosystem has been modified at all
        if(self == proposed):
            return
        elif(not self.eq_toplevel(proposed) and self.owningRepo != changeSource):
            vTree.addProblem(f"{self} top level has been modified by an unauthorized source")

    @abstractmethod    
    def checkIfChangesAreAuthorized(self, proposed : 'GitControlledObject', changeSource : 'Repository', vTree : ValidationTree) -> None:
        """This checks if the differences between the current and proposed objects are authorized by the specified change source"""
        raise NotImplementedError()
        
    def checkDictChangesAreAuthorized(self, current : Mapping[str, 'GitControlledObject'], proposed : Mapping[str, 'GitControlledObject'], changeSource : 'Repository', vTree : ValidationTree) -> None:
        """This checks if the current dict has been modified relative to the specified change source"""
        """This checks if any objects has been added or removed relative to e"""


        # Get the object keys from the current main ecosystem
        current_keys : set[str] = set(current)

        # Get the object keys from the proposed ecosystem
        proposed_keys : set[str] = set(proposed.keys())

        deleted_keys : set[str] = current_keys - proposed_keys
        added_keys : set[str] = proposed_keys - current_keys

        # first check any top level objects have been added or removed by the correct change sources
        for key in deleted_keys:
            # Check if the object was deleted by the authoized change source
            obj : Optional[GitControlledObject] = current[key]
            if(obj.owningRepo != changeSource):
                vTree.addProblem(f"Key {key} has been deleted by an unauthorized source")
        
        for key in added_keys:
            # Check if the object was added by the specified change source
            obj : Optional[GitControlledObject] = proposed[key]
            if(obj.owningRepo != changeSource):
                vTree.addProblem(f"Key {key} has been added by an unauthorized source")

        # Now check each common object for changes
        common_keys : set[str] = current_keys.intersection(proposed_keys)
        for key in common_keys:
            prop : Optional[GitControlledObject] = proposed[key]
            curr : Optional[GitControlledObject] = current[key]
            # Check prop against curr for unauthorized changes
            cTree : ValidationTree = vTree.createChild(curr)
            curr.checkIfChangesAreAuthorized(prop, changeSource, cTree)


def cyclic_safe_eq(a : object, b: object, visited : set[object]) -> bool:
    """This is a recursive equality checker which avoids infinite recursion by tracking visited objects. The \
        meta data objects have circular references which cause infinite recursion when using the default"""
    ida : int = id(a)
    idb : int = id(b)

    if(ida == idb):
        return True
    
    if(type(b) is not type(a)):
        return False
    
    if(idb > ida):
        ida, idb = idb, ida

    pair = (ida, idb)
    if(pair in visited):
        return True
    
    visited.add(pair)

    # Handle comparing dict objects
    if isinstance(a, dict) and isinstance(b, dict):
        d_a : dict[Any, Any] = a
        d_b : dict[Any, Any] = b

        if len(d_a) != len(d_b):
            return False
        for key in d_a:
            if key not in b or not cyclic_safe_eq(d_a[key], d_b[key], visited):
                return False
        return True

    # Handle comparing list objects
    if isinstance(a, list) and isinstance(b, list):
        l_a : list[Any] = a
        l_b : list[Any] = b

        if len(l_a) != len(l_b):
            return False
        for item_a, item_b in zip(l_a, l_b):
            if not cyclic_safe_eq(item_a, item_b, visited):
                return False
        return True

    # Now compare objects for equality
    try:
        self_vars : dict[str, Any] = vars(a)
    except TypeError:
        # This is a primitive type
        return a == b

    # Check same named attributes for equality    
    for attr, value in vars(b).items():
        if(not attr.startswith("_")):
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
    def __init__(self, ecoName : str) -> None:
        self.ecoName : str = ecoName

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, EcosystemKey) and self.ecoName == __value.ecoName
    
    def __str__(self) -> str:
        return f"Ecosystem({self.ecoName})"

    def __hash__(self) -> int:
        return hash(str(self))

class GovernanceZoneKey(EcosystemKey):
    """Soft link to a governance zone"""
    def __init__(self, e : EcosystemKey, gz : str) -> None:
        super().__init__(e.ecoName)
        self.gzName : str = gz

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, GovernanceZoneKey) and self.gzName == __value.gzName
    
    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return super().__str__() + f".GovernanceZone({self.gzName})"

class StoragePolicyKey(GovernanceZoneKey):
    """Soft link to a storage policy"""
    def __init__(self, gz : GovernanceZoneKey, policyName : str):
        super().__init__(gz, gz.gzName)
        self.policyName : str = policyName

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, StoragePolicyKey) and self.policyName == __value.policyName
    
    def __str__(self) -> str:
        return super().__str__() + f".StoragePolicy({self.policyName})"

    def __hash__(self) -> int:
        return hash(str(self))

class InfrastructureVendorKey(GovernanceZoneKey):
    """Soft link to an infrastructure vendor"""
    def __init__(self, gz : GovernanceZoneKey, iv : str) -> None:
        super().__init__(gz, gz.gzName)
        self.ivName : str = iv

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, InfrastructureVendorKey) and self.ivName == __value.ivName
    
    def __str__(self) -> str:
        return super().__str__() + f".InfrastructureVendor({self.ivName})"
    
    def __hash__(self) -> int:
        return hash(str(self))

class InfraLocationKey(InfrastructureVendorKey):
    """Soft link to an infrastructure location"""
    def __init__(self, iv : InfrastructureVendorKey, loc : list[str]) -> None:
        super().__init__(iv, iv.ivName)
        self.locationPath : list[str] = loc

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, InfraLocationKey) and self.locationPath == __value.locationPath
    
    def __str__(self) -> str:
        return super().__str__() + f".InfraLocation({self.locationPath})"

    def __hash__(self) -> int:
        return hash(str(self))


class TeamDeclarationKey(GovernanceZoneKey):
    """Soft link to a team declaration"""
    def __init__(self, gz : GovernanceZoneKey, td : str) -> None:
        super().__init__(gz, gz.gzName)
        self.tdName : str = td

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TeamDeclarationKey) and self.tdName == __value.tdName
    
    def __str__(self) -> str:
        return super().__str__() + f".TeamDeclaration({self.tdName})"
    
class DatastoreKey(TeamDeclarationKey):
    """Soft link to a datastore"""
    def __init__(self, td : TeamDeclarationKey, ds : str) -> None:
        super().__init__(td, td.tdName)
        self.dsName : str = ds

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
    '''This is the base class for storage policies. These are owned by a governance zone and are used to determine whether a container is compatible with the policy.'''

    def __init__(self, name : str, isMandatory : PolicyMandatedRule, doc : Optional[Documentation], deprecationStatus : DeprecationInfo) -> None:
        self.name : str = name
        self.mandatory : PolicyMandatedRule = isMandatory
        self.key : Optional[StoragePolicyKey] = None
        self.documentation : Optional[Documentation] = doc
        self.deprecationStatus : DeprecationInfo = deprecationStatus
        """If true then all data containers MUST comply with this policy regardless of whether a dataset specifies this policy or not"""
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, StoragePolicy) and self.name == __value.name and self.mandatory == __value.mandatory and \
            self.documentation == __value.documentation and self.key == __value.key and self.deprecationStatus == __value.deprecationStatus
    
    def setGovernanceZone(self, gz : 'GovernanceZone') -> None:
        if gz.key == None:
            raise Exception("GovernanceZone key not set")
        self.key = StoragePolicyKey(gz.key, self.name)

    def isCompatible(self, obj : 'DataContainer') -> bool:
        '''This returns true if the container is compatible with the policy. This is used to determine whether data tagged with a policy can be stored in a specific container.'''
        return False
    
    def __str__(self) -> str:
        return f"StoragePolicy({self.name})"

class StoragePolicyAllowAnyContainer(StoragePolicy):
    '''This is a storage policy that allows any container to be used.'''
    def __init__(self, name : str, isMandatory : PolicyMandatedRule, doc : Optional[Documentation] = None, deprecationStatus : DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)) -> None:
        super().__init__(name, isMandatory, doc, deprecationStatus)

    def isCompatible(self, obj : 'DataContainer') -> bool:
        return True
    
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is StoragePolicyAllowAnyContainer and \
            self.name == __value.name and self.mandatory == __value.mandatory

class LocalGovernanceManagedOnly(StoragePolicy):
    """A policy which only allows containers in the same governance zone as the policy"""
    def __init__(self, name : str, isMandatory : PolicyMandatedRule, doc : Optional[Documentation] = None, deprecationStatus : DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)) -> None:
        super().__init__(name, isMandatory, doc, deprecationStatus)

    def isCompatible(self, obj : 'DataContainer') -> bool:
        """Only allow if the container locations are managed by the required zone"""
        if(self.key == None):
            raise Exception("Policy Key not set")
        for loc in obj.locations:
            if(loc.gzName != self.key.gzName):
                return False
        return True
    
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is LocalGovernanceManagedOnly and \
            self.name == __value.name and self.mandatory == __value.mandatory

class InfraLocation:
    """This is a location within a vendors physical location hierarchy. This object
    is only fully initialized after construction when either the setParentLocation or
    setVendor methods are called. This is because the vendor is required to set the parent"""

    def __init__(self, name: str, *args: Union[Documentation, 'InfraLocation']) -> None:
        self.name: str = name
        self.key : Optional[InfraLocationKey] = None
        self.documentation : Optional[Documentation] = None

        self.locations: dict[str, 'InfraLocation'] = OrderedDict()
        """These are the 'child' locations under this location. A state location would have city children for example"""
        """This specifies the parent location of this location. State is parent on city and so on"""
        self.add(*args)

    def lint(self, tree : ValidationTree):
        """This checks if the vendor is valid for the specified ecosystem, governance zone and team"""
        if(self.key == None):
            tree.addProblem("Location key not set")
        if(self.documentation):
            dTree : ValidationTree = tree.createChild(self.documentation)
            self.documentation.lint(dTree)

        for loc in self.locations.values():
            loc.lint(tree)

    def setParentLocation(self, parent : InfraLocationKey) -> None:
        locList : list[str] = list(parent.locationPath)
        locList.append(self.name)
        self.key = InfraLocationKey(parent, locList)
        self.add()

    def add(self, *args : Union[Documentation, 'InfraLocation']) -> None:
        for loc in args:
            if(isinstance(loc, InfraLocation)):
                self.addLocation(loc)
            else:
                self.documentation = loc
        if(self.key):
            for loc in self.locations.values():
                loc.setParentLocation(self.key)

    def addLocation(self, loc : 'InfraLocation'):
        if self.locations.get(loc.name) != None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, InfraLocation):
            return self.name == __value.name and self.key == __value.key and self.locations == __value.locations and \
                self.documentation == __value.documentation
        return False
    
    def findLocationUsingKey(self, locationPath : list[str]) -> Optional['InfraLocation']:
        """Returns the location using the path"""
        if(len(locationPath) == 0):
            return None
        else:
            locName : str = locationPath[0]
            loc : Optional[InfraLocation] = self.locations.get(locName)
            if(loc):
                if(len(locationPath) == 1):
                    return loc
                else:
                    return loc.findLocationUsingKey(locationPath[1:])
            else:
                return None

class InfrastructureVendor:
    """This is a vendor which supplies infrastructure for storage and compute. It could be an internal supplier within an enterprise or an external cloud provider"""
    def __init__(self, name : str, *args : Union[InfraLocation, Documentation]) -> None:
        self.name : str = name
        self.key : Optional[InfrastructureVendorKey] = None
        self.locations : dict[str, 'InfraLocation'] = OrderedDict()
        self.documentation : Optional[Documentation] = None
        self.add(*args)

    def setGovernanceZone(self, gz : 'GovernanceZone') -> None:
        if gz.key == None:
            raise Exception("GovernanceZone key not set")
        self.key = InfrastructureVendorKey(gz.key, self.name)

        self.add()

    def add(self, *args : Union['InfraLocation', Documentation]) -> None:
        for loc in args:
            if(isinstance(loc, InfraLocation)):
                self.addLocation(loc)
            else:
                self.documentation = loc
        if(self.key):
            topLocationKey : InfraLocationKey = InfraLocationKey(self.key, [])
            for loc in self.locations.values():
                loc.setParentLocation(topLocationKey)

    def addLocation(self, loc : 'InfraLocation'):
        if self.locations.get(loc.name) != None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, InfrastructureVendor):
            return self.name == __value.name and self.key == __value.key and self.locations == __value.locations and \
                self.documentation == __value.documentation
        else:
            return False
        
    def getLocationOrThrow(self, locationName : str) -> 'InfraLocation':
        """Returns the location with the specified name or throws an exception"""
        loc : Optional[InfraLocation] = self.locations.get(locationName)
        if(loc):
            return loc
        else:
            raise Exception(f"Location {locationName} not found")
        
    def getLocation(self, locationName : str) -> Optional['InfraLocation']:
        """Returns the location with the specified name or None"""
        return self.locations.get(locationName)
    
    def findLocationUsingKey(self, locationPath : list[str]) -> Optional[InfraLocation]:
        """Returns the location using the path"""
        if(len(locationPath) == 0):
            return None
        else:
            locName : str = locationPath[0]
            loc : Optional[InfraLocation] = self.locations.get(locName)
            if(loc):
                if(len(locationPath) == 1):
                    return loc
                else:
                    return loc.findLocationUsingKey(locationPath[1:])
            else:
                return None

    def lint(self, tree : ValidationTree):
        """This checks if the vendor is valid for the specified ecosystem, governance zone and team"""
        if(self.key == None):
            tree.addProblem("Vendor key not set")
        if(self.documentation == None):
            tree.addProblem("Vendor documentation not set")
        else:
            self.documentation.lint(tree)

        for loc in self.locations.values():
            lTree : ValidationTree = tree.createChild(loc)
            loc.lint(lTree)

class EncryptionSystem:
    """This describes"""
    def __init__(self) -> None:
        self.name : Optional[str] = None
        self.keyContainer : Optional['DataContainer'] = None
        """Are keys stored on site or at a third party?"""
        self.hasThirdPartySuperUser : bool = False

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

class DataContainer:
    """This is a container for data. It's a logical container. The data can be physically stored in
    one or more locations through replication or fault tolerance measures. It is owned by a data platform
      and is used to determine whether a dataset is compatible with the container by a governancezone."""   
    def __init__(self, *args : 'InfraLocationKey') -> None:
        self.locations : set[InfraLocationKey] = set()
        self.name : Optional[str] = None
        self.serverSideEncryptionKeys : Optional[EncryptionSystem] = None
        """This is the vendor ecnryption system providing the container. For example, if a cloud vendor
        hosts the container, do they have access to the container data?"""
        self.clientSideEncryptionKeys : Optional[EncryptionSystem] = None
        """This is the encryption system used by the client to encrypt data before sending to the container. This could be used
        to encrypt data before sending to a cloud vendor for example"""
        self.isReadOnly : bool =  False
        self.add(*args)

    def add(self, *args : 'InfraLocationKey') -> None:
        for loc in args:
            if(loc in self.locations):
                raise Exception(f"Duplicate Location {loc}")
            self.locations.add(loc)

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
        if(self.name):
            return self.name
        else:
            raise Exception("Container name not set")

class Dataset(ANSI_SQL_NamedObject):
    """This is a single collection of homogeneous records with a primary key"""
    def __init__(self, name : str, *args : Union[Schema, StoragePolicy, Documentation, DeprecationInfo, DataClassification]) -> None:
        super().__init__(name)
        self.originalSchema : Optional[Schema] = None
        # Explicit policies, note these need to be added to mandatory policies for the owning GZ
        self.policies : dict[str, StoragePolicy] = OrderedDict()
        self.classification : Optional[DataClassification] = None
        """This is the classification of the data in the dataset. The overrides any classifications on the schema"""
        self.documentation : Optional[Documentation] = None
        self.deprecationStatus : DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)
        self.add(*args)

    def add(self, *args : Union[Schema, StoragePolicy, Documentation, DeprecationInfo, DataClassification]) -> None:
        for arg in args:
            if(isinstance(arg,Schema)):
                s : Schema = arg
                self.originalSchema = s
            elif(isinstance(arg, StoragePolicy)):
                p : StoragePolicy = arg
                self.addPolicy(p)
            elif(isinstance(arg, DeprecationInfo)):
                self.deprecationStatus = arg
            elif(isinstance(arg, DataClassification)):
                self.classification = arg
            else:
                d : Documentation = arg
                self.documentation = d
    
    def addPolicy(self, s : StoragePolicy) -> None:
        if self.policies.get(s.name) != None:
            raise Exception(f"Duplicate policy {s.name}")
        self.policies[s.name] = s

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, Dataset):
            return super().__eq__(__value) and self.name == __value.name and self.originalSchema == __value.originalSchema and \
                self.policies == __value.policies and self.documentation == __value.documentation and \
                self.deprecationStatus == __value.deprecationStatus and self.classification == __value.classification
        return False

    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', store : 'Datastore', tree : ValidationTree) -> None:
        """Place holder to validate constraints on the dataset"""
        self.nameLint(tree)
        for policy in self.policies.values():
            if(policy.key == None):
                tree.addProblem(f"Storage policy {policy.name} is not associated with a governance zone")
            else:
                if(policy.key.gzName != gz.name):
                    tree.addProblem(f"Datasets must be governed by storage policies from its managing zone")
                if(policy.deprecationStatus.status == DeprecationStatus.DEPRECATED):
                    if(store.isDatasetDeprecated(self)):
                        tree.addProblem(f"Storage policy {policy.name} is deprecated", ProblemSeverity.WARNING)
                    else:
                        tree.addProblem(f"Storage policy {policy.name} is deprecated", ProblemSeverity.ERROR)
        if(self.originalSchema):
            self.originalSchema.lint(tree)
        else:
            tree.addProblem("Original schema not set")

    def checkClassificationsAreOnly(self, verifier : DataClassificationPolicy) -> bool:
        """This checks if the dataset only has the specified classifications"""

        # Dataset level classification overrides schema level classification
        if(self.classification):
            return verifier.isCompatible(self.classification)
        else:
            if self.originalSchema:
                # check schema attribute classifications are good
                return self.originalSchema.checkClassificationsAreOnly(verifier)
            else:
                return True

    def isBackwardsCompatibleWith(self, other : object, vTree : ValidationTree) -> bool:
        """This checks if the dataset is backwards compatible with the other dataset. This means that the other dataset
        can be used in place of this dataset. This is used to check if a dataset can be replaced by another dataset
        when a new version is released"""
        if(not isinstance(other, Dataset)):
            vTree.addProblem(f"Object {other} is not a Dataset")
            return False
        super().isBackwardsCompatibleWith(other, vTree)
        if(self.originalSchema == None):
            vTree.addProblem(f"Original schema not set for {self.name}")
        elif(other.originalSchema == None):
            vTree.addProblem(f"Original schema not set for {other.name}")
        else:
            self.originalSchema.isBackwardsCompatibleWith(other.originalSchema, vTree)
        return not vTree.hasErrors()
    
    def __str__(self) -> str:
        return f"Dataset({self.name})"
    
class DataSourceConnection:
    def __init__(self, name : str) -> None:
        self.name : str = name

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is DataSourceConnection):
            return self.name == __value.name
        else:
            return False

class Credential(ABC):
    """These allow a client to connect to a service/server"""
    def __init__(self) -> None:
        pass

    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, Credential)):
            return True
        else:
            return False
    
    @abstractmethod
    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', tree : ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        raise NotImplementedError()


class FileSecretCredential(Credential):
    """This allows a secret to be read from the local filesystem. Usually the secret is
    placed in the file using an external service such as Docker secrets etc. The secret should be in the
    form of 2 lines, first line is user name, second line is password"""
    def __init__(self, filePath : str) -> None:
        super().__init__()
        self.secretFilePath : str = filePath

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is FileSecretCredential and self.secretFilePath == __value.secretFilePath
    
    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', tree : ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        # TODO This needs to be better
        if(self.secretFilePath == ""):
            tree.addProblem("Secret file path is empty")

    def __str__(self) -> str:
        return f"FileSecretCredential({self.secretFilePath})"

class ClearTextCredential(Credential):
    """This is implemented for testing but should never be used in production. All
    credentials should be stored and retrieved using secrets Credential objects also
    provided."""
    def __init__(self, username : str, password : str) -> None:
        super().__init__()
        self.username : str = username
        self.password : str = password

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is ClearTextCredential and self.username == __value.username and self.password == __value.password
    
    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', tree : ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if(self.username == ""):
            tree.addProblem("Username is empty")
        if(self.password == ""):
            tree.addProblem("Password is empty")

    def __str__(self) -> str:
        return f"ClearTextCredential({self.username})"

class LocalJDBCConnection(DataSourceConnection):
    def __init__(self, name: str, jdbcUrl : str, cred : Credential) -> None:
        super().__init__(name)
        self.jdbcUrl : str = jdbcUrl
        self.credential : Credential = cred

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is LocalJDBCConnection and self.jdbcUrl == __value.jdbcUrl and self.credential == __value.credential

class CaptureSourceInfo(ABC):
    """Describes how an IMD can connect to the database or similar to ingest data"""
    def __init__(self) -> None:
        pass

    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, CaptureSourceInfo)):
            return True
        else:
            return False
        
    @abstractmethod
    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', tree : ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        raise NotImplementedError()

class PyOdbcSourceInfo(CaptureSourceInfo):
    """This describes how to connect to a database using pyodbc"""
    def __init__(self, serverHost : str, databaseName : str, driver : str, connectionStringTemplate : str) -> None:
        super().__init__()
        self.serverHost : str = serverHost
        self.databaseName : str = databaseName
        self.driver : str = driver
        self.connectionStringTemplate : str = connectionStringTemplate

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is PyOdbcSourceInfo and self.serverHost == __value.serverHost and self.databaseName == __value.databaseName and self.driver == __value.driver and self.connectionStringTemplate == __value.connectionStringTemplate

    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', tree : ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        if(not is_valid_hostname_or_ip(self.serverHost)):
            tree.addProblem(f"Server host {self.serverHost} is not a valid hostname or IP address")

    def __str__(self) -> str:
        return f"PyOdbcSourceInfo({self.serverHost})"
        
class CaptureType(Enum):
    SNAPSHOT = 0
    INCREMENTAL = 1

class IngestionConsistencyType(Enum):
    """This determines whether data is ingested in consistent groups across multiple datasets or
    whether each dataset is ingested independently"""
    SINGLE = 0
    MULTI = 1

class CaptureMetaData(ABC):
    """Producers use these to describe HOW to snapshot and pull deltas from a data source in to
    data pipelines. The ingestion service interprets these to allow code free ingestion from
    supported sources and handle operation pipelines."""
    def __init__(self, *args : Union[Credential, CaptureSourceInfo, IngestionConsistencyType]) -> None:
        self.credential : Optional[Credential] = None
        self.SingleOrMultiDatasetIngestion : Optional[IngestionConsistencyType] = None
        self.captureSource : list[CaptureSourceInfo] = []
        self.add(*args)

    def add(self, *args : Union[Credential, CaptureSourceInfo, IngestionConsistencyType]) -> None:
        for arg in args:
            if(isinstance(arg, Credential)):
                c : Credential = arg
                if(self.credential != None):
                    raise AttributeAlreadySetException("Credential already set")
                self.credential = c
            elif(type(arg) == IngestionConsistencyType):
                if(self.SingleOrMultiDatasetIngestion != None):
                    raise AttributeAlreadySetException("SingleOrMultiDatasetIngestion already set")
                sm : IngestionConsistencyType = arg
                self.SingleOrMultiDatasetIngestion = sm
            elif(isinstance(arg, CaptureSourceInfo)):
                i : CaptureSourceInfo = arg
                self.captureSource.append(i)
            
    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, CaptureMetaData):
            return self.credential == __value.credential and self.SingleOrMultiDatasetIngestion == __value.SingleOrMultiDatasetIngestion and \
                self.captureSource == __value.captureSource
        return False
    
    @abstractmethod
    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', d : 'Datastore', tree : ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        for cap in self.captureSource:
            capTree : ValidationTree = tree.createChild(cap)
            cap.lint(eco, gz, t, capTree)

class CDCCaptureIngestion(CaptureMetaData):
    """This indicates CDC can be used to capture deltas from the source"""
    def __init__(self, *args : Union[Credential, CaptureSourceInfo, IngestionConsistencyType]) -> None:
        super().__init__(*args)

    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', d : 'Datastore', tree : ValidationTree) -> None:
        pass

    def __str__(self) -> str:
        return f"CDCCaptureIngestion()"
    
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is CDCCaptureIngestion
    
        
class SQLPullIngestion(CaptureMetaData):
    """This IMD describes how to pull a snapshot 'dump' from each dataset and then persist
    state variables which are used to next pull a delta per dataset and then persist the state
    again so that another delta can be pulled on the next pass and so on"""
    def __init__(self, *args : Union[Credential, CaptureSourceInfo]) -> None:
        super().__init__(*args)
        self.variableNames : list[str] = []
        """The names of state variables produced by snapshot and delta sql strings"""
        self.snapshotSQL : dict[str, str] = OrderedDict()
        """A SQL string per dataset which pulls a per table snapshot"""
        self.deltaSQL : dict[str, str] = OrderedDict()
        """A SQL string per dataset which pulls all rows which changed since last time for a table"""

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, SQLPullIngestion):
            return super().__eq__(__value) and self.variableNames == __value.variableNames and \
                self.snapshotSQL == __value.snapshotSQL and self.deltaSQL == __value.deltaSQL
        return False
    
    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', d : 'Datastore', tree : ValidationTree) -> None:
        raise NotImplementedError()
    
    def __str__(self) -> str:
        return f"SQLPullIngestion()"
    


class Datastore(ANSI_SQL_NamedObject):

    """This is a named group of datasets. It describes how to capture the data and make it available for processing"""
    def __init__(self, name : str, *args : Union[Dataset, CaptureMetaData, DataContainer, Documentation, ProductionStatus, DeprecationInfo]) -> None:
        super().__init__(name)
        self.datasets : dict[str, Dataset] = OrderedDict()
        self.imd : Optional[CaptureMetaData] = None
        self.container : Optional[DataContainer] = None
        self.documentation : Optional[Documentation] = None
        self.productionStatus : ProductionStatus = ProductionStatus.NOT_PRODUCTION
        self.deprecationStatus : DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)
        """Deprecating a store deprecates all datasets in the store regardless of their deprecation status"""
        self.add(*args)

    def add(self, *args : Union[Dataset, CaptureMetaData, DataContainer, Documentation, ProductionStatus, DeprecationInfo]) -> None:
        for arg in args:
            if(type(arg) is Dataset):
                d : Dataset = arg
                self.addDataset(d)
            elif(isinstance(arg, CaptureMetaData)):
                i : CaptureMetaData = arg
                self.imd = i
            elif(isinstance(arg, DataContainer)):
                c : DataContainer = arg
                self.container = c
            elif(isinstance(arg, ProductionStatus)):
                self.productionStatus = arg
            elif(isinstance(arg, DeprecationInfo)):
                self.deprecationStatus = arg
            elif(isinstance(arg, Documentation)):
                doc : Documentation = arg
                self.documentation = doc

    def addDataset(self, item : Dataset) -> None:
        """Add a named dataset"""
        if self.datasets.get(item.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Dataset {item.name}")
        self.datasets[item.name] = item

    def isDatasetDeprecated(self, dataset : Dataset) -> bool:
        """Returns true if the datastore is deprecated OR dataset is deprecated"""
        return self.deprecationStatus.status == DeprecationStatus.DEPRECATED or dataset.deprecationStatus.status == DeprecationStatus.DEPRECATED
    
    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, Datastore):
            return super().__eq__(__value) and self.datasets == __value.datasets and self.imd == __value.imd and \
                self.container == __value.container and self.documentation == __value.documentation and \
                self.productionStatus == __value.productionStatus and self.deprecationStatus == __value.deprecationStatus
        return False
    
    def lint(self, eco : 'Ecosystem', gz : 'GovernanceZone', t : 'Team', storeTree : ValidationTree) -> None:
        self.nameLint(storeTree)
        for dataset in self.datasets.values():
            dTree : ValidationTree = storeTree.createChild(dataset)
            dataset.lint(eco, gz, t, self, dTree)

        if(self.imd):
            imdTree : ValidationTree = storeTree.createChild(self.imd)
            self.imd.lint(eco, gz, t, self, imdTree)
        else:
            storeTree.addProblem("IMD not set")
    
    def isBackwardsCompatibleWith(self, other : object, vTree : ValidationTree) -> bool:
        """This checks if the other datastore is backwards compatible with this one. This means that the other datastore
        can be used to replace this one without breaking any data pipelines"""

        if(not isinstance(other, Datastore)):
            vTree.addProblem(f"Object {other} is not a Datastore")
            return False
        super().isBackwardsCompatibleWith(other, vTree)
        # Check if the datasets are compatible
        for dataset in self.datasets.values():
            dTree : ValidationTree = vTree.createChild(dataset)
            otherDataset : Optional[Dataset] = other.datasets.get(dataset.name)
            if(otherDataset):
                dataset.isBackwardsCompatibleWith(otherDataset, dTree)
            else:
                dTree.addProblem(f"Dataset {dataset.name} is missing from datastore {other.name}")
        return not vTree.hasErrors()
    
    def __str__(self) -> str:
        return f"Datastore({self.name})"
        
class Repository(ABC):
    """This is a repository which can store an ecosystem model. It is used to check whether changes are authorized when made from a repository"""
    def __init__(self, doc : Optional[Documentation]):
        self.documentation : Optional[Documentation] = doc

    @abstractmethod
    def lint(self, tree : ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        raise NotImplementedError()
    
    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, Repository)):
            return self.documentation == __value.documentation
        else:
            return False

class TestRepository(Repository):
    def __init__(self, name : str, doc : Optional[Documentation] = None) -> None:
        super().__init__(doc)
        self.name = name

    def lint(self, tree : ValidationTree) -> None:
        pass

    def __str__(self) -> str:
        return f"TestRepository({self.name})"
    
    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, TestRepository)):
            return super().__eq__(__value) and self.name == __value.name
        else:
            return False

class GitHubRepository(Repository):
    """This represents a GitHub Repository specifically, this branch should have an eco.py files in the root
    folder. The eco.py file should contain an ecosystem object which is used to construct the ecosystem"""
    def __init__(self, repo : str, branchName : str, doc : Optional[Documentation] = None) -> None:
        super().__init__(doc)
        self.repoURL : str = repo
        """The name of the git repository from which changes to Team objects are authorized"""
        self.branchName : str = branchName
        """The name of the branch containing an eco.py to construct an ecosystem"""

    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, GitHubRepository)):
            return super().__eq__(__value) and self.repoURL == __value.repoURL and self.branchName == __value.branchName
        else:
            return False
        
    def __str__(self) -> str:
        return f"GitRepository({self.repoURL})"
    
    def is_valid_github_url(self, url: str) -> bool:
        """This validates a github url"""
        https_pattern = r'https://github\.com/[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+/?'
        ssh_pattern = r'git@github\.com:[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+\.git'
        
        return re.match(https_pattern, url) is not None or re.match(ssh_pattern, url) is not None

    def is_valid_github_branch(self, branch: str) -> bool:
        # Branch names cannot contain the sequence ..
        if '..' in branch:
            return False

        # Branch names cannot have a . at the end
        if branch.endswith('.'):
            return False

        # Branch names cannot have multiple consecutive . characters
        if '..' in branch:
            return False

        # Branch names cannot contain any of the following characters: ~ ^ : \ * ? [ ] /
        if any(char in branch for char in ['~', '^', ':', '\\', '*', '?', '[', ']', '/']):
            return False

        # Branch names cannot start with -
        if branch.startswith('-'):
            return False

        # Branch names can only contain alphanumeric characters, ., -, and _
        pattern = r'^[a-zA-Z0-9_.-]+$'
        return re.match(pattern, branch) is not None
    
    def lint(self, tree : ValidationTree):
        """This checks if repository is valid syntaxically"""
        if(self.is_valid_github_url(self.repoURL) == False):
            tree.addProblem("Repository URL is not valid")
        if(self.is_valid_github_branch(self.branchName) == False):
            tree.addProblem("Branch name is not valid")

class TeamCacheEntry:
    """This is used by Ecosystem to cache teams"""
    def __init__(self, t : 'Team', td : 'TeamDeclaration') -> None:
        self.team : Team = t
        self.declaration : TeamDeclaration = td

class WorkspaceCacheEntry:
    """This is used by Ecosystem to cache workspaces"""
    def __init__(self, w : 'Workspace', t : 'Team') -> None:
        self.workspace : Workspace = w
        self.team : Team = t

class DatastoreCacheEntry:
    """This is used by Ecosystem to cache datastores"""
    def __init__(self, d : 'Datastore', t : 'Team') -> None:
        self.datastore : Datastore = d
        self.team : Team = t

class DependentWorkspaces:
    """This tracks a Workspaces dependent on a datastore"""
    def __init__(self, workSpace : 'Workspace'):
        self.workspace : Workspace = workSpace
        self.dependencies : set[DependentWorkspaces] = set()

    def addDependency(self, dep : 'DependentWorkspaces') -> None:
        self.dependencies.add(dep)

    def flatten(self) -> set['Workspace']:
        """Returns a flattened list of dependencies"""
        rc : set[Workspace] = {self.workspace}
        for dep in self.dependencies:
            rc.update(dep.flatten())
        return rc
    
    def __str__(self) -> str:
        return f"Dependency({self.flatten()})"
    
    def __hash__(self) -> int:
        return hash(self.workspace.name)
    
    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, DependentWorkspaces)):
            return self.workspace.name == __value.workspace.name
        else:
            return False


# Add regulators here with their named retention policies for reference in Workspaces
# Feels like regulators are across GovernanceZones
class Ecosystem(GitControlledObject):

    def createGZone(self, name : str, repo : Repository) -> 'GovernanceZone':
        gz : GovernanceZone = GovernanceZone(name, repo)
        gz.setEcosystem(self)
        return gz
    
    def __init__(self, name : str, repo : Repository, *args : 'GovernanceZoneDeclaration') -> None:
        super().__init__(repo)
        self.name : str = name
        self.key : EcosystemKey = EcosystemKey(self.name)

        self.zones : AuthorizedObjectManager[GovernanceZone, GovernanceZoneDeclaration] = AuthorizedObjectManager[GovernanceZone, GovernanceZoneDeclaration](lambda name, repo : self.createGZone(name, repo), repo)
        """This is the authorative list of governance zones within the ecosystem"""

        self.resetCaches()
        self.add(*args)

    def resetCaches(self) -> None:
        """Empties the caches"""
        self.datastoreCache : dict[str, DatastoreCacheEntry] = {}
        """This is a cache of all data stores in the ecosystem"""
        self.workSpaceCache : dict[str, WorkspaceCacheEntry] = {}
        """This is a cache of all workspaces in the ecosystem"""
        self.teamCache : dict[str, TeamCacheEntry] = {}
        """This is a cache of all team declarations in the ecosystem"""

    def add(self, *args : 'GovernanceZoneDeclaration') -> None:
        for arg in args:
            self.addZoneDef(arg)


    def addZoneDef(self, z : 'GovernanceZoneDeclaration') -> None:
        """Adds a zone def to the ecosystem and sets the zone to this ecosystem"""
        self.zones.addAuthorization(z)
        z.key = GovernanceZoneKey(self.key, z.name)

    def cache_addTeam(self, td : 'TeamDeclaration', t : 'Team'):
        if(self.teamCache.get(td.name) != None):
            if(self.teamCache.get(td.name) != None):
                raise ObjectAlreadyExistsException(f"Duplicate Team {td.name}")
            self.teamCache[td.name] = TeamCacheEntry(t, td)

    def cache_addWorkspace(self, team : 'Team', work : 'Workspace'):
        """This adds a workspace to the eco cache and flags duplicates"""
        if(self.workSpaceCache.get(work.name) != None):
            raise ObjectAlreadyExistsException(f"Duplicate workspace {work.name}")
        self.workSpaceCache[work.name] = WorkspaceCacheEntry(work, team)

    def cache_addDatastore(self, store : 'Datastore', t : 'Team'):
        """This adds a store to the eco cache and flags duplicates"""
        if(self.datastoreCache.get(store.name) != None):
            raise ObjectAlreadyExistsException(f"Duplicate data store {store.name}")
        self.datastoreCache[store.name] = DatastoreCacheEntry(store, t)

    def cache_getWorkspaceOrThrow(self, work : str) -> WorkspaceCacheEntry:
        """This returns the named workspace if it exists"""
        w : Optional[WorkspaceCacheEntry] = self.workSpaceCache.get(work)
        if(w):
            return w
        else:
            raise WorkspaceDoesntExistException(f"Unknown workspace {work}")
        
    def cache_getDatastoreOrThrow(self, store : str) -> DatastoreCacheEntry:
        s : Optional[DatastoreCacheEntry] = self.datastoreCache.get(store)
        if(s):
            return s
        else:
            raise DatastoreDoesntExistException(f"Unknown datastore {store}")

    def cache_getDataset(self, storeName : str, datasetName : str) -> Optional[Dataset]:
        """This returns the named dataset if it exists"""
        s : Optional[DatastoreCacheEntry] = self.datastoreCache.get(storeName)
        if(s):
            dataset = s.datastore.datasets.get(datasetName)
            return dataset
        return None

    def lintAndHydrateCaches(self) -> ValidationTree:
        """This validates the ecosystem and returns a list of problems which is empty if there are no issues"""
        self.resetCaches()

        # This will lint the ecosystem, zones, teams, datastores and datasets.
        ecoTree : ValidationTree = ValidationTree(self)
        
        # This will lint the ecosystem, zones, teams, datastores and datasets.
        # Workspaces are linted in a second pass later.
        # It populates the caches for zones, teams, stores and workspaces.
        """No need to dedup zones as the authorative list is already a dict"""
        for gz in self.zones.authorizedObjects.values():
            govTree : ValidationTree = ecoTree.createChild(gz)
            gz.lint(self, govTree)

        """All caches should now be populated"""

        # Now lint the workspaces
        for workInfo in self.workSpaceCache.values():
            work = workInfo.workspace
            workTree : ValidationTree = ecoTree.createChild(work)
            work.lint(self, workTree)

        self.superLint(ecoTree)
        self.zones.lint(ecoTree)
        return ecoTree

    def calculateDependenciesForDatastore(self, storeName : str, wsVisitedSet : set[str] = set()) -> Sequence[DependentWorkspaces]:
        rc : list[DependentWorkspaces] = []
        store : Datastore = self.datastoreCache[storeName].datastore

        # If the store is used in any Workspace then thats a dependency
        for w in self.workSpaceCache.values():
            # Do not enter a cyclic loop
            if(not w.workspace.name in wsVisitedSet):
                if(w.workspace.isDatastoreUsed(store)):
                    workspace : Workspace = w.workspace
                    dep : DependentWorkspaces = DependentWorkspaces(workspace)
                    rc.append(dep)
                    # prevent cyclic loops
                    wsVisitedSet.add(workspace.name)
                    # If the workspace has a data transformer then the output store's dependencies are also dependencies
                    if(workspace.dataTransformer != None):
                        outputStore : Datastore = self.datastoreCache[workspace.dataTransformer.outputStoreName].datastore
                        depList : Sequence[DependentWorkspaces] = self.calculateDependenciesForDatastore(outputStore.name, wsVisitedSet)
                        for dep in depList:
                            dep.addDependency(dep)
        return rc

    def checkIfChangesAreAuthorized(self, proposed : GitControlledObject, changeSource : Repository, vTree : ValidationTree) -> None:
        """This checks if the ecosystem top level has changed relative to the specified change source"""
        """This checks if any Governance zones has been added or removed relative to e"""

        prop_eco : Ecosystem = cast(Ecosystem, proposed)

        self.checkTopLevelAttributeChangesAreAuthorized(prop_eco, changeSource, vTree)

        self.zones.checkIfChangesAreAuthorized(prop_eco.zones, changeSource, vTree)
    
    def __eq__(self, proposed: object) -> bool:
        if super().__eq__(proposed) and isinstance(proposed, Ecosystem):
            return self.name == proposed.name and self.zones == proposed.zones and self.key == proposed.key
        else:
            return False
    
    def eq_toplevel(self, proposed: GitControlledObject) -> bool:
        """This is a shallow equality check for the top level ecosystem object"""
        if(isinstance(proposed, Ecosystem)):
            return self.name == proposed.name and self.owningRepo == proposed.owningRepo and self.zones.eq_toplevel(proposed.zones)
        else:
            return False
        
    def getZone(self, gz : str) -> Optional['GovernanceZone']:
        """Returns the governance zone with the specified name"""
        zone : Optional[GovernanceZone] = self.zones.getObject(gz)
        return zone
    
    def getZoneOrThrow(self, gz : str) -> 'GovernanceZone':
        z : Optional[GovernanceZone] = self.getZone(gz)
        if(z):
            return z
        else:
            raise ObjectDoesntExistException(f"Unknown governance zone {gz}")            
    
    def getTeam(self, gz : str, teamName : str) -> Optional['Team']:
        """Returns the team with the specified name in the specified zone"""
        zone : Optional[GovernanceZone] = self.getZone(gz)
        if(zone):
            t : Optional[Team] = zone.getTeam(teamName)
            return t
        else:
            return None
        
    def getTeamOrThrow(self, gz : str, teamName : str) -> 'Team':
        t : Optional[Team] = self.getTeam(gz, teamName)
        if(t):
            return t
        else:
            raise ObjectDoesntExistException(f"Unknown team {teamName} in governance zone {gz}")
        
    def __str__(self) -> str:
        return f"Ecosystem({self.name})"

    def checkIfChangesAreBackwardsCompatibleWith(self, originEco : 'Ecosystem', vTree : ValidationTree) -> None:
        """This checks if the proposed ecosystem is backwards compatible with the current ecosystem"""
        # Check if the zones are compatible
        for zone in self.zones.authorizedObjects.values():
            zTree : ValidationTree = vTree.createChild(zone)
            originZone : Optional[GovernanceZone] = originEco.getZone(zone.name)
            if originZone:
                zone.isBackwardsCompatibleWith(originZone, zTree)

    def checkIfChangesCanBeMerged(self, proposed : 'Ecosystem', source : Repository) -> ValidationTree:
        """This is called to check if the proposed changes can be merged in to the current ecosystem. It returns a ValidationTree with issues if not
        or an empty ValidationTree if allowed."""

        # First, the incoming ecosystem must be consistent and pass lint checks
        eTree : ValidationTree = proposed.lintAndHydrateCaches()

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
    def __init__(self, name : str, repo : Repository, *args : Union[Datastore, 'Workspace', Documentation]) -> None:
        super().__init__(repo)
        self.name : str = name
        self.workspaces : dict[str, Workspace] = OrderedDict()
        self.dataStores : dict[str, Datastore] = OrderedDict()
        self.documentation : Optional[Documentation] = None
        self.add(*args)

    def add(self, *args : Union[Datastore, 'Workspace', Documentation]) -> None:
        """Adds a workspace, datastore or gitrepository to the team"""
        for arg in args:
            if(isinstance(arg, Datastore)):
                s : Datastore = arg
                self.addStore(s)
            elif(isinstance(arg, Workspace)):
                w : Workspace = arg
                self.addWorkspace(w)
            else:
                d : Documentation = arg
                self.documentation = d

    def addStore(self, store : Datastore):
        """Adds a datastore to the team checking for duplicates"""
        if self.dataStores.get(store.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Datastore {store.name}")
        self.dataStores[store.name] = store

    def addWorkspace(self, w : 'Workspace'):
        if self.workspaces.get(w.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Workspace {w.name}")
        self.workspaces[w.name] = w

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, Team):
            return self.name == __value.name and self.workspaces == __value.workspaces and \
                self.dataStores == __value.dataStores and self.documentation == __value.documentation
        return False
    
    def eq_toplevel(self, proposed: GitControlledObject) -> bool:
        return super().eq_toplevel(proposed) and type(proposed) is Team and self.dataStores == proposed.dataStores and self.workspaces == proposed.workspaces
    
    def checkIfChangesAreAuthorized(self, proposed : GitControlledObject, changeSource : Repository, vTree : ValidationTree) -> None:
        """This checks if the team has changed relative to the specified change source"""
        prop_Team : Team = cast(Team, proposed)

        self.checkTopLevelAttributeChangesAreAuthorized(prop_Team, changeSource, vTree)
    
    def lint(self, eco : Ecosystem, gz: 'GovernanceZone', teamTree : ValidationTree) -> None:
        """This validates a single team declaration and populates the datastore cache with that team's stores"""
        for s in self.dataStores.values():
            if eco.datastoreCache.get(s.name) is not None:
                teamTree.addProblem(f"Duplicate Datastore {s.name}")
            else:
                storeTree : ValidationTree = teamTree.createChild(s)
                eco.cache_addDatastore(s, self)
                s.lint(eco, gz, self, storeTree)

        # Iterate over the workspaces to populate the cache but dont lint them yet
        for w in self.workspaces.values():
            if eco.workSpaceCache.get(w.name) is not None:
                teamTree.addProblem(f"Duplicate Workspace {w.name}")
                # Cannot validate Workspace datasets until everything is loaded
            else:
                eco.cache_addWorkspace(self, w)

                # Check all classification allows policies from gz are satisfied on every sink
                for dcc in gz.classificationPolicies:
                    for dsg in w.dsgs.values():
                        for sink in dsg.sinks.values():
                            store : Datastore = eco.cache_getDatastoreOrThrow(sink.storeName).datastore
                            dataset : Dataset = store.datasets[sink.datasetName]
                            if(dataset.checkClassificationsAreOnly(dcc) == False):
                                teamTree.addProblem(f"Sink {sink} has classification which is not allowed by policy {dcc}")
        self.superLint(teamTree)

    def __str__(self) -> str:
        return f"Team({self.name})"
    
    def isBackwardsCompatibleWith(self, originTeam : 'Team', vTree : ValidationTree):
        """This checks if the current team is backwards compatible with the origin team"""
        # Check if the datasets are compatible
        for store in self.dataStores.values():
            sTree : ValidationTree = vTree.createChild(store)
            originStore : Optional[Datastore] = originTeam.dataStores.get(store.name)
            if(originStore):
                store.isBackwardsCompatibleWith(originStore, sTree)
        
class NamedObjectAuthorization:
    """This represents a named object under the management of a repository. It is used to authorize the existence
    of the object before the specified repository can be used to edit/specify it."""
    def __init__(self, name : str, owningRepo : Repository) -> None:
        self.name : str = name
        self.owningRepo : Repository = owningRepo

    def lint(self, tree : ValidationTree):
        self.owningRepo.lint(tree)

    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, NamedObjectAuthorization)):
            return self.name == __value.name and self.owningRepo == __value.owningRepo
        else:
            return False


G = TypeVar('G', bound=GitControlledObject)
N = TypeVar('N', bound=NamedObjectAuthorization)

class AuthorizedObjectManager(Generic[G, N], GitControlledObject):
    """This tracks a list of named authorizations and the named objects themselves in seperate lists. It is used
    to allow one repository to managed the authorization to create named objects using a second object specific repository or branch. 
    Each named object can then be managed by a seperate repository. """
    def __init__(self, factory : Callable[[str, Repository], G], owningRepo : Repository) -> None:
        super().__init__(owningRepo)
        self.authorizedNames : dict[str, N] = OrderedDict[str, N]()
        self.authorizedObjects : dict[str, G] = OrderedDict[str, G]()
        self.factory : Callable[[str, Repository], G] = factory

    def getNumObjects(self) -> int:
        """Returns the number of objects"""
        return len(self.authorizedObjects)
    
    def addAuthorization(self, t : N):
        """This is used to add a named authorization along with its owning repository to the list of authorizations."""
        if self.authorizedNames.get(t.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate authorization {t.name}")
        self.authorizedNames[t.name] = t

    def getObject(self, name : str) -> Optional[G]:
        """This returns a managed object for the specified name. Users can then fill out the attributes 
        of the returned object."""
        noa : Optional[N] = self.authorizedNames.get(name)
        if(noa == None):
            return None
        t : Optional[G] = self.authorizedObjects.get(name)
        if(t == None):
            t = self.factory(name, noa.owningRepo) # Create an instance of the object
            self.authorizedObjects[name] = t
        return t
    
    def __eq__(self, __value: object) -> bool:
        if(super().__eq__(__value) and isinstance(__value, AuthorizedObjectManager)):
            a : AuthorizedObjectManager[G, N] = cast(AuthorizedObjectManager[G, N], __value)
            return self.authorizedNames == a.authorizedNames and self.authorizedObjects == a.authorizedObjects
        else:
            return False

    def eq_toplevel(self, proposed: GitControlledObject) -> bool:
        p : AuthorizedObjectManager[G, N] = cast(AuthorizedObjectManager[G, N], proposed)
        return self.authorizedNames == p.authorizedNames

    def checkIfChangesAreAuthorized(self, proposed : GitControlledObject, changeSource : Repository, vTree : ValidationTree) -> None:
        proposedGZ : AuthorizedObjectManager[G, N] = cast(AuthorizedObjectManager[G, N], proposed)
        
        """This checks if the governance zone has changed relative to the specified change source"""
        """This checks if any teams have been added or removed relative to e"""

        self.checkTopLevelAttributeChangesAreAuthorized(proposedGZ, changeSource, vTree)

        # Get the current teams from the change source
        self.checkDictChangesAreAuthorized(self.authorizedObjects, proposedGZ.authorizedObjects, changeSource, vTree)
    
    def removeAuthorization(self, name : str) -> Optional[N]:
        """Removes the authorization from the list of authorized names"""
        r : Optional[N] = self.authorizedNames.pop(name)
        if(r and self.authorizedObjects.get(name) != None):
            self.removeDefinition(name)
        return r
    
    def removeDefinition(self, name : str) -> Optional[G]:
        """Removes the object definition . This must be done by the object repo before the parent repo can remove the authorization"""
        r : Optional[G] = self.authorizedObjects.pop(name)
        return r
    
    def lint(self, tree : ValidationTree):
        self.superLint(tree)


class TeamDeclaration(NamedObjectAuthorization):
    """This is a declaration of a team within a governance zone. It is used to authorize
    the team and to provide the official source of changes for that object and its children"""
    def __init__(self, name : str, authRepo : Repository) -> None:
        super().__init__(name, authRepo)
        self.authRepo : Repository = authRepo
        self.key : Optional[TeamDeclarationKey] = None

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, TeamDeclaration) and self.authRepo == __value.authRepo and self.key == __value.key
    
    def setGovernanceZone(self, gz : 'GovernanceZone') -> None:
        """Sets the governance zone for this team and sets the team for all datastores and workspaces"""
        if gz.key:
            self.key = TeamDeclarationKey(gz.key, self.name)

class GovernanceZoneDeclaration(NamedObjectAuthorization):
    """This is a declaration of a governance zone within an ecosystem. It is used to authorize
    the definition of a governance zone and to provide the official source of changes for that object and its children"""
    def __init__(self, name : str, authRepo : Repository) -> None:
        super().__init__(name, authRepo)
        self.key : Optional[GovernanceZoneKey] = None

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and isinstance(__value, GovernanceZoneDeclaration) and self.key == __value.key
    

class GovernanceZone(GitControlledObject):
    """This declares the existence of a specific GovernanceZone and defines the teams it manages, the storage policies
    and which repos can be used to pull changes for various metadata"""
    def __init__(self, name : str, ownerRepo : Repository, *args : Union[InfrastructureVendor, StoragePolicy, DataClassificationPolicy, TeamDeclaration, Documentation, 'DataPlatform']) -> None:
        super().__init__(ownerRepo)
        self.name : str = name
        self.key : Optional[GovernanceZoneKey] = None
        self.platforms : dict[str, 'DataPlatform'] = OrderedDict[str, 'DataPlatform']()
        self.teams : AuthorizedObjectManager[Team, TeamDeclaration] = AuthorizedObjectManager[Team, TeamDeclaration](lambda name, repo : Team(name, repo), ownerRepo)
        self.vendors : dict[str, InfrastructureVendor] = OrderedDict[str, InfrastructureVendor]()
        self.storagePolicies : dict[str, StoragePolicy] = OrderedDict[str, StoragePolicy]()
        self.classificationPolicies : set[DataClassificationPolicy] = set[DataClassificationPolicy]()
        self.documentation : Optional[Documentation] = None
        self.add(*args)

    def setEcosystem(self, eco : Ecosystem) -> None:
        """Sets the ecosystem for this zone and sets the zone for all teams"""
        self.key = GovernanceZoneKey(eco.key, self.name)

        self.add()

    def add(self, *args : Union[InfrastructureVendor, StoragePolicy, DataClassificationPolicy, TeamDeclaration, 'DataPlatform', Documentation]) -> None:
        for arg in args:
            if(type(arg) is InfrastructureVendor):
                iv : InfrastructureVendor = arg
                if self.vendors.get(iv.name) != None:
                    raise ObjectAlreadyExistsException(f"Duplicate Vendor {iv.name}")
                self.vendors[iv.name] = iv
            elif(isinstance(arg, DataClassificationPolicy)):
                dcc : DataClassificationPolicy = arg
                self.classificationPolicies.add(dcc)
            elif(isinstance(arg, StoragePolicy)):
                sp : StoragePolicy = arg
                if self.storagePolicies.get(sp.name) != None:
                    raise Exception(f"Duplicate Storage Policy {sp.name}")
                self.storagePolicies[sp.name] = sp
            elif(type(arg) is TeamDeclaration):
                t : TeamDeclaration = arg
                self.teams.addAuthorization(t)
            elif(isinstance(arg, DataPlatform)):
                p : DataPlatform = arg
                if self.platforms.get(p.name) != None:
                    raise ObjectAlreadyExistsException(f"Duplicate Platform {p.name}")
                self.platforms[p.name] = p
            elif(isinstance(arg, Documentation)):
                d : Documentation = arg
                self.documentation = d

        # Set softlink keys
        if(self.key):
            for vendor in self.vendors.values():
                vendor.setGovernanceZone(self)
            for sp in self.storagePolicies.values():
                sp.setGovernanceZone(self)
            for td in self.teams.authorizedNames.values():
                td.setGovernanceZone(self)

    def getVendor(self, name : str) -> Optional[InfrastructureVendor]:
        return self.vendors.get(name)
    
    def getVendorOrThrow(self, name : str) -> InfrastructureVendor:
        v : Optional[InfrastructureVendor] = self.getVendor(name)
        if(v):
            return v
        else:
            raise ObjectDoesntExistException(f"Unknown vendor {name}")

    def getTeam(self, name : str) -> Optional[Team]:
        return self.teams.getObject(name)
    
    def getTeamOrThrow(self, name : str) -> Team:
        t : Optional[Team] = self.getTeam(name)
        if(t):
            return t
        else:
            raise ObjectDoesntExistException(f"Unknown team {name}")

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, GovernanceZone):
            return super().__eq__(__value) and self.name == __value.name and self.platforms == __value.platforms and \
                self.teams == __value.teams and self.vendors == __value.vendors and self.storagePolicies == __value.storagePolicies and \
                self.documentation == __value.documentation
        return False

    def eq_toplevel(self, proposed: GitControlledObject) -> bool:
        """Just check the not git controlled attributes"""
        if not(super().eq_toplevel(proposed) and type(proposed) is GovernanceZone and self.name == proposed.name):
            return False
        return self.storagePolicies == proposed.storagePolicies and self.platforms == proposed.platforms and self.vendors == proposed.vendors and \
            self.teams.eq_toplevel(proposed.teams)
    
    def checkIfChangesAreAuthorized(self, proposed : GitControlledObject, changeSource : Repository, vTree : ValidationTree) -> None:
        proposedGZ : GovernanceZone = cast(GovernanceZone, proposed)
        
        """This checks if the governance zone has changed relative to the specified change source"""
        """This checks if any teams have been added or removed relative to e"""

        self.checkTopLevelAttributeChangesAreAuthorized(proposedGZ, changeSource, vTree)

        # Get the current teams from the change source
        self.teams.checkIfChangesAreAuthorized(proposedGZ.teams, changeSource, vTree)

    def lint(self, eco : Ecosystem, govTree : ValidationTree) -> None:
        """This validates a GovernanceZone and populates the teamcache with the zones teams"""
        for team in self.teams.authorizedObjects.values():
            td : TeamDeclaration = self.teams.authorizedNames[team.name]
            if(eco.teamCache.get(team.name) != None):
                govTree.addProblem(f"Duplicate TeamDeclaration {team.name}")
            else:
                eco.cache_addTeam(td, team)
                teamTree : ValidationTree = govTree.createChild(team)
                team.lint(eco, self, teamTree)
        self.superLint(govTree)
        self.teams.lint(govTree)
        if(self.key == None):
            govTree.addProblem("Key not set")

    def __str__(self) -> str:
        return f"GovernanceZone({self.name})"
    
    def isBackwardsCompatibleWith(self, originZone : 'GovernanceZone', tree : ValidationTree):
        """This checks if this zone is backwards compatible with the original zone. This means that the proposed zone
        can be used to replace this one without breaking any data pipelines"""

        # Check if the teams are compatible
        for team in self.teams.authorizedObjects.values():
            tTree : ValidationTree = tree.createChild(team)
            originTeam : Optional[Team] = originZone.getTeam(team.name)
            if originTeam:
                team.isBackwardsCompatibleWith(originTeam, tTree)
            else:
                tTree.addProblem(f"Team {team.name} is missing from zone {originZone.name}")

    def getDatasetStoragePolicies(self, dataset : Dataset) -> Sequence[StoragePolicy]:
        """Returns the storage policies for the specified dataset including mandatory ones"""
        rc : list[StoragePolicy] = []
        rc.extend(dataset.policies.values())
        for sp in self.storagePolicies.values():
            if(sp.mandatory == PolicyMandatedRule.MANDATED_WITHIN_ZONE):
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

class DataPlatform(object):
    """This is a system which can interpret data flows in the metadata and realize those flows"""
    def __init__(self, name : str) -> None:
        self.name : str = name

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

class DataLatency(Enum):
    """Specifies the acceptable latency range from a consumer"""
    SECONDS = 0 
    """Up to 59 seconds"""
    MINUTES = 1
    """Up to 59 minutes"""
    HOURS = 3
    """Up to 24 hours"""
    DAYS = 4 

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
    def __init__(self, r : DataRetentionPolicy, l : DataLatency, regulator : Optional[str], minRetentionDurationIfNeeded : Optional[timedelta] = None) -> None:
        self.policy : DataRetentionPolicy = r
        self.latency : DataLatency = l
        self.minRetentionTime : Optional[timedelta] = minRetentionDurationIfNeeded
        self.regulator : Optional[str] = regulator

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

class WorkspacePlatformConfig(object):
    """This allows a Workspace to specify per pipeline hints for behavior, i.e.
    allowed latency and so on"""
    def __init__(self, hist : ConsumerRetentionRequirements) -> None:
        self.retention : ConsumerRetentionRequirements = hist

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

class DeprecationsAllowed(Enum):
    """This specifies if deprecations are allowed for a specific dataset in a workspace dsg"""
    NEVER = 0
    """Deprecations are never allowed"""
    ALLOWED = 1
    """Deprecations are allowed but not will generate warnings"""

class DatasetSink(object):
    """This is a reference to a dataset in a Workspace"""
    def __init__(self, storeName : str, datasetName : str, deprecationsAllowed : DeprecationsAllowed = DeprecationsAllowed.NEVER) -> None:
        self.storeName : str = storeName
        self.datasetName : str = datasetName
        self.key = f"{self.storeName}:{self.datasetName}"
        self.deprecationsAllowed : DeprecationsAllowed = deprecationsAllowed

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is DatasetSink):
            return self.key == __value.key and self.storeName == __value.storeName and self.datasetName == __value.datasetName
        else:
            return False
        
    def lint(self, eco : Ecosystem, ws : 'Workspace', tree : ValidationTree):
        if(is_valid_sql_identifier(self.storeName) == False):
            tree.addProblem(f"DatasetSink store name {self.storeName} is not a valid SQL identifier")
        if(is_valid_sql_identifier(self.datasetName) == False):
            tree.addProblem(f"DatasetSink dataset name {self.datasetName} is not a valid SQL identifier")
        dataset : Optional[Dataset] = eco.cache_getDataset(self.storeName, self.datasetName)
        if(dataset == None):
            tree.addProblem(f"Unknown dataset {self.storeName}:{self.datasetName}")
        else:
            storeI : Optional[DatastoreCacheEntry] = eco.datastoreCache.get(self.storeName)
            if storeI:
                store : Datastore = storeI.datastore
                # Production data in non production or vice versa should be noted
                if(store.productionStatus != ws.productionStatus):
                    tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} is using a datastore with a different production status", ProblemSeverity.WARNING)
                if store.isDatasetDeprecated(dataset):
                    if self.deprecationsAllowed == DeprecationsAllowed.NEVER:
                        tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} is deprecated and deprecations are not allowed")
                    elif(self.deprecationsAllowed == DeprecationsAllowed.ALLOWED):
                        tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} is using deprecated dataset", ProblemSeverity.WARNING)
                dataset : Optional[Dataset] = store.datasets.get(self.datasetName)
                if(dataset == None):
                    tree.addProblem(f"Unknown dataset {self.storeName}:{self.datasetName}")
                else:
                    if(ws.classificationVerifier and not dataset.checkClassificationsAreOnly(ws.classificationVerifier)):
                        tree.addProblem(f"Dataset {self.storeName}:{self.datasetName} has unexpected classifications")
            else:
                tree.addProblem(f"Unknown datastore {self.storeName}")

    def __str__(self) -> str:
        return f"DatasetSink({self.storeName}:{self.datasetName})"

class DatasetGroup(ANSI_SQL_NamedObject):
    """A collection of Datasets which are rendered with a specific pipeline spec in a Workspace. The name should be
    ANSI SQL compliant because it could be used as part of a SQL View/Table name in a Workspace database"""
    def __init__(self, name : str, *args : Union[DatasetSink, WorkspacePlatformConfig]) -> None:
        super().__init__(name)
        self.platformMD : Optional[WorkspacePlatformConfig] = None
        self.sinks : dict[str, DatasetSink] = OrderedDict[str, DatasetSink]()
        for arg in args:
            if(type(arg) is DatasetSink):
                sink : DatasetSink = arg
                if(self.sinks.get(sink.key) != None):
                    raise ObjectAlreadyExistsException(f"Duplicate DatasetSink {sink.key}")
                self.sinks[sink.key] = sink
            elif(type(arg) is WorkspacePlatformConfig):
                if self.platformMD == None:
                    self.platformMD = arg
                else:
                    raise AttributeAlreadySetException("Platform")
            else:
                raise UnknownArgumentException(f"Unknown argument {type(arg)}")
            
    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())
    
    def lint(self, eco : Ecosystem, ws : 'Workspace', tree : ValidationTree):
        super().nameLint(tree)
        if(is_valid_sql_identifier(self.name) == False):
            tree.addProblem(f"DatasetGroup name {self.name} is not a valid SQL identifier")
        for sink in self.sinks.values():
            sinkTree : ValidationTree = tree.createChild(sink)
            sink.lint(eco, ws, sinkTree)

    def __str__(self) -> str:
        return f"DatasetGroup({self.name})"

class TransformerTrigger:
    pass

class CodeArtifact:
    """This defines a piece of code which can be used to transform data in a workspace"""
    pass

class PythonCodeArtifact(CodeArtifact):
    """This describes a python job and its dependencies"""
    def __init__(self, requirements : list[str], envVars : dict[str, str], requiredVersion : str) -> None:
        self.requirements : list[str] = requirements
        self.envVars : dict[str, str] = envVars
        self.requiredVersion : str = requiredVersion

class CodeExecutionEnvironment:
    """This is an environment which can execute code, AWS Lambda, Azure Functions, Kubernetes, etc"""
    pass

class KubernetesEnvironment(CodeExecutionEnvironment):
    """This is a Kubernetes environment"""
    def __init__(self, hostName : str, cred : Credential) -> None:
        self.hostName : str = hostName
        """This is the hostname of the Kubernetes cluster"""
        self.credential : Credential = cred
        """This is the credential used to access the Kubernetes cluster"""

class DataTransformer(ANSI_SQL_NamedObject):
    """This allows new data to be produced from existing data. The inputs to the transformer are the
    datasets in the workspace and the output is a Datastore associated with the transformer. The transformer
    will be triggered when needed"""
    def __init__(self, name : str, outputStoreName : str, trigger : TransformerTrigger, code : CodeArtifact, codeEnv : CodeExecutionEnvironment) -> None:
        super().__init__(name)
        self.outputStoreName : str = outputStoreName
        self.trigger : TransformerTrigger = trigger
        self.code : CodeArtifact = code
        self.codeEnv : CodeExecutionEnvironment = codeEnv

    def lint(self, eco : Ecosystem, ws : 'Workspace', tree : ValidationTree):
        super().nameLint(tree)
        # Does store exist
        storeI : Optional[DatastoreCacheEntry] = eco.datastoreCache.get(self.outputStoreName)
        if(storeI == None):
            tree.addProblem(f"Unknown datastore {self.outputStoreName}")
        else:
            if(storeI.datastore.productionStatus != ws.productionStatus):
                tree.addProblem(f"DataTransformer {self.name} is using a datastore with a different production status", ProblemSeverity.WARNING)
            
            workSpaceI : WorkspaceCacheEntry = eco.cache_getWorkspaceOrThrow(ws.name)
            if(workSpaceI.team != storeI.team):
                tree.addProblem(f"DataTransformer {self.name} is using a datastore from a different team", ProblemSeverity.ERROR)


class Workspace(ANSI_SQL_NamedObject):
    """A collection of datasets used by a consumer for a specific use case. This consists of one or more groups of datasets with each set using the correct pipeline spec.
    Specific datasets can be present in multiple groups. They will be named differently in each group. The name needs to be ANSI SQL because
    it could be used as part of a SQL View/Table name in a Workspace database. Workspaces must have ecosystem unique names"""
    def __init__(self, name : str, *args : Union[DatasetGroup, 'Asset', Documentation, DataClassificationPolicy, ProductionStatus, DeprecationInfo, DataTransformer]) -> None:
        super().__init__(name)
        self.dsgs : dict[str, DatasetGroup] = OrderedDict[str, DatasetGroup]()
        self.asset : Optional['Asset'] = None
        self.documentation : Optional[Documentation] = None
        self.productionStatus : ProductionStatus = ProductionStatus.NOT_PRODUCTION
        self.deprecationStatus : DeprecationInfo = DeprecationInfo(DeprecationStatus.NOT_DEPRECATED)
        self.dataTransformer : Optional[DataTransformer] = None
        # This is the set of classifications expected in the Workspace. Linting fails
        # if any datsets/attributes found with classifications different than these
        self.classificationVerifier : Optional[DataClassificationPolicy] = None
        """This workspace is the input to a data transformer if set"""
        for arg in args:
            if(type(arg) is DatasetGroup):
                dsg : DatasetGroup = arg
                if(self.dsgs.get(dsg.name) != None):
                    raise ObjectAlreadyExistsException(f"Duplicate DatasetGroup {dsg.name}")
                self.dsgs[dsg.name] = dsg
            elif(isinstance(arg, DataClassificationPolicy)):
                self.classificationVerifier = arg
            elif(isinstance(arg, Asset)):
                a : 'Asset' = arg
                if(self.asset != None and self.asset != a):
                    raise AttributeAlreadySetException("Asset")
                self.asset = a
            elif(isinstance(arg, ProductionStatus)):
                self.productionStatus = arg
            elif(isinstance(arg, DeprecationInfo)):
                self.deprecationStatus = arg
            elif(isinstance(arg, Documentation)):
                d : Documentation = arg
                if(self.documentation != None and self.documentation != d):
                    raise AttributeAlreadySetException("Documentation")
                self.documentation = d
            elif(isinstance(arg, DataTransformer)):
                dt : DataTransformer = arg
                if(self.dataTransformer != None and self.dataTransformer != dt):
                    raise AttributeAlreadySetException("DataTransformer")
                self.dataTransformer = dt
            else:
                raise UnknownArgumentException(f"Unknown argument {type(arg)}")

    def __hash__(self) -> int:
        return hash(self.name)
    
    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())
    
    def isDatastoreUsed(self, store : Datastore) -> bool:
        """Returns true if the specified datastore is used by this workspace"""
        for dsg in self.dsgs.values():
            for sink in dsg.sinks.values():
                if sink.storeName == store.name:
                    return True
        return False
    
    def lint(self, eco : Ecosystem, tree : ValidationTree):
        super().nameLint(tree)
        if not is_valid_sql_identifier(self.name):
            tree.addProblem(f"Workspace name {self.name} is not a valid SQL identifier")

        # Check production status of workspace matches all datasets in use
        # Check deprecation status of workspace generates warnings for all datasets in use
        # Lint the DSGs
        for dsg in self.dsgs.values():
            dsgTree : ValidationTree = tree.createChild(dsg)
            dsg.lint(eco, self, dsgTree)

        # Link the transformer if present
        if self.dataTransformer:
            dtTree : ValidationTree = tree.createChild(self.dataTransformer)
            self.dataTransformer.lint(eco, self, dtTree)

    def __str__(self) -> str:
        return f"Workspace({self.name})"    
    
class Asset:
    def __init__(self, name : str, containers : Iterable[DataContainer]) -> None:
        self.name : str = name
        self.containers : dict[str, 'DataContainer'] = OrderedDict()
        for container in containers:
            self.containers[container.getName()] = container
        self.consumers : dict[str, Workspace] = OrderedDict()

    def addConsumer(self, consumer : Workspace):
        if self.consumers.get(consumer.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate consumer {consumer.name}")
        self.consumers[consumer.name] = consumer

class PlatformStyle(Enum):
    OLTP = 0
    OLAP = 1
    COLUMNAR = 2
    OBJECT = 3

class ManagedAsset(Asset):
    def __init__(self, name: str, style : PlatformStyle, rawAsset : Asset) -> None:
        super().__init__(rawAsset.name, rawAsset.containers.values())
        self.rawAsset : Asset = rawAsset
        self.platformStyle = style

class AssetSet(Asset):

    def hydrateContainers(self):
        self.containers = OrderedDict()
        for a in self.assets.values():
            for c in a.containers.values():
                self.containers[c.getName()] = c

    def __init__(self, name : str, *args : Asset) -> None:
        super().__init__(name, [])
        self.activeAssetName : Optional[str] = None
        self.assets : dict[str, Asset] = OrderedDict()
        self.add(*args)

    def add(self, *args : Asset) -> None:
        for arg in args:
                self.addAsset(arg)
        self.hydrateContainers()

    def addAsset(self, asset : Asset):
        if self.assets.get(asset.name) != None:
            raise Exception(f"Duplicate Asset {asset.name}")
        self.assets[asset.name] = asset
    def setActiveAsset(self, assetName : str):
        if(self.assets.get(assetName) != None):
            self.activeAssetName = assetName
        else:
            raise AssetDoesntExistException(f"Unknown asset {assetName}")

