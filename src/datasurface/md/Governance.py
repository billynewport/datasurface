from dataclasses import dataclass
from collections import OrderedDict
from typing import Any, Iterable, List, Mapping, Optional, Sequence, TypeVar, Union, cast
from .Schema import Schema
from abc import ABC, abstractmethod
from .Exceptions import AttributeAlreadySetException, ObjectAlreadyExistsException, ObjectDoesntExistException, UnknownArgumentException, DatasetDoesntExistException, DatastoreDoesntExistException, AssetDoesntExistException
from datetime import timedelta
from enum import Enum
from typing import Optional, TypeVar

T = TypeVar('T')

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
        return False
    
    def checkTopLevelAttributeChangesAreAuthorized(self, proposed : 'GitControlledObject', changeSource : 'Repository') -> List['ValidationProblem']:
        """This checks if the local attributes of the object have been modified by the authorized change source"""
        rc : List[ValidationProblem] = []

        # Check if the ecosystem has been modified at all
        if(self == proposed):
            return rc # No changes then no problems
        elif(not self.eq_toplevel(proposed) and self.owningRepo != changeSource):
            rc.append(ValidationProblem("Ecosystem top level has been modified by an unauthorized source"))
        return rc

    @abstractmethod    
    def checkIfChangesAreAuthorized(self, proposed : 'GitControlledObject', changeSource : 'Repository') -> List['ValidationProblem']:
        return []
        
    def checkDictChangesAreAuthorized(self, current : Mapping[str, 'GitControlledObject'], proposed : Mapping[str, 'GitControlledObject'], changeSource : 'Repository') -> List['ValidationProblem']:
        """This checks if the current dict has been modified relative to the specified change source"""
        """This checks if any Governance zones has been added or removed relative to e"""


        # Get the current committed state from the change source
        current_keys : set[str] = set(current)

        rc : List[ValidationProblem] = []

        # Get the governance zones from the ecosystem
        proposed_keys : set[str] = set(proposed.keys())

        deleted_keys : set[str] = current_keys - proposed_keys
        added_keys : set[str] = proposed_keys - current_keys

        # first check any top level governance zones have been added or removed by the correct change sources
        for key in deleted_keys:
            # Check if the zone was deleted by the authoized change source
            obj : Optional[GitControlledObject] = current[key]
            if(obj.owningRepo != changeSource):
                rc.append(ValidationProblem(f"Key {key} has been deleted by an unauthorized source"))
        
        for key in added_keys:
            # Check if the zone was added by the specified change source
            obj : Optional[GitControlledObject] = proposed[key]
            if(obj.owningRepo != changeSource):
                rc.append(ValidationProblem(f"Key {key} has been added by an unauthorized source"))

        # Now check each common governance zone for changes
        common_keys : set[str] = current_keys.intersection(proposed_keys)
        for key in common_keys:
            prop : Optional[GitControlledObject] = proposed[key]
            curr : Optional[GitControlledObject] = current[key]
            if(prop != curr):
                if(curr.owningRepo != changeSource):
                    rc.append(ValidationProblem(f"Key {key} has been modified by an unauthorized source"))
                else:
                    # Check prop against curr for unauthorized changes
                    rc.extend(curr.checkIfChangesAreAuthorized(prop, changeSource))
        return rc


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

class InfraPath:
    """This is a path to a location in the infrastructure hierarchy. It is used to specify the location of a container"""
    def __init__(self, eco : str, gz : str, iv : str, locPath : list['InfraLocation']):
        self.ecosystemName : str = eco
        self.governanceZoneName : str = gz
        self.infraVendorName : str = iv
        self.locPath : list[InfraLocation] = locPath

    def __str__(self) -> str:
        return f"{self.ecosystemName}:{self.governanceZoneName}:{self.infraVendorName}:{self.locPath}"
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, InfraPath) and self.ecosystemName == __value.ecosystemName and \
            self.governanceZoneName == __value.governanceZoneName and \
            self.infraVendorName == __value.infraVendorName and self.locPath == __value.locPath

class StoragePolicy(ABC):
    '''This is the base class for storage policies. These are owned by a governance zone and are used to determine whether a container is compatible with the policy.'''

    def __init__(self, name : str, isMandatory : bool) -> None:
        self.name : str = name
        self.mandatory : bool = isMandatory
        """If true then all data containers MUST comply with this policy regardless of whether a dataset specifies this policy or not"""
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, StoragePolicy) and self.name == __value.name and self.mandatory == __value.mandatory
    

    @abstractmethod
    def isCompatible(self, policyPath : InfraPath, containerPath : InfraPath, container : 'DataContainer') -> bool:
        '''This returns true if the container is compatible with the policy. This is used to determine whether data tagged with a policy can be stored in a specific container.'''
        return False

class StoragePolicyAllowAnyContainer(StoragePolicy):
    '''This is a storage policy that allows any container to be used.'''
    def __init__(self, name : str, isMandatory : bool) -> None:
        super().__init__(name, isMandatory)

    def isCompatible(self, policyPath : InfraPath, containerPath : InfraPath, container : 'DataContainer') -> bool:
        return True
    
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is StoragePolicyAllowAnyContainer and \
            self.name == __value.name and self.mandatory == __value.mandatory

class LocalGovernanceManagedOnly(StoragePolicy):
    """A policy which only allows containers in the same governance zone as the policy"""
    def __init__(self, name : str, isMandatory : bool) -> None:
        super().__init__(name, isMandatory)

    def isCompatible(self, policyPath : InfraPath, containerPath : InfraPath, container : 'DataContainer') -> bool:
        """Only allow if the location is managed by the required zone"""
        return containerPath.governanceZoneName == policyPath.governanceZoneName
    
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is LocalGovernanceManagedOnly and \
            self.name == __value.name and self.mandatory == __value.mandatory

class InfraLocation:
    """This is a location within a vendors physical location hierarchy. This object
    is only fully initialized after construction when either the setParentLocation or
    setVendor methods are called. This is because the vendor is required to set the parent"""

    def __init__(self, name: str, *args: 'InfraLocation') -> None:
        self.name: str = name

        self.locations: dict[str, 'InfraLocation'] = OrderedDict()
        """These are the 'child' locations under this location. A state location would have city children for example"""
        """This specifies the parent location of this location. State is parent on city and so on"""
        self.containers: dict[str, 'DataContainer'] = OrderedDict()
        self.add(*args)

    def add(self, *args : 'InfraLocation') -> None:
        for loc in args:
            self.addLocation(loc)

    def addLocation(self, loc : 'InfraLocation'):
        if self.locations.get(loc.name) != None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc

    def addContainer(self, c : 'DataContainer'):
        if self.containers.get(c.getName()) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Container {c.name}")
        self.containers[c.getName()] = c
        c.location = self

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())
    
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
    def __init__(self, name : str, *args : InfraLocation) -> None:
        self.name : str = name
        self.locations : dict[str, 'InfraLocation'] = OrderedDict()
        self.add(*args)

    def add(self, *args : 'InfraLocation') -> None:
        for loc in args:
            self.addLocation(loc)

    def addLocation(self, loc : 'InfraLocation'):
        if self.locations.get(loc.name) != None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())
        
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
    """This is a container for data. It is a physical location where data is stored. It is owned by a data platform
      and is used to determine whether a dataset is compatible with the container by a governancezone."""   
    def __init__(self, *args : 'InfraLocation') -> None:
        self.location : Optional[InfraLocation] = None
        self.name : Optional[str] = None
        self.serverSideEncryptionKeys : Optional[EncryptionSystem] = None
        """This is the vendor ecnryption system providing the container. For example, if a cloud vendor
        hosts the container, do they have access to the container data?"""
        self.clientSideEncryptionKeys : Optional[EncryptionSystem] = None
        """This is the encryption system used by the client to encrypt data before sending to the container. This could be used
        to encrypt data before sending to a cloud vendor for example"""
        self.isReadOnly : bool =  False
        self.add(*args)

    def add(self, *args : 'InfraLocation') -> None:
        for loc in args:
            if(self.location != None):
                raise ObjectAlreadyExistsException("Location already set")
            self.location = loc

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())
        
    def getName(self) -> str:
        """Returns the name of the container"""
        if(self.name):
            return self.name
        else:
            raise Exception("Container name not set")

class Dataset(object):
    """This is a single collection of homogeneous records with a primary key"""
    def __init__(self, name : str, *args : Union[Schema, StoragePolicy]) -> None:
        self.name : str = name
        self.originalSchema : Optional[Schema] = None
        self.policies : dict[str, StoragePolicy] = OrderedDict()
        self.add(*args)

    def add(self, *args : Union[Schema, StoragePolicy]) -> None:
        for arg in args:
            if(isinstance(arg,Schema)):
                s : Schema = arg
                self.originalSchema = s
            else:
                p : StoragePolicy = arg
                self.addPolicy(p)
    
    def addPolicy(self, s : StoragePolicy) -> None:
        if self.policies.get(s.name) != None:
            raise Exception(f"Duplicate policy {s.name}")
        self.policies[s.name] = s

    def __eq__(self, __value: object) -> bool:
        """Check for equality but shallow check to referenced objects to prevent recursion"""
        return cyclic_safe_eq(self, __value, set())

    def validate(self):
        """Place holder to validate constraints on the dataset"""
#        for policy in self.policies.values():
#            if(policy.governanceZone != self.getZone()):
#                raise StoragePolicyFromDifferentZone("Datasets must be governed by storage policies from its managing zone")
        pass       
    
class DataSourceConnection:
    def __init__(self, name : str) -> None:
        self.name : str = name

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is DataSourceConnection):
            return self.name == __value.name
        else:
            return False

class Credential:
    """These allow a client to connect to a service/server"""
    def __init__(self) -> None:
        pass

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())


class FileSecretCredential(Credential):
    """This allows a secret to be read from the local filesystem. Usually the secret is
    placed in the file using an external service such as Docker secrets etc. The secret should be in the
    form of 2 lines, first line is user name, second line is password"""
    def __init__(self, filePath : str) -> None:
        super().__init__()
        self.secretFilePath : str = filePath

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is FileSecretCredential and self.secretFilePath == __value.secretFilePath

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

class LocalJDBCConnection(DataSourceConnection):
    def __init__(self, name: str, jdbcUrl : str, cred : Credential) -> None:
        super().__init__(name)
        self.jdbcUrl : str = jdbcUrl
        self.credential : Credential = cred

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is LocalJDBCConnection and self.jdbcUrl == __value.jdbcUrl and self.credential == __value.credential

class CaptureSourceInfo:
    """Describes how an IMD can connect to the database or similar to ingest data"""
    def __init__(self) -> None:
        pass

    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, CaptureSourceInfo)):
            return True
        else:
            return False

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
        
class CaptureType(Enum):
    SNAPSHOT = 0
    INCREMENTAL = 1

class IngestionConsistencyType(Enum):
    """This determines whether data is ingested in consistent groups across multiple datasets or
    whether each dataset is ingested independently"""
    SINGLE = 0
    MULTI = 1

class CaptureMetaData(object):
    """Producers use these to describe HOW to snapshot and pull deltas from a data source in to
    data pipelines. The ingestion service interprets these to allow code free ingestion from
    supported sources and handle operation pipelines."""
    def __init__(self, *args : Union[Credential, CaptureSourceInfo, IngestionConsistencyType]) -> None:
        self.credential : Optional[Credential] = None
        self.SingleOrMultiDatasetIngestion : Optional[IngestionConsistencyType] = None
        self.captureSource : Optional[CaptureSourceInfo] = None
        self.add(*args)

    def add(self, *args : Union[Credential, CaptureSourceInfo, IngestionConsistencyType]) -> None:
        for arg in args:
            if(isinstance(arg, Credential)):
                c : Credential = arg
                self.credential = c
            elif(type(arg) == IngestionConsistencyType):
                sm : IngestionConsistencyType = arg
                self.SingleOrMultiDatasetIngestion = sm
            elif(isinstance(arg, CaptureSourceInfo)):
                i : CaptureSourceInfo = arg
                self.captureSource = i
            
    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

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
        return cyclic_safe_eq(self, __value, set())
    


class Datastore(object):

    """This is a named group of datasets. It describes how to capture the data and make it available for processing"""
    def __init__(self, name : str, *args : Union[Dataset, CaptureMetaData, DataContainer]) -> None:
        self.name : str = name
        self.datasets : dict[str, Dataset] = OrderedDict()
        self.imd : Optional[CaptureMetaData] = None
        self.container : Optional[DataContainer] = None
        self.add(*args)

    def add(self, *args : Union[Dataset, CaptureMetaData, DataContainer]) -> None:
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

    def addDataset(self, item : Dataset) -> None:
        """Add a named dataset"""
        if self.datasets.get(item.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Dataset {item.name}")
        self.datasets[item.name] = item

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())
        
class ValidationProblem:
    def __init__(self, desc : str) -> None:
        self.object : object = None
        """The original object that is in use"""
        self.description : Optional[str] = desc
        """A description of what the issue is"""

    def __str__(self) -> str:
        if(self.object):
            return f"{self.description} {self.object}"
        elif(self.description):
            return self.description
        else:
            return "Unknown validation problem"


class Repository(ABC):
    pass

class GitRepository(Repository):
    """This represents a source of changes. All changes to objects in the ecosystem are gated to come from a specific repository"""
    def __init__(self, repo : str, moduleName : str) -> None:
        self.repoURL : str = repo
        """The name of the git repository from which changes to Team objects are authorized"""
        self.moduleName : str = moduleName
        """The name of the root module contain a main function to declare the teams"""

    def __eq__(self, __value: object) -> bool:
        if(isinstance(__value, GitRepository)):
            return self.repoURL == __value.repoURL and self.moduleName == __value.moduleName
        else:
            return False

# Add regulators here with their named retention policies for reference in Workspaces
# Feels like regulators are across GovernanceZones
class Ecosystem(GitControlledObject):
    def __init__(self, name : str, repo : Repository, *args : 'GovernanceZone') -> None:
        super().__init__(repo)
        self.name : str = name

        self.governanceZones : dict[str, GovernanceZone] = OrderedDict()
        """This is the authorative list of governance zones within the ecosystem"""

        self.resetCaches()
        self.add(*args)

    def resetCaches(self) -> None:
        """Empties the caches"""
        self.datastoreCache : dict[str, 'Datastore'] = {}
        """This is a cache of all data stores in the ecosystem"""
        self.workSpaceCache : dict[str, 'Workspace'] = {}
        """This is a cache of all workspaces in the ecosystem"""
        self.teamDeclarationCache : dict[str, 'TeamDeclaration'] = {}
        """This is a cache of all team declarations in the ecosystem"""

    def add(self, *args : 'GovernanceZone') -> None:
        for arg in args:
            self.addZoneDef(arg)
            
    def addZoneDef(self, z : 'GovernanceZone'):
        """Adds a zone def to the ecosystem and sets the zone to this ecosystem"""
        if(self.governanceZones.get(z.name) != None):
            raise ObjectAlreadyExistsException(f"GoveranceZone already exists {z.name}")
        self.governanceZones[z.name] = z

    def cache_addTeamDeclaration(self, td : 'TeamDeclaration'):
        if(self.teamDeclarationCache.get(td.name) != None):
            if(self.teamDeclarationCache.get(td.name) != None):
                raise ObjectAlreadyExistsException(f"Duplicate TeamDeclaration {td.name}")
            self.teamDeclarationCache[td.name] = td

    def cache_addWorkspace(self, work : 'Workspace'):
        """This adds a workspace to the eco cache and flags duplicates"""
        if(self.workSpaceCache.get(work.name) != None):
            raise ObjectAlreadyExistsException(f"Duplicate workspace {work.name}")
        self.workSpaceCache[work.name] = work

    def cache_addDatastore(self, store : 'Datastore'):
        """This adds a store to the eco cache and flags duplicates"""
        if(self.datastoreCache.get(store.name) != None):
            raise ObjectAlreadyExistsException(f"Duplicate data store {store.name}")
        self.datastoreCache[store.name] = store

    def cache_getDatastoreOrThrow(self, store : str):
        s : Optional[Datastore] = self.datastoreCache.get(store)
        if(s):
            return s
        else:
            raise DatastoreDoesntExistException(f"Unknown datastore {store}")

    def cache_getDatasetOrThrow(self, store : str, set : str):
        s : Datastore = self.cache_getDatastoreOrThrow(store)
        dataset = s.datasets.get(set)
        if(dataset == None):
            raise DatasetDoesntExistException(f"Dataset doesn't exist {store}:{set}")
        return dataset

    def validateDatastore(self, gz : 'GovernanceZone', t : 'TeamDeclaration', s : Datastore) -> Sequence[ValidationProblem]:
        problems : List[ValidationProblem] = []
        # TODO code this
        return problems

    def validateTeamDeclaration(self, gz: 'GovernanceZone', td: 'TeamDeclaration') -> Sequence['ValidationProblem']:
        """This validates a single team declaration and populates the datastore cache with that team's stores"""
        problem_list : List[ValidationProblem] = []
        for s in td.getTeam().dataStores.values():
            if self.datastoreCache.get(s.name) is not None:
                p = ValidationProblem(f"Duplicate Datastore {s.name}")
                p.object = s
                problem_list.append(p)
            else:
                self.datastoreCache[s.name] = s
                self.validateDatastore(gz, td, s)
        for w in td.getTeam().workspaces.values():
            if self.workSpaceCache.get(w.name) is not None:
                p = ValidationProblem(f"Duplicate Workspace {w.name}")
                p.object = td
                problem_list.append(p)
                # Cannot validate Workspace datasets until everything is loaded
            else:
                self.workSpaceCache[w.name] = w
        return problem_list

    def validateGoveranceZone(self, gz : 'GovernanceZone') -> Sequence[ValidationProblem]:
        """This validates a GovernanceZone and populates the teamcache with the zones teams"""
        list : List[ValidationProblem] = []
        for t in gz.teams.values():
            if(self.teamDeclarationCache.get(t.name) != None):
                p : ValidationProblem = ValidationProblem(f"Duplicate TeamDeclaration {t.name}")
                p.object = t
                list.append(p)
            else:
                self.teamDeclarationCache[t.name] = t
                self.validateTeamDeclaration(gz, t)
        return list

    def validateAndHydrateCaches(self) -> Sequence[ValidationProblem]:
        """This validates the ecosystem and returns a list of problems which is empty if there are no issues"""
        self.resetCaches()

        list : List[ValidationProblem] = []
        """No need to dedup zones as the authorative list is already a dict"""
        for gz in self.governanceZones.values():
            self.validateGoveranceZone(gz)

        """All caches should now be populated"""

        for work in self.workSpaceCache.values():
            for dsg in work.dsgs.values():
                for sink in dsg.datasets.values():
                    try:
                        # Check all datasets in the workspace exist
                        self.cache_getDatasetOrThrow(sink.storeName, sink.datasetName)
                    except Exception as e:
                        p : ValidationProblem = ValidationProblem(str(e))
                        p.object = sink
                        list.append(p)
        return list

    def checkIfChangesAreAuthorized(self, proposed : GitControlledObject, changeSource : Repository) -> List[ValidationProblem]:
        """This checks if the ecosystem top level has changed relative to the specified change source"""
        """This checks if any Governance zones has been added or removed relative to e"""

        prop_eco : Ecosystem = cast(Ecosystem, proposed)

        rc : List[ValidationProblem] = self.checkTopLevelAttributeChangesAreAuthorized(prop_eco, changeSource)

        rc.extend(self.checkDictChangesAreAuthorized(self.governanceZones, prop_eco.governanceZones, changeSource))

        return rc
    
    def __eq__(self, proposed: object) -> bool:
        return cyclic_safe_eq(self, proposed, set())
    
    def eq_toplevel(self, proposed: GitControlledObject) -> bool:
        """This is a shallow equality check for the top level ecosystem object"""
        if(isinstance(proposed, Ecosystem)):
            return self.name == proposed.name and self.owningRepo == proposed.owningRepo
        else:
            return False
        
    def getTeam(self, gz : str, teamName : str) -> Optional['Team']:
        """Returns the team with the specified name in the specified zone"""
        zone : Optional[GovernanceZone] = self.governanceZones.get(gz)
        if(zone):
            td : Optional[TeamDeclaration] = zone.teams.get(teamName)
            if(td):
                return td.getTeam() 
        else:
            return None

class Team:
    """This is the authoritive definition of a team within a goverance zone. All teams must have
    a corresponding TeamDeclaration in the owning GovernanceZone"""
    def __init__(self, td : 'TeamDeclaration', *args : Union[Datastore, 'Workspace']) -> None:
        self.td : 'TeamDeclaration' = td
        self.workspaces : dict[str, Workspace] = OrderedDict()
        self.dataStores : dict[str, Datastore] = OrderedDict()
        self.add(*args)

    def add(self, *args : Union[Datastore, 'Workspace']) -> None:
        """Adds a workspace, datastore or gitrepository to the team"""
        for arg in args:
            if(type(arg) is Datastore):
                s : Datastore = arg
                self.addStore(s)
            elif(type(arg) is Workspace):
                w : Workspace = arg
                self.addWorkspace(w)

    def getName(self) -> str:
        """Returns the name of the team"""
        return self.td.name
    
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
        return cyclic_safe_eq(self, __value, set())
        
class TeamDeclaration(GitControlledObject):
    """All teams must be declared at the GovernanceZone level using one of these objects. The Team objects are initialized in a secondary step"""
    def __init__(self, name : str, repo : Repository) -> None:
        super().__init__(repo)
        self.name : str = name
    
        """Changes to the Team object can only be done using committed changes in the specified repository"""
        """Files for this team must be in this folder in the master repo"""
        self.team : Optional[Team] = None

    def getTeam(self) -> Team:
        """Return a singleton Team object managed by this declaration"""
        if(not self.team):
            self.team = Team(self) 
        return self.team

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

    def eq_toplevel(self, proposed: GitControlledObject) -> bool:
        """Just check the not git controlled attributes"""
        propTD : TeamDeclaration = cast(TeamDeclaration, proposed)
        return super().eq_toplevel(proposed) and type(propTD) is TeamDeclaration and self.name == propTD.name and self.team == propTD.team
    
    def checkIfChangesAreAuthorized(self, proposed : GitControlledObject, changeSource : Repository) -> List[ValidationProblem]:
        prop_Team : TeamDeclaration = cast(TeamDeclaration, proposed)

        """This checks if the team has changed relative to the specified change source"""
        rc : List[ValidationProblem] = []

        rc.extend(self.checkTopLevelAttributeChangesAreAuthorized(prop_Team, changeSource))

        return rc

class GovernanceZone(GitControlledObject):

    """This declares the existence of a specific GovernanceZone and defines the teams it manages, the storage policies
    and which repos can be used to pull changes for various metadata"""
    def __init__(self, name : str, ownerRepo : Repository, *args : Union[InfrastructureVendor, StoragePolicy, TeamDeclaration, 'DataPlatform']) -> None:
        super().__init__(ownerRepo)
        self.name : str = name
        self.platforms : dict[str, 'DataPlatform'] = OrderedDict[str, 'DataPlatform']()
        self.teams : dict[str, TeamDeclaration] = OrderedDict[str, TeamDeclaration]()
        self.vendors : dict[str, InfrastructureVendor] = OrderedDict[str, InfrastructureVendor]()
        self.storagePolicies : dict[str, StoragePolicy] = OrderedDict[str, StoragePolicy]()
        self.add(*args)

    def add(self, *args : Union[InfrastructureVendor, StoragePolicy, TeamDeclaration, 'DataPlatform']) -> None:
        for arg in args:
            if(type(arg) is InfrastructureVendor):
                vendor : InfrastructureVendor = arg
                self.addVendor(vendor)
            elif(isinstance(arg, StoragePolicy)):
                s : StoragePolicy = arg
                self.addPolicy(s)
            elif(type(arg) is TeamDeclaration):
                t : TeamDeclaration = arg
                self.addTeam(t)
            elif(isinstance(arg, DataPlatform)):
                p : DataPlatform = arg
                self.addPlatform(p)

    def addPlatform(self, p : 'DataPlatform'):
        if self.platforms.get(p.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Platform {p.name}")
        self.platforms[p.name] = p

    def addPolicy(self, p : StoragePolicy):
        if self.storagePolicies.get(p.name) != None:
            raise Exception(f"Duplicate Storage Policy {p.name}")
        self.storagePolicies[p.name] = p

    def addTeam(self, t : TeamDeclaration):
        if self.teams.get(t.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Team {t.name}")
        self.teams[t.name] = t

    def addVendor(self, iv : InfrastructureVendor):
        if self.vendors.get(iv.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Vendor {iv.name}")
        self.vendors[iv.name] = iv

    def getTeam(self, name : str) -> Team:
        """This retrieves the team object for a declared team. Additional team elements can then be added to the team"""
        td : Optional[TeamDeclaration] = self.teams.get(name)
        if(td == None):
            raise ObjectDoesntExistException(f"Team {name} doesn't exist")
        return td.getTeam()

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

    def eq_toplevel(self, proposed: GitControlledObject) -> bool:
        """Just check the not git controlled attributes"""
        if super().eq_toplevel(proposed) and type(proposed) is GovernanceZone and self.name == proposed.name:
            return True
        prop_gZone : GovernanceZone = cast(GovernanceZone, proposed)
        return self.storagePolicies == prop_gZone.storagePolicies and self.platforms == prop_gZone.platforms and self.vendors == prop_gZone.vendors
    
    def checkIfChangesAreAuthorized(self, proposed : GitControlledObject, changeSource : Repository) -> List[ValidationProblem]:
        prop_TD : GovernanceZone = cast(GovernanceZone, proposed)
        
        """This checks if the governance zone has changed relative to the specified change source"""
        """This checks if any teams have been added or removed relative to e"""

        rc : List[ValidationProblem] = self.checkTopLevelAttributeChangesAreAuthorized(prop_TD, changeSource)

        # Get the current teams from the change source
        self.checkDictChangesAreAuthorized(self.teams, prop_TD.teams, changeSource)

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

class DatasetSink(object):
    """This is a reference to a dataset in a Workspace"""
    def __init__(self, storeName : str, datasetName : str) -> None:
        self.storeName : str = storeName
        self.datasetName : str = datasetName
        self.key = f"{self.storeName}:{self.datasetName}"

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is DatasetSink):
            return self.key == __value.key and self.storeName == __value.storeName and self.datasetName == __value.datasetName
        else:
            return False

class DatasetGroup(object):
    """A collection of Datasets which are rendered with a specific pipeline spec in a Workspace"""
    def __init__(self, name : str, *args : Union[DatasetSink, WorkspacePlatformConfig]) -> None:
        self.name : str = name
        self.platformMD : Optional[WorkspacePlatformConfig] = None
        self.datasets : dict[str, DatasetSink] = OrderedDict[str, DatasetSink]()
        for arg in args:
            if(type(arg) is DatasetSink):
                sink : DatasetSink = arg
                if(self.datasets.get(sink.key) != None):
                    raise ObjectAlreadyExistsException(f"Duplicate DatasetSink {sink.key}")
                self.datasets[sink.key] = sink
            elif(type(arg) is WorkspacePlatformConfig):
                if self.platformMD == None:
                    self.platformMD = arg
                else:
                    raise AttributeAlreadySetException("Platform")
            else:
                raise UnknownArgumentException(f"Unknown argument {type(arg)}")
            
    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())

class Workspace(object):
    """A collection of datasets used by a consumer for a specific use case. This consists of one or more groups of datasets with each set using the correct pipeline spec.
    Specific datasets can be present in multiple groups. They will be named differently in each group"""
    def __init__(self, name : str, *args : Union[DatasetGroup, 'Asset']) -> None:
        self.name : str = name
        self.dsgs : dict[str, DatasetGroup] = OrderedDict[str, DatasetGroup]()
        self.asset : Optional['Asset'] = None
        for arg in args:
            if(type(arg) is DatasetGroup):
                dsg : DatasetGroup = arg
                if(self.dsgs.get(dsg.name) != None):
                    raise ObjectAlreadyExistsException(f"Duplicate DatasetGroup {dsg.name}")
                self.dsgs[dsg.name] = dsg
            elif(isinstance(arg, Asset)):
                a : 'Asset' = arg
                if(self.asset != None and self.asset != a):
                    raise AttributeAlreadySetException("Asset")
                self.asset = a
            else:
                raise UnknownArgumentException(f"Unknown argument {type(arg)}")

    def __eq__(self, __value: object) -> bool:
        return cyclic_safe_eq(self, __value, set())
    
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

    def __init__(self, name : str, *args : Any) -> None:
        super().__init__(name, [])
        self.activeAssetName : Optional[str] = None
        self.assets : dict[str, Asset] = OrderedDict()
        self.add(*args)

    def add(self, *args : Any) -> None:
        for arg in args:
            if(isinstance(arg,Asset)):
                a : Asset = arg
                self.addAsset(a)
            else:
                raise UnknownArgumentException(f"Unknown argument {type(arg)}")
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

