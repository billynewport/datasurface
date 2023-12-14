from dataclasses import dataclass
from collections import OrderedDict
from typing import Any, Iterable, List, Optional, Sequence, Union
from .Schema import Schema
from abc import ABC, abstractmethod
from .Exceptions import ObjectAlreadyExistsException, ObjectDoesntExistException, UnknownArgumentException, DatasetDoesntExistException, DatastoreDoesntExistException, StoragePolicyFromDifferentZone, AssetDoesntExistException
from datetime import timedelta

from enum import Enum

class StoragePolicy(ABC):
    '''This is the base class for storage policies. These are owned by a governance zone and are used to determine whether a container is compatible with the policy.'''

    def __init__(self, name : str, isMandatory : bool) -> None:
        self.name : str = name
        self.governanceZone : Optional['GovernanceZone'] = None
        self.mandatory : bool = isMandatory
        """If true then all data containers MUST comply with this policy regardless of whether a dataset specifies this policy or not"""

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, type(self)):
            return self.name == __value.name and self.governanceZone == __value.governanceZone and self.mandatory == __value.mandatory
        return False
    

    @abstractmethod
    def isCompatible(self, container : 'DataContainer') -> bool:
        '''This returns true if the container is compatible with the policy. This is used to determine whether data tagged with a policy can be stored in a specific container.'''
        return False

class StoragePolicyAllowAnyContainer(StoragePolicy):
    '''This is a storage policy that allows any container to be used.'''
    def __init__(self, name : str, isMandatory : bool) -> None:
        super().__init__(name, isMandatory)

    def isCompatible(self, container : 'DataContainer') -> bool:
        return True
    
    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, type(self)):
            return super().__eq__(__value) and self.name == __value.name and self.governanceZone == __value.governanceZone and self.mandatory == __value.mandatory
        else:
            return False

class LocalGovernanceManagedOnly(StoragePolicy):
    """A policy which only allows containers in the same governance zone as the policy"""
    def __init__(self, name : str, isMandatory : bool, localZone : 'GovernanceZone') -> None:
        super().__init__(name, isMandatory)
        self.localZone = localZone

    def isCompatible(self, container : 'DataContainer') -> bool:
        """Only allow if the location is managed by the required zone"""
        if(container.location and container.location.vendor):
            return container.location.vendor.zone == self.localZone
        else:
            return False
    
    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is LocalGovernanceManagedOnly and self.name == __value.name and self.governanceZone == __value.governanceZone and self.mandatory == __value.mandatory and self.localZone == __value.localZone

class InfraLocation:
    """This is a location within a vendors physical location hierarchy"""

    def setVendor(self, v : Optional['InfrastructureVendor']):
        self.vendor = v
        for l in self.locations.values():
            l.setVendor(v)

    def __init__(self, name: str, *args: 'InfraLocation') -> None:
        self.name: str = name
        self.parentLocation: Optional['InfraLocation'] = None
        self.vendor: Optional['InfrastructureVendor'] = None

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
        loc.setVendor(self.vendor)
        loc.parentLocation = self

    def addContainer(self, c : 'DataContainer'):
        if self.containers.get(c.getName()) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Container {c.name}")
        self.containers[c.getName()] = c
        c.location = self

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is InfraLocation):
            v : InfraLocation = __value
            return self.name == v.name and self.vendor == v.vendor and self.locations == v.locations and self.parentLocation == v.parentLocation and self.containers == v.containers
        else:
            return False
        

class InfrastructureVendor:
    """This is a vendor which supplies infrastructure for storage and compute. It could be an internal supplier within an enterprise or an external cloud provider"""
    def __init__(self, name : str, *args : InfraLocation) -> None:
        self.name : str = name
        self.locations : dict[str, 'InfraLocation'] = OrderedDict()
        self.zone : Optional['GovernanceZone'] = None
        self.add(*args)

    def add(self, *args : 'InfraLocation') -> None:
        for loc in args:
            self.addLocation(loc)

    def addLocation(self, loc : 'InfraLocation'):
        if self.locations.get(loc.name) != None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc
        loc.setVendor(self)

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is InfrastructureVendor):
            return self.name == __value.name and self.locations == __value.locations and self.zone == __value.zone
        else:
            return False


class EncryptionSystem:
    """This describes"""
    def __init__(self) -> None:
        self.name : Optional[str] = None
        self.keyContainer : Optional['DataContainer'] = None
        self.hasThirdPartySuperUser : bool = False

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is EncryptionSystem):
            return self.name == __value.name and self.keyContainer == __value.keyContainer and self.hasThirdPartySuperUser == __value.hasThirdPartySuperUser
        else:
            return False

class DataContainer:
    def __init__(self, *args : 'InfraLocation') -> None:
        self.location : Optional[InfraLocation] = None
        self.name : Optional[str] = None
        self.serverSideEncryptionKeys : Optional[EncryptionSystem] = None
        self.clientSideEncryptionKeys : Optional[EncryptionSystem] = None
        self.isReadOnly : bool =  False
        self.add(*args)

    def add(self, *args : 'InfraLocation') -> None:
        for loc in args:
            if(self.location != None):
                raise ObjectAlreadyExistsException("Location already set")
            self.location = loc

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is DataContainer):
            return self.name == __value.name and self.location == __value.location and self.serverSideEncryptionKeys == __value.serverSideEncryptionKeys and self.clientSideEncryptionKeys == __value.clientSideEncryptionKeys and self.isReadOnly == __value.isReadOnly
        else:
            return False
        
    def getName(self) -> str:
        """Returns the name of the container"""
        if(self.name):
            return self.name
        else:
            raise Exception("Container name not set")

class Dataset(object):
    def __init__(self, name : str, *args : Union[Schema, StoragePolicy]) -> None:
        self.name : str = name
        self.store : Optional[Datastore] = None
        self.originalSchema : Optional[Schema] = None
        self.policies : dict[str, StoragePolicy] = OrderedDict()
        self.add(*args)

    def getZone(self) -> Optional['GovernanceZone']:
        if(self.store and self.store.team):
            return self.store.team.getZone()
        else:
            return None
    
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
        if(type(__value) is Dataset):
            return self.name == __value.name and self.originalSchema == __value.originalSchema and self.policies == __value.policies and self.refersToSameStoreShallowly(__value)
        else:
            return False

    def refersToSameStoreShallowly(self, o : 'Dataset') -> bool:
        """Checks if another store is the same as this one"""
        if(self.store is None and o.store is not None):
            return False
        if(self.store is not None and o.store is None):
            return False
        if(self.store is None and o.store is None):
            return True
        else:
            return self.store is not None and o.store is not None and self.store.name == o.store.name
    
    def validate(self):
        """Place holder to validate constraints on the dataset"""
        for policy in self.policies.values():
            if(policy.governanceZone != self.getZone()):
                raise StoragePolicyFromDifferentZone("Datasets must be governed by storage policies from its managing zone")
        
    
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
    def __init__(self, name : str) -> None:
        self.name : str = name

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is CaptureSourceInfo):
            return self.name == __value.name
        else:
            return False

class SimpleJDBCSourceInfo(CaptureSourceInfo):
    """Stores connection metadata for simple JDBC connection to source"""
    def __init__(self, name : str, jdbcURL : str) -> None:
        super().__init__(name)
        self.jdbcURL : str = jdbcURL

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is SimpleJDBCSourceInfo and self.jdbcURL == __value.jdbcURL
        
class CaptureType(Enum):
    SNAPSHOT = 0
    INCREMENTAL = 1

class CaptureMetaData(object):
    """Producers use these to describe HOW to snapshot and pull deltas from a data source in to
    data pipelines. The ingestion service interprets these to allow code free ingestion from
    supported sources and handle operation pipelines."""
    def __init__(self, *args : Union[Credential, CaptureSourceInfo, 'Datastore']) -> None:
        self.store : Optional['Datastore'] = None
        self.credential : Optional[Credential] = None
        self.captureSource : Optional[CaptureSourceInfo] = None
        self.add(*args)

    def add(self, *args : Union[Credential, CaptureSourceInfo, 'Datastore']) -> None:
        for arg in args:
            if(isinstance(arg, Credential)):
                c : Credential = arg
                self.credential = c
            elif(isinstance(arg, CaptureSourceInfo)):
                i : CaptureSourceInfo = arg
                self.captureSource = i
            else:
                d : Datastore = arg
                self.store = d
            
    def __eq__(self, __value: object) -> bool:
        if(type(__value) is CaptureMetaData):
            return self.store == __value.store and self.credential == __value.credential and self.captureSource == __value.captureSource
        else:
            return False

class SQLPullIngestion(CaptureMetaData):
    """This IMD describes how to pull a snapshot 'dump' from each dataset and then persist
    state variables which are used to next pull a delta per dataset and then persist the state
    again so that another delta can be pulled on the next pass and so on"""
    def __init__(self, *args : Union[Credential, CaptureSourceInfo, 'Datastore']) -> None:
        super().__init__(*args)
        self.variableNames : list[str] = []
        """The names of state variables produced by snapshot and delta sql strings"""
        self.snapshotSQL : dict[str, str] = OrderedDict()
        """A SQL string per dataset which pulls a per table snapshot"""
        self.deltaSQL : dict[str, str] = OrderedDict()
        """A SQL string per dataset which pulls all rows which changed since last time for a table"""

    def __eq__(self, __value: object) -> bool:
        return super().__eq__(__value) and type(__value) is SQLPullIngestion and self.variableNames == __value.variableNames and self.snapshotSQL == __value.snapshotSQL and self.deltaSQL == __value.deltaSQL
    


class Datastore(object):

    """This is a unit of ingestion for a group of datasets."""
    def __init__(self, name : str, *args : Union[Dataset, CaptureMetaData, DataContainer, 'Team']) -> None:
        self.name : str = name
        self.team : Optional[Team] = None
        self.datasets : dict[str, Dataset] = OrderedDict()
        self.imd : Optional[CaptureMetaData] = None
        self.container : Optional[DataContainer] = None
        self.add(*args)

    def add(self, *args : Union[Dataset, CaptureMetaData, DataContainer, 'Team']) -> None:
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
            elif(isinstance(arg, Team)):
                t : Team = arg
                self.team = t

    def addDataset(self, item : Dataset) -> None:
        """Add a named dataset"""
        if self.datasets.get(item.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Dataset {item.name}")
        self.datasets[item.name] = item
        item.store = self
    def setTeam(self, t : 'Team'):
        """Set the data stores owning team"""
        if(self.team is not None):
            raise Exception("Team already set")
        self.team = t

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is Datastore):
            return (
                self.name == __value.name
                and self.datasets == __value.datasets
                and self.imd == __value.imd
                and self.container == __value.container
                and self.refersToSameTeamShallowly(__value)
            )
        else:
            return False
        
    def refersToSameTeamShallowly(self, o : 'Datastore') -> bool:
        """Avoiding recursion, check if another store uses the same team as this one"""
        if(self.team is None and o.team is not None):
            return False
        if(self.team is not None and o.team is None):
            return False
        if(self.team is None and o.team is None):
            return True
        if self.team is not None and o.team is not None:
            return self.team == o.team
        return False
        

class ValidationProblem:
    def __init__(self) -> None:
        self.object : object = None
        """The original object that is in use"""
        self.description : Optional[str] = None
        """A description of what the issue is"""


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
class Ecosystem:
    def __init__(self, name : str, repo : Repository, *args : 'GovernanceZone') -> None:
        self.name : str = name
        self.owningRepo : Repository = repo
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
        z.eco  = self

    def addTeamDeclaration(self, td : 'TeamDeclaration'):
        if(self.teamDeclarationCache.get(td.name) != None):
            if(self.teamDeclarationCache.get(td.name) != None):
                raise ObjectAlreadyExistsException(f"Duplicate TeamDeclaration {td.name}")
            self.teamDeclarationCache[td.name] = td

    def addWorkspace(self, work : 'Workspace'):
        """This adds a workspace to the eco cache and flags duplicates"""
        if(self.workSpaceCache.get(work.name) != None):
            raise ObjectAlreadyExistsException(f"Duplicate workspace {work.name}")
        self.workSpaceCache[work.name] = work

    def addDatastore(self, store : 'Datastore'):
        """This adds a store to the eco cache and flags duplicates"""
        if(self.datastoreCache.get(store.name) != None):
            raise ObjectAlreadyExistsException(f"Duplicate data store {store.name}")
        self.datastoreCache[store.name] = store

    def getDatastoreOrThrow(self, store : str):
        s : Optional[Datastore] = self.datastoreCache.get(store)
        if(s):
            return s
        else:
            raise DatastoreDoesntExistException(f"Unknown datastore {store}")

    def getDatasetOrThrow(self, store : str, set : str):
        s : Datastore = self.getDatastoreOrThrow(store)
        dataset = s.datasets.get(set)
        if(dataset == None):
            raise DatasetDoesntExistException(f"Dataset doesn't exist {store}:{set}")
        return dataset

    def validateDatastore(self, gz : 'GovernanceZone', t : 'TeamDeclaration', s : Datastore) -> Sequence[ValidationProblem]:
        problems : List[ValidationProblem] = []
        l = self.validateAndHydrateCaches()
        problems.extend(l)
        l = self.validateTeamDeclaration(gz, t)
        problems.extend(l)
        l = self.validateGoveranceZone(gz)
        problems.extend(l)
        return problems

    def validateTeamDeclaration(self, gz: 'GovernanceZone', t: 'TeamDeclaration') -> Sequence['ValidationProblem']:
        """This validates a single team declaration and populates the datastore cache with that team's stores"""
        problem_list : List[ValidationProblem] = []
        for s in t.getTeam().dataStores.values():
            if self.datastoreCache.get(s.name) is not None:
                p = ValidationProblem()
                p.description = f"Duplicate Datastore {s.name}"
                p.object = s
                problem_list.append(p)
            else:
                self.datastoreCache[s.name] = s
                self.validateDatastore(gz, t, s)
        for w in t.getTeam().workspaces.values():
            if self.workSpaceCache.get(w.name) is not None:
                p = ValidationProblem()
                p.description = f"Duplicate Workspace {w.name}"
                p.object = t
                problem_list.append(p)
            else:
                self.workSpaceCache[w.name] = w
        return problem_list

    def validateGoveranceZone(self, gz : 'GovernanceZone') -> Sequence[ValidationProblem]:
        """This validates a GovernanceZone and populates the teamcache with the zones teams"""
        list : List[ValidationProblem] = []
        for t in gz.teams.values():
            if(self.teamDeclarationCache.get(t.name) != None):
                p : ValidationProblem = ValidationProblem()
                p.description = f"Duplicate TeamDeclaration {t.name}"
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
                        self.getDatasetOrThrow(sink.storeName, sink.datasetName)
                    except Exception as e:
                        p : ValidationProblem = ValidationProblem()
                        p.object = sink
                        p.description = str(e)
                        list.append(p)
        return list


    def checkChangesAreAuthorized(self, e : 'Ecosystem', changeSource : Repository) -> Sequence[ValidationProblem]:
        """This checks that the changes made in e relative to this ecosystem are authorized from the specified change source"""
        list : List[ValidationProblem] = []
        return list

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is Ecosystem):
            return self.name == __value.name and self.governanceZones == __value.governanceZones
        else:
            return False
        

class Team:
    """This is the authoritive definition of a team within a goverance zone. All teams must have
    a corresponding TeamDeclaration in the owning GovernanceZone"""
    def __init__(self, td : 'TeamDeclaration', *args : Union[Datastore, 'Workspace', Repository]) -> None:
        self.td : 'TeamDeclaration' = td
        self.workspaces : dict[str, Workspace] = OrderedDict()
        self.dataStores : dict[str, Datastore] = OrderedDict()
        self.add(*args)

    def add(self, *args : Union[Datastore, 'Workspace', Repository]) -> None:
        """Adds a workspace, datastore or gitrepository to the team"""
        for arg in args:
            if(type(arg) is Datastore):
                s : Datastore = arg
                self.addStore(s)
            elif(type(arg) is Workspace):
                w : Workspace = arg
                self.addWorkspace(w)
            elif(isinstance(arg, Repository)):
                r : Repository = arg
                self.masterRepo = r

    def getName(self) -> str:
        """Returns the name of the team"""
        return self.td.name
    
    def addStore(self, store : Datastore):
        """Adds a datastore to the team checking for duplicates"""
        if self.dataStores.get(store.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Datastore {store.name}")
        self.dataStores[store.name] = store
        store.setTeam(self)
    def addWorkspace(self, w : 'Workspace'):
        if self.workspaces.get(w.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Workspace {w.name}")
        self.workspaces[w.name] = w
        w.setTeam(self)

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is Team):
            return self.td.name == __value.td.name and self.workspaces == __value.workspaces and self.dataStores == __value.dataStores
        else:
            return False
        
    def getZone(self) -> Optional['GovernanceZone']:
        """Returns the governance zone for this team"""
        if(self.td and self.td.gZone):
            return self.td.gZone

class TeamDeclaration:
    """All teams must be declared at the GovernanceZone level using one of these objects. The Team objects are initialized in a secondary step"""
    def __init__(self, name : str, *args : Repository) -> None:
        self.name : str = name
        self.gZone : Optional['GovernanceZone'] = None
        self.masterRepo : Optional[Repository] = None
        """Changes to the Team object can only be done using committed changes in the specified repository"""
        """Files for this team must be in this folder in the master repo"""
        self.team : Optional[Team] = None
        self.add(*args)

    def getTeam(self) -> Team:
        """Return a singleton Team object managed by this declaration"""
        if(not self.team):
            self.team = Team(self) 
        return self.team

    def add(self, *args : Repository) -> None:
        for arg in args:
            self.masterRepo = arg
            
    def __eq__(self, __value: object) -> bool:
        if(type(__value) is TeamDeclaration):
            return (
                self.name == __value.name and
                self.refersToSameZoneShallowly(__value) and
                self.masterRepo == __value.masterRepo
            )
        else:
            return False

    def refersToSameZoneShallowly(self, o : 'TeamDeclaration') -> bool:
        """Avoiding recursion, check if another team uses the same zone as this one"""
        if(self.gZone is None and o.gZone is not None):
            return False
        if(self.gZone is not None and o.gZone is None):
            return False
        if(self.gZone is None and o.gZone is None):
            return True
        if(self.gZone and o.gZone):
            return self.gZone.name == o.gZone.name
        else:
            return False

class GovernanceZone:

    """This declares the existence of a specific GovernanceZone and defines the teams it manages, the storage policies
    and which repos can be used to pull changes for various metadata"""
    def __init__(self, name : str, *args : Union[InfrastructureVendor, StoragePolicy, TeamDeclaration, 'DataPlatform']) -> None:
        self.name : str = name
        self.platforms : dict[str, 'DataPlatform'] = OrderedDict[str, 'DataPlatform']()
        self.teams : dict[str, TeamDeclaration] = OrderedDict[str, TeamDeclaration]()
        self.vendors : dict[str, InfrastructureVendor] = OrderedDict[str, InfrastructureVendor]()
        self.storagePolicies : dict[str, StoragePolicy] = OrderedDict[str, StoragePolicy]()
        self.eco : Optional[Ecosystem] = None
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
            elif(type(arg) is DataPlatform):
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
        p.governanceZone = self   

    def addTeam(self, t : TeamDeclaration):
        if self.teams.get(t.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Team {t.name}")
        self.teams[t.name] = t
        t.gZone = self

    def addVendor(self, iv : InfrastructureVendor):
        if self.vendors.get(iv.name) != None:
            raise ObjectAlreadyExistsException(f"Duplicate Vendor {iv.name}")
        self.vendors[iv.name] = iv
        iv.zone = self

    def getTeam(self, name : str) -> Team:
        """This retrieves the team object for a declared team. Additional team elements can then be added to the team"""
        td : Optional[TeamDeclaration] = self.teams.get(name)
        if(td == None):
            raise ObjectDoesntExistException(f"Team {name} doesn't exist")
        return td.getTeam()

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is GovernanceZone):
            return self.name == __value.name and self.platforms == __value.platforms and self.teams == __value.teams and self.vendors == __value.vendors and self.storagePolicies == __value.storagePolicies and self.eco == __value.eco
        else:
            return False
    
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
    def __init__(self, name : str) -> None:
        self.name : str = name

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is DataPlatform):
            return self.name == __value.name
        else:
            return False

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
        if(type(__value) is ConsumerRetentionRequirements):
            return self.policy == __value.policy and self.latency == __value.latency and self.minRetentionTime == __value.minRetentionTime and self.regulator == __value.regulator
        else:
            return False

class WorkspacePlatformConfig(object):
    """This allows a Workspace to specify per pipeline hints for behavior, i.e.
    allowed latency and so on"""
    def __init__(self, p : DataPlatform, hist : ConsumerRetentionRequirements) -> None:
        self.platform : DataPlatform = p
        self.retention : ConsumerRetentionRequirements = hist

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is WorkspacePlatformConfig):
            return self.platform == __value.platform and self.retention == __value.retention
        else:
            return False

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
    def __init__(self, name : str, *args : Any) -> None:
        self.name : str = name
        self.platformMD : Optional[WorkspacePlatformConfig] = None
        self.datasets : dict[str, DatasetSink] = OrderedDict[str, DatasetSink]()
        for arg in args:
            if(type(arg) is DatasetSink):
                sink : DatasetSink = arg
                self.datasets[sink.key] = sink
            elif(type(arg) is WorkspacePlatformConfig):
                if self.platformMD == None:
                    self.platformMD = arg
                else:
                    raise ObjectAlreadyExistsException("Platform already specified")
            else:
                raise UnknownArgumentException(f"Unknown argument {type(arg)}")
            
    def __eq__(self, __value: object) -> bool:
        if(type(__value) is DatasetGroup):
            return self.name == __value.name and self.datasets == __value.datasets and self.platformMD == __value.platformMD
        else:
            return False
        


class Workspace(object):
    """A collection of datasets used by a consumer for a specific use case. This consists of one or more groups of datasets with each set using the correct pipeline spec.
    Specific datasets can be present in multiple groups. They will be named differently in each group"""
    def __init__(self, name : str, *args : Any) -> None:
        self.name : str = name
        self.dsgs : dict[str, DatasetGroup] = OrderedDict[str, DatasetGroup]()
        self.asset : Optional[Asset] = None
        self.team : Optional[Team] = None
        for arg in args:
            if(type(arg) is DatasetGroup):
                dsg : DatasetGroup = arg
                self.dsgs[dsg.name] = dsg
            elif(isinstance(arg, Asset)):
                a : Asset = arg
                self.asset = a
            else:
                raise UnknownArgumentException(f"Unknown argument {type(arg)}")

    def setTeam(self, t : Team):
        self.team = t

    def __eq__(self, __value: object) -> bool:
        if(type(__value) is Workspace):
            return self.name == __value.name and self.dsgs == __value.dsgs and self.asset == __value.asset and self.refersToSameTeamShallowly(__value)
        else:
            return False
    
    def refersToSameTeamShallowly(self, o : 'Workspace') -> bool:
        """Avoiding recursion, check if another store uses same team as this one"""
        if(self.team == None and o.team != None):
            return False
        if(self.team and o.team == None):
            return False
        if(self.team == None and o.team == None):
            return True
        if(self.team and o.team):
            return self.team.getName() == o.team.getName()
        else:
            return False

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

