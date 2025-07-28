"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from typing import Any, Optional, Union, OrderedDict
from datasurface.md.lint import ValidationTree, AttributeNotSet, ValidationProblem, ProblemSeverity
from datasurface.md.json import JSONable
from datasurface.md.documentation import Documentation, Documentable
from datasurface.md.keys import InfraLocationKey, InfrastructureVendorKey, EcosystemKey
from datasurface.md.policy import Literal
from enum import Enum


class UnknownLocationProblem(ValidationProblem):
    def __init__(self, locStr: str, severity: ProblemSeverity) -> None:
        super().__init__(f"Unknown location {locStr}", severity)

    def __hash__(self) -> int:
        return hash(self.description)


class UnknownVendorProblem(ValidationProblem):
    def __init__(self, vendor: str, severity: ProblemSeverity) -> None:
        super().__init__(f"Unknown vendor {vendor}", severity)

    def __hash__(self) -> int:
        return hash(self.description)


class InfrastructureLocation(Documentable, JSONable):
    """This is a location within a vendors physical location hierarchy. This object
    is only fully initialized after construction when either the setParentLocation or
    setVendor methods are called. This is because the vendor is required to set the parent"""

    def __init__(self,
                 name: str,
                 *args: Union[Documentation, 'InfrastructureLocation'],
                 locations: Optional[list['InfrastructureLocation']] = None,
                 documentation: Optional[Documentation] = None) -> None:
        Documentable.__init__(self, documentation)
        JSONable.__init__(self)

        self.name: str = name
        self.key: Optional[InfraLocationKey] = None

        self.locations: dict[str, 'InfrastructureLocation'] = OrderedDict()
        """These are the 'child' locations under this location. A state location would have city children for example"""
        """This specifies the parent location of this location. State is parent on city and so on"""

        # Handle backward compatibility: if *args are provided, parse them the old way
        if args:
            # Legacy mode: parse *args (slower but compatible)
            parsed_locations: list[InfrastructureLocation] = list(locations) if locations else []
            parsed_documentation: Optional[Documentation] = documentation

            for arg in args:
                if isinstance(arg, InfrastructureLocation):
                    parsed_locations.append(arg)
                else:
                    # Remaining argument should be Documentation
                    parsed_documentation = arg

            # Use parsed values
            if parsed_locations:
                for loc in parsed_locations:
                    self.addLocation(loc)

            if parsed_documentation:
                self.documentation = parsed_documentation
        else:
            # New mode: use named parameters directly (faster!)
            if locations:
                for loc in locations:
                    self.addLocation(loc)

            if documentation:
                self.documentation = documentation
        self.resetKey()

    @classmethod
    def create_legacy(cls, name: str, *args: Union[Documentation, 'InfrastructureLocation']) -> 'InfrastructureLocation':
        """Legacy factory method for backward compatibility with old *args pattern.
        Use this temporarily during migration, then switch to named parameters for better performance."""
        locations: list[InfrastructureLocation] = []
        documentation: Optional[Documentation] = None

        for arg in args:
            if isinstance(arg, InfrastructureLocation):
                locations.append(arg)
            else:
                # Remaining argument should be Documentation
                documentation = arg

        return cls(
            name=name,
            locations=locations if locations else None,
            documentation=documentation
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "locations": {k: k.to_json() for k in self.locations.values()},
        }

    def __str__(self) -> str:
        return f"InfrastructureLocation({self.name})"

    def __hash__(self) -> int:
        return hash(self.name)

    def lint(self, tree: ValidationTree):
        """This checks if the vendor is valid for the specified ecosystem, governance zone and team"""
        if (self.key is None):
            tree.addRaw(AttributeNotSet("Location"))
        if (self.documentation):
            dTree: ValidationTree = tree.addSubTree(self.documentation)
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
            if isinstance(loc, InfrastructureLocation):
                self.addLocation(loc)
            else:
                self.documentation = loc
        self.resetKey()

    def resetKey(self) -> None:
        if (self.key):
            for loc in self.locations.values():
                loc.setParentLocation(self.key)

    def addLocation(self, loc: 'InfrastructureLocation'):
        if self.locations.get(loc.name) is not None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc

    def __eq__(self, other: object) -> bool:
        if super().__eq__(other) and isinstance(other, InfrastructureLocation):
            return self.name == other.name and self.key == other.key and self.locations == other.locations
        return False

    def getEveryChildLocation(self) -> set['InfrastructureLocation']:
        """This returns every child location of this location"""
        rc: set[InfrastructureLocation] = set()
        for loc in self.locations.values():
            rc.add(loc)
            rc = rc.union(loc.getEveryChildLocation())
        return rc

    def containsLocation(self, child: 'InfrastructureLocation') -> bool:
        """This true if this or a child matches the passed location"""
        if (self == child):
            return True
        for loc in self.locations.values():
            if loc.containsLocation(child):
                return True
        return False

    def getLocationOrThrow(self, locationName: str) -> 'InfrastructureLocation':
        """Returns the location with the specified name or throws an exception"""
        loc: Optional[InfrastructureLocation] = self.locations.get(locationName)
        assert loc is not None
        return loc

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
    PRIVATE = 10  # Onsite or private cloud


class InfrastructureVendor(Documentable, JSONable):
    """This is a vendor which supplies infrastructure for storage and compute. It could be an internal supplier within an
    enterprise or an external cloud provider"""
    def __init__(self,
                 name: str,
                 *args: Union[InfrastructureLocation, Documentation, CloudVendor],
                 locations: Optional[list[InfrastructureLocation]] = None,
                 documentation: Optional[Documentation] = None,
                 cloud_vendor: Optional[CloudVendor] = None) -> None:
        Documentable.__init__(self, documentation)
        JSONable.__init__(self)
        self.name: str = name
        self.key: Optional[InfrastructureVendorKey] = None
        self.locations: dict[str, 'InfrastructureLocation'] = OrderedDict()
        self.hardCloudVendor: Optional[CloudVendor] = None

        # Handle backward compatibility: if *args are provided, parse them the old way
        if args:
            # Legacy mode: parse *args (slower but compatible)
            parsed_locations: list[InfrastructureLocation] = list(locations) if locations else []
            parsed_documentation: Optional[Documentation] = documentation
            parsed_cloud_vendor: Optional[CloudVendor] = cloud_vendor

            for arg in args:
                if isinstance(arg, InfrastructureLocation):
                    parsed_locations.append(arg)
                elif isinstance(arg, CloudVendor):
                    parsed_cloud_vendor = arg
                else:
                    # Remaining argument should be Documentation
                    parsed_documentation = arg

            # Use parsed values
            if parsed_locations:
                for loc in parsed_locations:
                    self.addLocation(loc)

            if parsed_cloud_vendor:
                self.hardCloudVendor = parsed_cloud_vendor

            if parsed_documentation:
                self.documentation = parsed_documentation
        else:
            # New mode: use named parameters directly (faster!)
            if locations:
                for loc in locations:
                    self.addLocation(loc)

            if cloud_vendor:
                self.hardCloudVendor = cloud_vendor

            if documentation:
                self.documentation = documentation
        self.resetKey()

    @classmethod
    def create_legacy(cls, name: str, *args: Union[InfrastructureLocation, Documentation, CloudVendor]) -> 'InfrastructureVendor':
        """Legacy factory method for backward compatibility with old *args pattern.
        Use this temporarily during migration, then switch to named parameters for better performance."""
        locations: list[InfrastructureLocation] = []
        documentation: Optional[Documentation] = None
        cloud_vendor: Optional[CloudVendor] = None

        for arg in args:
            if isinstance(arg, InfrastructureLocation):
                locations.append(arg)
            elif isinstance(arg, CloudVendor):
                cloud_vendor = arg
            else:
                # Remaining argument should be Documentation
                documentation = arg

        return cls(
            name=name,
            locations=locations if locations else None,
            documentation=documentation,
            cloud_vendor=cloud_vendor
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "locations": {k: k.to_json() for k in self.locations.values()},
            "hardCloudVendor": self.hardCloudVendor.name if self.hardCloudVendor else None,
        }

    def __hash__(self) -> int:
        return hash(self.name)

    def setEcosystem(self, eco: 'EcosystemKey') -> None:
        self.key = InfrastructureVendorKey(eco, self.name)

        self.add()

    def add(self, *args: Union['InfrastructureLocation', Documentation, CloudVendor]) -> None:
        """We always expect the instance variables to be set with this method whether the constructor or not. The topLocationKey
        is set here after changing the instance variables"""
        for loc in args:
            if isinstance(loc, InfrastructureLocation):
                self.addLocation(loc)
            elif isinstance(loc, CloudVendor):
                self.hardCloudVendor = loc
            else:
                self.documentation = loc
        self.resetKey()

    def resetKey(self) -> None:
        if (self.key):
            topLocationKey: InfraLocationKey = InfraLocationKey(self.key, [])
            for loc in self.locations.values():
                loc.setParentLocation(topLocationKey)

    def addLocation(self, loc: 'InfrastructureLocation'):
        if self.locations.get(loc.name) is not None:
            raise Exception(f"Duplicate Location {loc.name}")
        self.locations[loc.name] = loc

    def __eq__(self, other: object) -> bool:
        if super().__eq__(other) and isinstance(other, InfrastructureVendor):
            return self.name == other.name and self.key == other.key and self.locations == other.locations and \
                self.hardCloudVendor == other.hardCloudVendor
        else:
            return False

    def getLocationOrThrow(self, locationName: str) -> 'InfrastructureLocation':
        """Returns the location with the specified name or throws an exception"""
        loc: Optional[InfrastructureLocation] = self.locations.get(locationName)
        assert loc is not None
        return loc

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
            lTree: ValidationTree = tree.addSubTree(loc)
            loc.lint(lTree)

    def __str__(self) -> str:
        return f"InfrastructureVendor({self.name}, {self.hardCloudVendor})"


def convertCloudVendorItems(items: Optional[set[CloudVendor]]) -> Optional[set[Literal[CloudVendor]]]:
    if items is None:
        return None
    else:
        return {Literal(item) for item in items}
