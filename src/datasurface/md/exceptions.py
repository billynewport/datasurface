"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


class DataSurfaceException(Exception):
    """Base exception for all DataSurface errors"""
    pass


class UnknownArgumentException(DataSurfaceException):
    """This means an object was passed to a constructor using varargs that isn't expected"""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class NameMustBeANSISQLIdentifierException(DataSurfaceException):
    """This means the name must be an ANSI SQL identifier"""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class ObjectAlreadyExistsException(DataSurfaceException):
    """This means there is already an object with the same name"""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class AttributeAlreadySetException(DataSurfaceException):
    """This means the attribute has already been set"""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class ObjectDoesntExistException(DataSurfaceException):
    """This means the named object doesn't exist"""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class DatasetDoesntExistException(ObjectDoesntExistException):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class DatastoreDoesntExistException(ObjectDoesntExistException):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class WorkspaceDoesntExistException(ObjectDoesntExistException):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class StoragePolicyFromDifferentZone(DataSurfaceException):
    """This means the storage policy is from a different governance zone"""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
