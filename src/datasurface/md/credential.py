"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from enum import Enum
from datasurface.md.lint import UserDSLObject, ValidationTree, ProblemSeverity
from datasurface.md.json import JSONable
from datasurface.md.keys import LocationKey
from abc import abstractmethod
from typing import Any


class CredentialType(Enum):
    """This describes the type of credential"""
    API_KEY_PAIR = 0  # AWS S3 type credential, an access id and a secret key
    USER_PASSWORD = 1  # Username and password
    CLIENT_CERT_WITH_KEY = 2  # Public and private key pair for mTLS or similar
    CA_CERT_BUNDLE = 3  # Bundle of KEYs for verifying server keys
    API_TOKEN = 4  # API token for a service


class Credential(UserDSLObject, JSONable):
    """These allow a client to connect to a service/server"""
    def __init__(self, name: str, credentialType: CredentialType) -> None:
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.name: str = name
        self.credentialType: CredentialType = credentialType

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "name": self.name,
            "credentialType": self.credentialType.name,
        }

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, Credential)):
            return self.credentialType == other.credentialType and self.name == other.name
        else:
            return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    @abstractmethod
    def lint(self, tree: ValidationTree) -> None:
        pass


class FileSecretCredential(Credential):
    """This allows a secret to be read from the local filesystem. Usually the secret is
    placed in the file using an external service such as Docker secrets etc. The secret should be in the
    form of 2 lines, first line is user name, second line is password"""
    def __init__(self, name: str, type: CredentialType, filePath: str) -> None:
        super().__init__(name, type)
        self.secretFilePath: str = filePath

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"secretFilePath": self.secretFilePath})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is FileSecretCredential and self.secretFilePath == other.secretFilePath

    def lint(self, tree: ValidationTree) -> None:
        """This checks if the source is valid for the specified ecosystem, governance zone and team"""
        # TODO This needs to be better
        if (self.secretFilePath == ""):
            tree.addProblem("Secret file path is empty")

    def __str__(self) -> str:
        return f"FileSecretCredential({self.secretFilePath})"


class CredentialStore(UserDSLObject, JSONable):
    """This is a credential store which stores credential data in a set of infra locations"""
    def __init__(self, name: str, locs: set['LocationKey']) -> None:
        UserDSLObject.__init__(self)
        JSONable.__init__(self)
        self.name: str = name
        self.locs: set[LocationKey] = locs

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "name": self.name,
            "locs": {k: k.to_json() for k in self.locs},
        }

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and \
            isinstance(other, CredentialStore) and \
            self.name == other.name and \
            self.locs == other.locs

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def lint(self, tree: ValidationTree) -> None:
        if (self.name == ""):
            tree.addProblem("Name is empty")
        for loc in self.locs:
            loc.lint(tree.addSubTree(loc))

    @abstractmethod
    def checkCredentialIsAvailable(self, cred: Credential, tree: ValidationTree) -> None:
        """This is used to check if a Credential is supported by this store."""
        pass

    @abstractmethod
    def getAsUserPassword(self, cred: Credential) -> tuple[str, str]:
        """This returns the username and password for the credential"""
        pass

    @abstractmethod
    def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
        """This fetches the credential and returns a tuple with the public and private key
        paths and an environment variable name which contains the private key password"""
        pass

    @abstractmethod
    def getAsToken(self, cred: Credential) -> str:
        """This fetches the credential and returns a token. This is used for API tokens."""
        pass

    @abstractmethod
    def lintCredential(self, cred: Credential, tree: ValidationTree) -> None:
        """This is used to lint a credential. This is used to check if the credential is available and compatible with the store."""
        pass


class LocalFileCredentialStore(CredentialStore):
    """This is a local file credential store. It represents a folder on the local file system where certificates are stored in files. This could be used with
    docker secrets or similar"""
    def __init__(self, name: str, locs: set['LocationKey'], folder: str) -> None:
        super().__init__(name, locs)
        self.credentials: dict[str, Credential] = dict()
        self.folder: str = folder

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"folder": self.folder})
        rc.update({"credentials": {k: v.to_json() for k, v in self.credentials.items()}})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, LocalFileCredentialStore) and self.folder == other.folder

    def lint(self, tree: ValidationTree) -> None:
        super().lint(tree)

    def getAsUserPassword(self, cred: Credential) -> tuple[str, str]:
        """This will read the file holding the secret and return the first and second lines
        as a tuple to the caller."""
        if cred.credentialType != CredentialType.USER_PASSWORD:
            raise RuntimeError(f"Unsupported credential type: {cred.credentialType.name}")
        if isinstance(cred, FileSecretCredential):
            file_path = f"{self.folder}/{cred.secretFilePath}"
            try:
                with open(file_path, 'r') as file:
                    lines = file.readlines()
                    if len(lines) < 2:
                        raise ValueError("Credential file does not contain enough lines.")
                    username = lines[0].strip()
                    password = lines[1].strip()
                    return username, password
            except FileNotFoundError:
                raise FileNotFoundError(f"Credential file {file_path} not found.")
            except Exception as e:
                raise RuntimeError(f"An error occurred while reading the credential file: {e}")
        elif isinstance(cred, ClearTextCredential):
            return cred.username, cred.password
        else:
            raise RuntimeError(f"Unsupported credential type: {type(cred)}")

    def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
        """This assumes there are 3 secrets on the local filesystem. The public key
        the private key and the private key password. A naming convention is assumed for the 3 files
        using the credential name as the prefix with _pub, _prv, _pwd as the postfixes. The password
        file is not mapped into the container. Instead an environment variable with the credential
        name is provided instead."""
        if cred.credentialType != CredentialType.CLIENT_CERT_WITH_KEY:
            raise RuntimeError(f"Unsupported credential type: {cred.credentialType.name}")
        if isinstance(cred, FileSecretCredential):
            file_path_root: str = f"{self.folder}/{cred.secretFilePath}_"
            pub_path: str = f"{file_path_root}pub"
            prv_path: str = f"{file_path_root}prv"
            env_var: str = f"CERT_{cred.secretFilePath}_PWD"
            return pub_path, prv_path, env_var
        else:
            raise RuntimeError(f"Only FileSecretCredentials are supported: {cred}")

    def getAsToken(self, cred: Credential) -> str:
        """This fetches the credential and returns a token. This is used for API tokens."""
        if cred.credentialType != CredentialType.API_TOKEN:
            raise RuntimeError(f"Unsupported credential type: {cred.credentialType.name}")
            file_path: str = f"{self.folder}/{cred.name}"
            try:
                with open(file_path, 'r') as file:
                    return file.read().strip()
            except FileNotFoundError:
                raise FileNotFoundError(f"Credential file {file_path} not found.")
        else:
            raise RuntimeError(f"Only {CredentialType.API_TOKEN} are supported: {cred}")

    def checkCredentialIsAvailable(self, cred: Credential, tree: ValidationTree) -> None:
        return super().checkCredentialIsAvailable(cred, tree)


class ClearTextCredential(Credential):
    """This is implemented for testing but should never be used in production. All
    credentials should be stored and retrieved using secrets Credential objects also
    provided."""
    def __init__(self, name: str, username: str, password: str) -> None:
        super().__init__(name, CredentialType.USER_PASSWORD)
        self.username: str = username
        self.password: str = password

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"username": self.username})
        rc.update({"password": self.password})
        return rc

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and type(other) is ClearTextCredential and self.username == other.username and self.password == other.password

    def lint(self, tree: ValidationTree) -> None:
        super().lint(tree)
        tree.addProblem("ClearText credential found", ProblemSeverity.WARNING)

    def __str__(self) -> str:
        return f"ClearTextCredential({self.username})"
