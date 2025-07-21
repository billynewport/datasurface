
"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from enum import Enum
from datasurface.md.lint import UserDSLObject, ValidationTree, ProblemSeverity, ValidationProblem
from datasurface.md.keys import LocationKey
from abc import abstractmethod
from typing import Any, Optional
import re
import os


class CredentialType(Enum):
    """This describes the type of credential"""
    API_KEY_PAIR = 0  # AWS S3 type credential, an access id and a secret key
    USER_PASSWORD = 1  # Username and password
    CLIENT_CERT_WITH_KEY = 2  # Public and private key pair for mTLS or similar
    CA_CERT_BUNDLE = 3  # Bundle of KEYs for verifying server keys
    API_TOKEN = 4  # API token for a service


class Credential(UserDSLObject):
    """These allow a client to connect to a service/server"""
    def __init__(self, name: str, credentialType: CredentialType) -> None:
        UserDSLObject.__init__(self)
        self.name: str = name
        self.credentialType: CredentialType = credentialType

    def __eq__(self, other: object) -> bool:
        if (isinstance(other, Credential)):
            return self.credentialType == other.credentialType and self.name == other.name
        else:
            return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "name": self.name,
            "credentialType": self.credentialType.name,
        }

    def __hash__(self) -> int:
        return hash((self.name, self.credentialType))


class CredentialLookupException(Exception):
    """This is raised when a credential is not found in the credential store"""
    def __init__(self, cred: Credential, issue: str):
        super().__init__(f"Credential {cred.name} is not found: {issue}")
        self.cred: Credential = cred
        self.issue: str = issue


class CredentialNotAvailableException(CredentialLookupException):
    """This is raised when a credential is not available in the credential store"""
    def __init__(self, cred: Credential, issue: str):
        super().__init__(cred, issue)


class CredentialWrongTypeException(CredentialLookupException):
    """This is raised when a credential is not of the expected type"""
    def __init__(self, cred: Credential, expectedType: CredentialType):
        super().__init__(cred, f"Credential {cred.name} is not of type {expectedType.name}")


class CredentialTypeNotSupportedProblem(ValidationProblem):
    """This indicates a credential type is not supported"""
    def __init__(self, cred: Credential, supportedTypes: list[CredentialType]) -> None:
        super().__init__(f"Credential {cred.name} is not of type {supportedTypes}", ProblemSeverity.ERROR)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, CredentialTypeNotSupportedProblem)


class CredentialNotAvailableProblem(ValidationProblem):
    """This is raised when a credential is not available in the credential store"""
    def __init__(self, cred: Credential, issue: str):
        super().__init__(f"Credential {cred.name} is not available: {issue}", ProblemSeverity.ERROR)

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, CredentialNotAvailableProblem)


class CredentialStore(UserDSLObject):
    """This is a credential store which stores credential data in a set of infra locations"""
    def __init__(self, name: str, locs: set['LocationKey']) -> None:
        UserDSLObject.__init__(self)
        self.name: str = name
        self.locs: set[LocationKey] = locs

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

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "name": self.name,
            "locs": [k.to_json() for k in self.locs]
        }

    @abstractmethod
    def checkCredentialIsAvailable(self, cred: Credential, tree: ValidationTree) -> None:
        """This is used to check if a Credential is supported by this store."""
        pass

    @abstractmethod
    def getAsUserPassword(self, cred: Credential) -> tuple[str, str]:
        """This returns the username and password for the credential. This can raise a CredentialNotAvailable exception if the credential is not available."""
        pass

    @abstractmethod
    def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
        """This fetches the credential and returns a tuple with the public and private key
        strings and the private key password. This can raise a CredentialNotAvailable exception if the credential is not available."""
        pass

    @abstractmethod
    def getAsToken(self, cred: Credential) -> str:
        """This fetches the credential and returns a token. This is used for API tokens. This can raise a CredentialNotAvailable exception if the
        credential is not available."""
        pass

    @abstractmethod
    def lintCredential(self, cred: Credential, tree: ValidationTree) -> None:
        """This is used to lint a credential. This is used to check if the credential is available and compatible with the store."""
        pass


class NoopCredentialStore(CredentialStore):
    """This is a no-op credential store. It's intent is to specify that the data flows are already realized and externally managed
    by existing systems. However, DataSurface will still track the data flows and manage governance for the data."""
    def __init__(self) -> None:
        super().__init__("NoopCredentialStore", set())

    def checkCredentialIsAvailable(self, cred: Credential, tree: ValidationTree) -> None:
        raise NotImplementedError("NoopCredentialStore does not support checking credentials")

    def getAsUserPassword(self, cred: Credential) -> tuple[str, str]:
        raise NotImplementedError("NoopCredentialStore does not support getting credentials")

    def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
        raise NotImplementedError("NoopCredentialStore does not support getting credentials")

    def getAsToken(self, cred: Credential) -> str:
        raise NotImplementedError("NoopCredentialStore does not support getting credentials")

    def lintCredential(self, cred: Credential, tree: ValidationTree) -> None:
        pass

    def __str__(self) -> str:
        return "NoopCredentialStore()"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and isinstance(other, NoopCredentialStore)

    def to_json(self) -> dict[str, Any]:
        return {
            "_type": self.__class__.__name__,
            "name": self.name,
            "locs": [k.to_json() for k in self.locs]
        }


class LocalFileCredentialStore(CredentialStore):
    """This is a local file credential store. It represents a folder on the local file system where certificates are stored in files. This could be used with
    docker secrets or similar"""
    def __init__(self, name: str, locs: set['LocationKey'], folder: str) -> None:
        super().__init__(name, locs)
        self.credentials: dict[str, Credential] = dict()
        self.folder: str = folder

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
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
            raise CredentialWrongTypeException(cred, CredentialType.USER_PASSWORD)
        file_path = f"{self.folder}/{cred.name}"
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()
                if len(lines) < 2:
                    raise CredentialNotAvailableException(cred, "Credential file does not contain enough lines.")
                username = lines[0].strip()
                password = lines[1].strip()
                return username, password
        except FileNotFoundError:
            raise CredentialNotAvailableException(cred, f"Credential file {file_path} not found.")
        except Exception as e:
            raise RuntimeError(f"An error occurred while reading the credential file: {e}")

    def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
        """This assumes there are 3 secrets on the local filesystem. The public key
        the private key and the private key password. A naming convention is assumed for the 3 files
        using the credential name as the prefix with _pub, _prv, _pwd as the postfixes. The password
        file is not mapped into the container. Instead an environment variable with the credential
        name is provided instead."""
        if cred.credentialType != CredentialType.CLIENT_CERT_WITH_KEY:
            raise CredentialWrongTypeException(cred, CredentialType.CLIENT_CERT_WITH_KEY)
        file_path_root: str = f"{self.folder}/{cred.name}"
        pub_path: str = f"{file_path_root}_pub"
        prv_path: str = f"{file_path_root}_prv"
        env_var: str = f"CERT_{cred.name}_PWD"
        # Now read the files and return the strings
        with open(pub_path, 'r') as pub_file:
            pub_key: str = pub_file.read().strip()
        with open(prv_path, 'r') as prv_file:
            prv_key: str = prv_file.read().strip()
        # Return the value of the private key password from the environment variable
        pwd: Optional[str] = os.getenv(env_var)
        if pwd is None:
            raise CredentialNotAvailableException(cred, f"Private key password environment variable {env_var} is not set")
        else:
            return pub_key, prv_key, pwd

    def getAsToken(self, cred: Credential) -> str:
        """This fetches the credential and returns a token. This is used for API tokens."""
        if cred.credentialType == CredentialType.API_TOKEN:
            file_path: str = f"{self.folder}/{cred.name}"
            try:
                with open(file_path, 'r') as file:
                    return file.read().strip()
            except FileNotFoundError:
                raise CredentialNotAvailableException(cred, f"Credential file {file_path} not found.")
        else:
            raise CredentialWrongTypeException(cred, CredentialType.API_TOKEN)

    def checkCredentialIsAvailable(self, cred: Credential, tree: ValidationTree) -> None:
        return super().checkCredentialIsAvailable(cred, tree)

    def lintCredential(self, cred: Credential, tree: ValidationTree) -> None:
        """This store supports token, user password and api key pair"""
        if cred.credentialType not in [CredentialType.API_TOKEN, CredentialType.USER_PASSWORD, CredentialType.API_KEY_PAIR]:
            tree.addRaw(CredentialTypeNotSupportedProblem(cred, [CredentialType.API_TOKEN, CredentialType.USER_PASSWORD, CredentialType.API_KEY_PAIR]))
        # Check the name is a legal environment variable name using regex
        if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', cred.name):
            tree.addProblem(f"Credential name {cred.name} not compatible with an environment variable name", ProblemSeverity.ERROR)
            return
