"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import PlatformServicesProvider
from datasurface.md import Datastore, IngestionMetadata
from datasurface.md import Ecosystem, CredentialStore, LocationKey, Credential
from datasurface.md.lint import ValidationTree, ProblemSeverity
import os
from datasurface.md.credential import FileSecretCredential, ClearTextCredential, CredentialType, LocalFileCredentialStore

# The design of this is documented here: docs/SwarmRenderer.md


class DockerSwarmCredentialStore(LocalFileCredentialStore):
    """This represents a set of docker swarm secrets to DataSurface. The credentials will
    all be stored in temporary RAM files in the secret folder when a container starts."""
    def __init__(self, name: str, locs: set[LocationKey], folder: str):
        super().__init__(name, locs, folder)

    def checkCredentialIsAvailable(self, cred: Credential, tree: ValidationTree) -> None:
        if isinstance(cred, FileSecretCredential):
            if cred.secretFilePath == "":
                tree.addProblem("Secret file path is empty")
            else:
                # Check secret for this Credential exists
                # Depending on the credential type there may be more than one file
                if cred.credentialType == CredentialType.API_KEY_PAIR:
                    # Check for the access key and secret key in the same file
                    accessKeyFile: str = f"{self.folder}/{cred.secretFilePath}_access"
                    if not os.path.exists(accessKeyFile):
                        tree.addProblem(f"Access key file {accessKeyFile} does not exist")
                elif cred.credentialType == CredentialType.USER_PASSWORD:
                    # Check for the username and password in the same file
                    usernamePasswordFile: str = f"{self.folder}/{cred.secretFilePath}"
                    if not os.path.exists(usernamePasswordFile):
                        tree.addProblem(f"Username and password file {usernamePasswordFile} does not exist")
                elif cred.credentialType == CredentialType.CLIENT_CERT_WITH_KEY:
                    # Check for the public key, private key and password in the same file
                    publicKeyFile: str = f"{self.folder}/{cred.secretFilePath}_pub"
                    privateKeyFile: str = f"{self.folder}/{cred.secretFilePath}_prv"
                    passwordFile: str = f"{self.folder}/{cred.secretFilePath}_pwd"
                    if not os.path.exists(publicKeyFile):
                        tree.addProblem(f"Public key file {publicKeyFile} does not exist")
                    if not os.path.exists(privateKeyFile):
                        tree.addProblem(f"Private key file {privateKeyFile} does not exist")
                    if not os.path.exists(passwordFile):
                        tree.addProblem(f"Password file {passwordFile} does not exist")
                else:
                    tree.addProblem(f"Unsupported credential type: {cred.credentialType}", ProblemSeverity.ERROR)
        elif isinstance(cred, ClearTextCredential):
            pass
        else:
            tree.addProblem(f"Unsupported credential type: {type(cred)}", ProblemSeverity.ERROR)

    def getAsUserPassword(self, cred: Credential) -> tuple[str, str]:
        return super().getAsUserPassword(cred)

    def getAsPublicPrivateCertificate(self, cred: Credential) -> tuple[str, str, str]:
        return super().getAsPublicPrivateCertificate(cred)


class DockerSwarmServicesProvider(PlatformServicesProvider):
    """This is a provider which takes a full intention graph of a DataSurface Ecosystem and then
    invokes each DataPlatform to render the subset assigned to it. The DataPlatform renders its
    graph on a particular technology stack with a specific configuration. A render engine works within
    a specific runtime context. This runtime engine works within a Docker Swarm cluster and uses
    docker swarm for secret management as well as for containers."""

    def __init__(self, name: str, credStore: CredentialStore, brokerFolder: str = "/mnt/broker"):
        super().__init__(name, credStore)
        self.brokerFolder = brokerFolder

    def lint(self, eco: Ecosystem, tree: ValidationTree):
        super().lint(eco, tree)

    def verifyAllCredentialsArePresent(self, eco: Ecosystem, tree: ValidationTree):
        # We want to check all Credentials used are present as secrets before
        # we try to do anything. Missing credentials would also hold up
        # deployment of a model change if those changes required more credentials.
        # Credentials are specified for ingestions and for DataTransformers.

        # First we get all Datastores and look at the CMDs for Credentials
        # and verify they are present as secrets by seeing if the file exists.
        eco.lintAndHydrateCaches()
        for storeCacheEntry in eco.datastoreCache.values():
            store: Datastore = storeCacheEntry.datastore
            if store.cmd is not None and isinstance(store.cmd, IngestionMetadata):
                if store.cmd.credential is not None:
                    self.credStore.checkCredentialIsAvailable(store.cmd.credential, tree.addSubTree(store.cmd.credential))
