"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md import BrokerRenderEngine, LocalFileCredentialStore, FileSecretCredential
from datasurface.md import ClearTextCredential
from datasurface.md import Ecosystem, EcosystemPipelineGraph, CredentialStore, LocationKey, Credential
from datasurface.md.lint import ValidationTree, ProblemSeverity


class DockerSwarmCredentialStore(LocalFileCredentialStore):
    """This represents a set of docker swarm secrets to DataSurface. The credentials will
    all be stored in temporary RAM files in the secret folder when a container starts."""
    def __init__(self, name: str, locs: set[LocationKey], folder: str):
        super().__init__(name, locs, folder)

    def lintCredential(self, cred: Credential, tree: ValidationTree) -> None:
        if isinstance(cred, FileSecretCredential):
            if cred.secretFilePath == "":
                tree.addProblem("Secret file path is empty")
        elif isinstance(cred, ClearTextCredential):
            pass
        else:
            tree.addProblem(f"Unsupported credential type: {type(cred)}", ProblemSeverity.ERROR)

    def getAsUserPassword(self, cred: Credential) -> tuple[str, str]:
        """This will read the file holding the secret and return the first and second lines
        as a tuple to the caller."""
        if isinstance(cred, FileSecretCredential):
            try:
                file_path = f"{self.folder}/{cred.secretFilePath}"
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


class SwarmRenderEngine(BrokerRenderEngine):
    """This is a render engine which takes a full intention graph of a DataSurface Ecosystem and then
    invokes each DataPlatform to render the subset assigned to it. The DataPlatform renders its
    graph on a particular technology stack with a specific configuration. A render engine works within
    a specific runtime context. This runtime engine works within a Docker Swarm cluster and uses
    docker swarm for secret management as well as for containers."""

    def __init__(self, name: str, ecosystem: Ecosystem, credStore: CredentialStore, brokerFolder: str = "/mnt/broker"):
        super().__init__(name, ecosystem, credStore)
        self.graph: EcosystemPipelineGraph = EcosystemPipelineGraph(ecosystem)
        self.brokerFolder = brokerFolder

    def lint(self, tree: ValidationTree):
        self.graph.lint(self.credStore, tree.addSubTree(self.graph))
