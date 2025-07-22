"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.repo import GitHubRepository
from datasurface.md.model_loader import loadEcosystemFromEcoModule
from datasurface.md import Ecosystem, DataPlatform, EcosystemPipelineGraph, PlatformPipelineGraph, DataPlatformGraphHandler
from datasurface.md import ValidationTree
from typing import Optional
import os
import time
import subprocess


def generatePlatformBootstrap(ringLevel: int, modelFolderName: str, basePlatformDir: str, *platformNames: str) -> Ecosystem:
    """This will generate the platform bootstrap files for a given platform using the model defined in
    the model folder, the model must be called 'eco.py'. The files will be generated in the {basePlatformDir}/{platformName} directory."""

    # Load the model
    eco: Optional[Ecosystem]
    ecoTree: Optional[ValidationTree]
    eco, ecoTree = loadEcosystemFromEcoModule(modelFolderName)
    if eco is None or (ecoTree is not None and ecoTree.hasErrors()):
        if ecoTree is not None:
            ecoTree.printTree()
        raise Exception(f"Failed to load ecosystem from {modelFolderName}")

    # Generate the platform bootstrap files
    assert eco is not None

    # Find the platform
    for platformName in platformNames:
        dp: DataPlatform = eco.getDataPlatformOrThrow(platformName)
        # Generate the bootstrap files
        bootstrapArtifacts: dict[str, str] = dp.generateBootstrapArtifacts(eco, ringLevel)
        # Create the platform directory if it doesn't exist
        os.makedirs(os.path.join(basePlatformDir, platformName), exist_ok=True)
        for name, content in bootstrapArtifacts.items():
            with open(os.path.join(basePlatformDir, platformName, name), "w") as f:
                f.write(content)

    return eco


def getNewModelFolder(modelFolderName: str) -> str:
    """This will return the new model folder name in the model folder. The model folder is expected to be a directory
    with subdirectories named model-{timestamp_secs}."""

    # Get the current time UTC in seconds and zero pad it to 10 digits
    timestamp = int(time.time())
    timestamp_str = f"{timestamp:010d}"

    # Return the new model folder name
    return f"{modelFolderName}/model-{timestamp_str}"


def getLatestModelFolder(modelFolderName: str) -> str:
    """This will return the latest model folder name in the model folder. The model folder is expected to be a directory
    with subdirectories named model-{timestamp_secs}."""

    # Get the list of model folders
    modelFolders = os.listdir(modelFolderName)

    # Find the latest model folder
    latestModelFolder = max(modelFolders, key=lambda x: int(x.split("-")[1]))

    return latestModelFolder


def cloneGitRepository(repo: GitHubRepository, gitRepoPath: str) -> str:
    """This will clone the git repository into the given path. If the directory is empty, it will clone the repository.
    If the directory is not empty, it will not clone the repository."""

    # get as temporary folder in the folder gitRepoPath
    # Cannot have a dash in it.
    tempFolder = os.path.join(gitRepoPath, "modeltemp")

    # Clone the git repository if the directory is empty
    if not os.path.exists(tempFolder) or not os.listdir(tempFolder):
        git_token = os.environ.get('git_TOKEN')
        if not git_token:
            print("ERROR: git_TOKEN environment variable not found")
            return ""

        # Ensure the directory exists
        os.makedirs(tempFolder, exist_ok=True)

        # Clone the repository (billynewport/mvpmodel)
        git_url = f"https://{git_token}@github.com/{repo.repositoryName}.git"
        try:
            result = subprocess.run(
                ['git', 'clone', '--branch', repo.branchName, git_url, '.'],
                cwd=tempFolder,
                capture_output=True,
                text=True,
                check=True
            )
            print(f"Successfully cloned repository: {result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to clone repository: {e.stderr}")
            return ""

    # Get latest timestamp model folder and rename the temp folder to it. This means
    # jobs looking at a timestamp model folder never see a partial model while it
    # is being cloned.
    finalModelFolder = getNewModelFolder(gitRepoPath)
    os.rename(tempFolder, finalModelFolder)
    print(f"Cloned git repository into {finalModelFolder}")
    return finalModelFolder


def getLatestModelAtTimestampedFolder(repo: GitHubRepository, modelFolderBaseName: str, doClone: bool = False) -> \
            tuple[Optional[Ecosystem], Optional[ValidationTree]]:
    """This will return the latest model at a timestamped folder. The model folder is expected to be a directory
    with subdirectories named model-{timestamp_secs}."""

    latestModelFolder: str
    if doClone:
        # Clone the git repository if the directory is empty
        latestModelFolder = cloneGitRepository(repo, modelFolderBaseName)
    else:
        # Get latest model timestamped folder
        latestModelFolder = getLatestModelFolder(modelFolderBaseName)

    # Load the model
    eco: Optional[Ecosystem]
    ecoTree: Optional[ValidationTree]
    eco, ecoTree = loadEcosystemFromEcoModule(os.path.join(modelFolderBaseName, latestModelFolder))
    return eco, ecoTree


def handleModelMerge(modelFolderName: str, basePlatformDir: str, *platformNames: str) -> Ecosystem:
    """This creates the graph defined by the model and for the specified data platforms, it asks each dataplatform
    to turns the subset of the graph that it is responsible for in to artifacts to use to execute the data pipelines
    needed for this subset. These artifacts would include job schedule DAGs, terraform files and so on as needed."""

    # Load the model
    eco: Optional[Ecosystem]
    ecoTree: Optional[ValidationTree]
    eco, ecoTree = loadEcosystemFromEcoModule(modelFolderName)
    if eco is None or (ecoTree is not None and ecoTree.hasErrors()):
        if ecoTree is not None:
            ecoTree.printTree()
        raise Exception(f"Failed to load ecosystem from {modelFolderName}")

    # Generate the platform bootstrap files
    assert eco is not None

    # Find the platform
    for platformName in platformNames:
        dp: DataPlatform = eco.getDataPlatformOrThrow(platformName)
        graph: EcosystemPipelineGraph = EcosystemPipelineGraph(eco)

        # Get the platform graph
        platformGraph: PlatformPipelineGraph = graph.roots[dp.name]
        platformGraphHandler: DataPlatformGraphHandler = dp.createGraphHandler(platformGraph)

        # Create the platform directory if it doesn't exist
        os.makedirs(os.path.join(basePlatformDir, platformName), exist_ok=True)

        tree: ValidationTree = ValidationTree(eco)
        files: dict[str, str] = platformGraphHandler.renderGraph(dp.getCredentialStore(), tree)

        if tree.hasWarnings() or tree.hasErrors():
            tree.printTree()

        # Write the files to the platform directory
        for name, content in files.items():
            with open(os.path.join(basePlatformDir, platformName, name), "w") as f:
                f.write(content)

    return eco


def resetBatchState(modelFolderName: str, platformName: str, storeName: str, datasetName: Optional[str] = None) -> None:
    """Reset batch state for a YellowDataPlatform store/dataset.

    Args:
        modelFolderName: Model folder name (containing eco.py)
        platformName: Name of the YellowDataPlatform
        storeName: Name of the datastore to reset
        datasetName: Optional dataset name for single-dataset reset
    """

    # Load the model
    eco: Optional[Ecosystem]
    ecoTree: Optional[ValidationTree]
    eco, ecoTree = loadEcosystemFromEcoModule(modelFolderName)
    if eco is None or (ecoTree is not None and ecoTree.hasErrors()):
        if ecoTree is not None:
            ecoTree.printTree()
        raise Exception(f"Failed to load ecosystem from {modelFolderName}")

    assert eco is not None

    # Find the platform
    dp: DataPlatform = eco.getDataPlatformOrThrow(platformName)

    # Reset batch state
    dp.resetBatchState(eco, storeName, datasetName)


def executeDataTransformerJob(modelFolderName: str, platformName: str, workspaceName: str, workingFolder: str) -> None:
    """Execute a DataTransformer job for a specific workspace.

    Args:
        modelFolderName: Model folder name (containing eco.py)
        platformName: Name of the DataPlatform
        workspaceName: Name of the workspace containing the DataTransformer
        workingFolder: Working folder for temporary files
    """
    from datasurface.platforms.yellow.transformerjob import DataTransformerJob
    from datasurface.platforms.yellow.yellow_dp import YellowDataPlatform

    # Load the model
    eco: Optional[Ecosystem]
    ecoTree: Optional[ValidationTree]
    eco, ecoTree = loadEcosystemFromEcoModule(modelFolderName)
    if eco is None or (ecoTree is not None and ecoTree.hasErrors()):
        if ecoTree is not None:
            ecoTree.printTree()
        raise Exception(f"Failed to load ecosystem from {modelFolderName}")

    assert eco is not None

    # Find the platform
    dp: DataPlatform = eco.getDataPlatformOrThrow(platformName)

    # Ensure it's a YellowDataPlatform
    if not isinstance(dp, YellowDataPlatform):
        raise Exception(f"Platform {platformName} is not a YellowDataPlatform")

    # Get credential store
    credStore = dp.getCredentialStore()

    # Create and execute the DataTransformer job
    job = DataTransformerJob(eco, credStore, dp, workspaceName, workingFolder)
    status = job.run()

    print(f"DataTransformer job completed with status: {status.name}")

    # Exit with appropriate code for Airflow to understand
    if status.name == "DONE":
        exit(0)  # Success
    else:
        exit(1)  # Error


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Platform utilities for DataSurface")
    subparsers = parser.add_subparsers(dest="command")

    # Subcommand: generatePlatformBootstrap
    parser_bootstrap = subparsers.add_parser("generatePlatformBootstrap", help="Generate platform bootstrap files")
    parser_bootstrap.add_argument("--ringLevel", type=int, required=True, help=", 0 to N, Ring level to generate bootstrap artifacts for")
    parser_bootstrap.add_argument("--model", required=True, help="Model folder name (containing eco.py)")
    parser_bootstrap.add_argument("--output", required=True, help="Base output directory for platform files")
    parser_bootstrap.add_argument("--platform", required=True, nargs="+", help="One or more platform names")

    # Subcommand: handleModelMerge
    parser_merge = subparsers.add_parser("handleModelMerge", help="Generate pipeline artifacts for platforms")
    parser_merge.add_argument("--model", required=True, help="Model folder name (containing eco.py)")
    parser_merge.add_argument("--output", required=True, help="Base output directory for platform files")
    parser_merge.add_argument("--platform", required=True, nargs="+", help="One or more platform names")

    # Subcommand: resetBatchState
    parser_reset = subparsers.add_parser("resetBatchState", help="Reset batch state for YellowDataPlatform")
    parser_reset.add_argument("--model", required=True, help="Model folder name (containing eco.py)")
    parser_reset.add_argument("--platform", required=True, help="YellowDataPlatform name")
    parser_reset.add_argument("--store", required=True, help="Datastore name to reset")
    parser_reset.add_argument("--dataset", required=False, help="Optional dataset name for single-dataset reset")

    # Subcommand: executeDataTransformerJob
    parser_dt = subparsers.add_parser("executeDataTransformerJob", help="Execute a DataTransformer job")
    parser_dt.add_argument("--model", required=True, help="Model folder name (containing eco.py)")
    parser_dt.add_argument("--platform", required=True, help="DataPlatform name")
    parser_dt.add_argument("--workspace", required=True, help="Workspace name containing the DataTransformer")
    parser_dt.add_argument("--working-folder", required=True, help="Working folder for temporary files")

    args = parser.parse_args()
    if args.command == "generatePlatformBootstrap":
        generatePlatformBootstrap(args.ringLevel, args.model, args.output, *args.platform)
    elif args.command == "handleModelMerge":
        handleModelMerge(args.model, args.output, *args.platform)
    elif args.command == "resetBatchState":
        resetBatchState(args.model, args.platform, args.store, args.dataset)
    elif args.command == "executeDataTransformerJob":
        executeDataTransformerJob(args.model, args.platform, args.workspace, getattr(args, 'working_folder'))
    else:
        parser.print_help()
