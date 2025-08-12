"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header, Request
from dataclasses import dataclass
from pydantic import BaseModel
import importlib
import sys
import os
import copy
from types import ModuleType
from typing import Optional, Dict, Any, List, AsyncGenerator, Callable, Protocol
import logging
from enum import Enum
from datasurface.md import Ecosystem, JSONable

# Standard Python logger - works everywhere
logger = logging.getLogger(__name__)


# Pydantic models for API documentation
class QueryRequestModel(BaseModel):
    """API schema for query requests"""
    command: str
    params: Dict[str, Any]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "command": "list_workspaces",
                    "params": {}
                },
                {
                    "command": "get_dataset",
                    "params": {
                        "store_name": "my_store",
                        "dataset_name": "my_dataset"
                    }
                }
            ]
        }
    }


class QueryResponseModel(BaseModel):
    """API schema for query responses"""
    workspaces: Optional[List[Dict[str, Any]]] = None
    dataset: Optional[Dict[str, Any]] = None
    workspace: Optional[Dict[str, Any]] = None
    teams: Optional[List[str]] = None
    team: Optional[Dict[str, Any]] = None
    datastore: Optional[Dict[str, Any]] = None
    datastores: Optional[List[str]] = None
    dependencies: Optional[List[Dict[str, Any]]] = None


class JsonSerializable(Protocol):
    """Protocol for JSON serializable objects"""
    def to_dict(self) -> Dict[str, Any]: ...


def serialize_object(obj: JSONable) -> dict[str, Any]:
    """Helper function to serialize objects that might implement to_dict"""
    return obj.to_json()


class EcosystemCommand(str, Enum):
    LIST_WORKSPACES = "list_workspaces"
    GET_DATASET = "get_dataset"
    GET_WORKSPACE = "get_workspace"
    LIST_TEAMS = "list_teams"
    GET_TEAM = "get_team"
    GET_DATASTORE = "get_datastore"
    LIST_DATASTORES = "list_datastores"
    GET_DEPENDENCIES = "get_dependencies"


@dataclass
class QueryRequest:
    command: EcosystemCommand
    params: Dict[str, Any]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryRequest':
        if not data:
            raise ValueError("Input must be a non-empty dictionary")

        if 'command' not in data or 'params' not in data:
            raise ValueError("Missing required fields: command, params")

        try:
            command = EcosystemCommand(data['command'])
        except ValueError:
            raise ValueError(f"Invalid command: {data['command']}")

        params: Dict[str, Any] = data['params']
        return cls(command=command, params=params)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "command": self.command,
            "params": self.params
        }


class ModelServer:
    def __init__(self, model_path: str) -> None:
        self.model_path: str = model_path
        self.model: ModuleType = self._load_model()
        self.version: str = self._get_model_version()
        self.ecosystem: Ecosystem = self._load_ecosystem()

    def _load_model(self) -> ModuleType:
        """Load model from specified path"""
        orig_system_path: List[str] = copy.deepcopy(sys.path)
        try:
            # Add both the model path and its parent directory to sys.path
            model_parent = os.path.dirname(self.model_path)
            if model_parent not in sys.path:
                sys.path.insert(0, model_parent)
            if self.model_path not in sys.path:
                sys.path.insert(0, self.model_path)

            logger.info(f"Python path: {sys.path}")  # Debug print
            logger.info(f"Looking for eco.py in: {self.model_path}")

            # Remove the module to force reload
            if 'eco' in sys.modules:
                del sys.modules['eco']
            if 'tests.actionHandlerResources.step4.eco' in sys.modules:
                del sys.modules['tests.actionHandlerResources.step4.eco']

            try:
                # First try importing as a package
                try:
                    module: ModuleType = importlib.import_module("tests.actionHandlerResources.step4.eco")
                    logger.info("Loaded eco.py as package")
                    return module
                except ModuleNotFoundError:
                    # Fall back to direct import
                    module: ModuleType = importlib.import_module("eco")
                    logger.info("Loaded eco.py directly")
                    return module
            except ModuleNotFoundError as e:
                logger.error(f"Failed to load eco module: {e}")  # Debug print
                raise RuntimeError(f"Failed to find eco.py in model path: {self.model_path}")
        finally:
            sys.path = orig_system_path

    def _load_ecosystem(self) -> Ecosystem:
        function: Callable[[], Ecosystem] = getattr(self.model, "createEcosystem")
        ecosystem: Ecosystem = function()
        ecosystem.lintAndHydrateCaches()
        return ecosystem

    def _get_model_version(self) -> str:
        """Get version from model metadata"""
        return getattr(self.model, 'VERSION', 'unknown')

    async def execute_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute query against model"""
        try:
            command_str: str = query['command']
            command: EcosystemCommand = EcosystemCommand(command_str)
            params: Dict[str, Any] = query['params']

            # Using match statement for command handling
            match command_str:
                case EcosystemCommand.LIST_WORKSPACES.value:
                    return {"workspaces": [serialize_object(entry.workspace) for entry in self.ecosystem.workSpaceCache.values()]}

                case EcosystemCommand.GET_DATASET.value:
                    store_name: str = params.get("store_name", "")
                    if len(store_name) == 0:
                        raise ValueError("store_name is required")
                    dataset = self.ecosystem.cache_getDataset(store_name, params["dataset_name"])
                    if not dataset:
                        raise ValueError(f"Dataset {params['dataset_name']} not found")
                    return {"dataset": serialize_object(dataset)}

                case EcosystemCommand.GET_WORKSPACE.value:
                    workspace_name: str = params.get("workspace_name", "")
                    if len(workspace_name) == 0:
                        raise ValueError("workspace_name is required")
                    workspace = self.ecosystem.cache_getWorkspaceOrThrow(workspace_name)
                    return {"workspace": serialize_object(workspace.workspace)}

                case EcosystemCommand.LIST_TEAMS.value:
                    return {"teams": list(self.ecosystem.teamCache.keys())}

                case EcosystemCommand.GET_TEAM.value:
                    governance_zone: str = params.get("governance_zone", "")
                    if len(governance_zone) == 0:
                        raise ValueError("governance_zone is required")
                    team_name: str = params.get("team_name", "")
                    if len(team_name) == 0:
                        raise ValueError("team_name is required")
                    team = self.ecosystem.getTeamOrThrow(governance_zone, team_name)
                    return {"team": serialize_object(team)}

                case EcosystemCommand.GET_DATASTORE.value:
                    store_name: str = params.get("store_name", "")
                    if len(store_name) == 0:
                        raise ValueError("store_name is required")
                    store_entry = self.ecosystem.cache_getDatastoreOrThrow(store_name)
                    return {"datastore": serialize_object(store_entry.datastore)}

                case EcosystemCommand.LIST_DATASTORES.value:
                    return {"datastores": list(self.ecosystem.datastoreCache.keys())}

                case EcosystemCommand.GET_DEPENDENCIES.value:
                    store_name: str = params.get("store_name", "")
                    if len(store_name) == 0:
                        raise ValueError("store_name is required")
                    deps = self.ecosystem.calculateDependenciesForDatastore(store_name, set())
                    return {"dependencies": [serialize_object(dep) for dep in deps]}

                case _:
                    raise ValueError(f"Unknown command: {command}")

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Startup
    global model_server
    model_path: str = os.environ.get("MODEL_PATH", "/app/model")
    model_server = ModelServer(model_path)
    logger.info(f"Started language server with model version: {model_server.version}")
    yield
    # Shutdown
    model_server = None


app: FastAPI = FastAPI(lifespan=lifespan)
model_server: Optional[ModelServer] = None


@app.get("/health")
async def health_check() -> Dict[str, str]:
    return {
        "status": "healthy",
        "version": model_server.version if model_server else "not_loaded"
    }


@app.post("/api/query", response_model=QueryResponseModel)
async def handle_query(
    request: Request,
    session_id: Optional[str] = Header(None),
    model_version: Optional[str] = Header(None)
) -> Dict[str, Any]:
    """
    Execute a query against the model server.

    The query can be one of:
    - list_workspaces: List all workspaces
    - get_dataset: Get details of a specific dataset
    - get_workspace: Get details of a specific workspace
    - list_teams: List all teams
    - get_team: Get details of a specific team
    - get_datastore: Get details of a specific datastore
    - list_datastores: List all datastores
    - get_dependencies: Get dependencies for a datastore
    """
    if not model_server:
        raise HTTPException(status_code=503, detail="Server not ready")

    if model_version and model_version != model_server.version:
        raise HTTPException(
            status_code=400,
            detail=f"Version mismatch. Server version: {model_server.version}"
        )

    try:
        body = await request.json()
        query = QueryRequest.from_dict(body)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return await model_server.execute_query(query.to_dict())
