from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
import importlib
import sys
import os
import copy
from types import ModuleType
from typing import Optional, Dict, Any, List, AsyncGenerator, Callable
import logging
from enum import Enum
from datasurface.md import Ecosystem


class EcosystemCommand(str, Enum):
    LIST_WORKSPACES = "list_workspaces"
    GET_DATASET = "get_dataset"
    GET_WORKSPACE = "get_workspace"
    LIST_TEAMS = "list_teams"
    GET_TEAM = "get_team"
    GET_DATASTORE = "get_datastore"
    LIST_DATASTORES = "list_datastores"
    GET_DEPENDENCIES = "get_dependencies"


class QueryRequest(BaseModel):
    command: EcosystemCommand
    params: Dict[str, Any]


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

            print(f"Python path: {sys.path}")  # Debug print
            print(f"Looking for eco.py in: {self.model_path}")

            # Remove the module to force reload
            if 'eco' in sys.modules:
                del sys.modules['eco']
            if 'tests.actionHandlerResources.step4.eco' in sys.modules:
                del sys.modules['tests.actionHandlerResources.step4.eco']

            try:
                # First try importing as a package
                try:
                    module: ModuleType = importlib.import_module("tests.actionHandlerResources.step4.eco")
                    print("Loaded eco.py as package")
                    return module
                except ModuleNotFoundError:
                    # Fall back to direct import
                    module: ModuleType = importlib.import_module("eco")
                    print("Loaded eco.py directly")
                    return module
            except ModuleNotFoundError as e:
                logging.error(f"Failed to load eco module: {e}")
                print(f"Failed to load eco module: {e}")  # Debug print
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
            # Get the command value and convert it to the enum
            command_str: str = query['command']
            command: EcosystemCommand = EcosystemCommand(command_str)
            params: Dict[str, Any] = query['params']

            print(f"Executing query with command: {command}, type: {type(command)}, value: {command.value}")  # Debug print
            print(f"Command as string: {command_str}")  # Debug print

            # Using match statement for command handling
            match command.value:  # Match on the enum value
                case "list_workspaces":
                    return {"workspaces": [entry.workspace.name for entry in self.ecosystem.workSpaceCache.values()]}

                case "get_dataset":
                    dataset = self.ecosystem.cache_getDataset(params.get("store_name", ""), params["dataset_name"])
                    if not dataset:
                        raise ValueError(f"Dataset {params['dataset_name']} not found")
                    return {"dataset": str(dataset)}

                case "get_workspace":
                    workspace = self.ecosystem.cache_getWorkspaceOrThrow(params["workspace_name"])
                    return {"workspace": str(workspace)}

                case "list_teams":
                    return {"teams": list(self.ecosystem.teamCache.keys())}

                case "get_team":
                    team = self.ecosystem.getTeamOrThrow(params["governance_zone"], params["team_name"])
                    return {"team": str(team)}

                case "get_datastore":
                    store = self.ecosystem.cache_getDatastoreOrThrow(params["store_name"])
                    return {"datastore": str(store)}

                case "list_datastores":
                    return {"datastores": list(self.ecosystem.datastoreCache.keys())}

                case "get_dependencies":
                    deps = self.ecosystem.calculateDependenciesForDatastore(params["store_name"])
                    return {"dependencies": [str(dep) for dep in deps]}

                case _:
                    raise ValueError(f"Unknown command: {command}")

        except Exception as e:
            logging.error(f"Query execution failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Startup
    global model_server
    model_path: str = os.environ.get("MODEL_PATH", "/app/model")
    model_server = ModelServer(model_path)
    logging.info(f"Started language server with model version: {model_server.version}")
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


@app.post("/api/query")
async def handle_query(
    query: QueryRequest,
    session_id: Optional[str] = Header(None),
    model_version: Optional[str] = Header(None)
) -> Dict[str, Any]:
    if not model_server:
        raise HTTPException(status_code=503, detail="Server not ready")

    if model_version and model_version != model_server.version:
        raise HTTPException(
            status_code=400,
            detail=f"Version mismatch. Server version: {model_server.version}"
        )

    return await model_server.execute_query(query.model_dump())
