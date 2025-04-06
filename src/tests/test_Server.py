import os
import sys
import pytest
from fastapi.testclient import TestClient
from typing import List, Dict, Any, cast, TypeVar, ClassVar, Generator, Optional
from datasurface.md.server import app, EcosystemCommand, QueryRequest, ModelServer


# Type variable for response data
ResponseData = TypeVar('ResponseData', Dict[str, str], Dict[str, List[str]], Dict[str, Any])


# Add src directory to Python path if not already there
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
    print(f"Added src directory to Python path: {src_dir}")


# Get the absolute path to the test resources
TEST_MODEL_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__),
    "actionHandlerResources",
    "step4"
))

print(f"Loading model from: {TEST_MODEL_PATH}")  # Debug print
print(f"Directory exists: {os.path.exists(TEST_MODEL_PATH)}")
print(f"eco.py exists: {os.path.exists(os.path.join(TEST_MODEL_PATH, 'eco.py'))}")
print(f"Current working directory: {os.getcwd()}")
print(f"Python path: {sys.path}")


class TestModelServer:
    """Test suite for the Model Server API endpoints."""

    # Class variables with type hints
    test_client: ClassVar[TestClient]
    model_path: ClassVar[str] = TEST_MODEL_PATH
    model_server: ClassVar[Optional[ModelServer]]

    @pytest.fixture(autouse=True, scope="class")
    def setup_class(self, request: pytest.FixtureRequest) -> Generator[None, None, None]:
        """Set up test environment before running tests.

        Args:
            request: Pytest fixture request object

        Yields:
            None
        """
        print(f"Loading model from: {self.model_path}")
        print(f"Directory exists: {os.path.exists(self.model_path)}")
        print(f"eco.py exists: {os.path.exists(os.path.join(self.model_path, 'eco.py'))}")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Python path: {sys.path}")

        os.environ["MODEL_PATH"] = self.model_path
        print(f"Setting MODEL_PATH environment variable to: {self.model_path}")

        # Initialize model server directly
        self.__class__.model_server = ModelServer(self.model_path)

        # Initialize test client with the model server
        self.__class__.test_client = TestClient(app)

        # Run lifespan event
        with self.test_client as client:
            # Check if server is healthy before proceeding
            print("Making health check request...")
            response = client.get("/health")
            print(f"Health check response: {response.status_code}")
            if response.status_code != 200:
                print(f"Health check failed with response: {response.text}")
            assert response.status_code == 200
            data = cast(Dict[str, str], response.json())
            assert data["status"] == "healthy"

            yield

            # Cleanup
            self.__class__.model_server = None

    def test_health_check(self) -> None:
        """Test the health check endpoint."""
        response = self.test_client.get("/health")
        assert response.status_code == 200
        data = cast(Dict[str, str], response.json())
        assert data["status"] == "healthy"
        assert "version" in data

    def test_list_workspaces(self) -> None:
        """Test listing all workspaces."""
        query = QueryRequest(
            command=EcosystemCommand.LIST_WORKSPACES,
            params={}
        )
        response = self.test_client.post("/api/query", json=query.to_dict())
        assert response.status_code == 200
        data = cast(Dict[str, List[str]], response.json())
        workspaces = data["workspaces"]
        assert isinstance(workspaces, list)
        # There are no workspaces in the test model
        assert len(workspaces) == 0

    def test_list_datastores(self) -> None:
        """Test listing all datastores."""
        query = QueryRequest(
            command=EcosystemCommand.LIST_DATASTORES,
            params={}
        )
        response = self.test_client.post("/api/query", json=query.to_dict())
        assert response.status_code == 200
        data = cast(Dict[str, List[str]], response.json())
        datastores = data["datastores"]
        assert isinstance(datastores, list)
        assert len(datastores) == 2
        assert "EU_Customers" in datastores
        assert "USA_Customers" in datastores

    def test_get_dataset(self) -> None:
        """Test getting a specific dataset."""
        # First get a list of datastores
        query = QueryRequest(
            command=EcosystemCommand.LIST_DATASTORES,
            params={}
        )
        response = self.test_client.post("/api/query", json=query.to_dict())
        assert response.status_code == 200
        data = cast(Dict[str, list[str]], response.json())
        store_name: str = data["datastores"][0]  # Use the first store

        # Fetch that store
        query = QueryRequest(
            command=EcosystemCommand.GET_DATASTORE,
            params={
                "store_name": store_name
            }
        )
        response = self.test_client.post("/api/query", json=query.to_dict())
        assert response.status_code == 200
        data = cast(Dict[str, Any], response.json())
        store = data["datastore"]
        assert store["name"] == store_name
        assert "team" in store
        assert "governance_zone" in store
        assert "datasets" in store
        datasets: dict[str, Any] = store["datasets"]
        assert isinstance(datasets, dict)
        assert len(datasets) > 0
        firstDataset: dict[str, Any] = list(datasets.values())[0]
        dataset_name: str = firstDataset["name"]

        # Now try to get a dataset from that store
        query = QueryRequest(
            command=EcosystemCommand.GET_DATASET,
            params={
                "store_name": store_name,
                "dataset_name": dataset_name
            }
        )
        response = self.test_client.post("/api/query", json=query.to_dict())
        # Note: This might need adjustment based on your test data
        assert response.status_code in [200, 404]  # 404 if dataset doesn't exist
        data = cast(Dict[str, List[str]], response.json())
        assert "dataset" in data

    def test_get_workspace(self) -> None:
        """Test getting a specific workspace."""
        # First get list of workspaces
        query = QueryRequest(
            command=EcosystemCommand.LIST_WORKSPACES,
            params={}
        )
        response = self.test_client.post("/api/query", json=query.to_dict())
        assert response.status_code == 200
        data = cast(Dict[str, List[str]], response.json())
        workspaces = data["workspaces"]
        # There are no workspaces in the test model
        assert len(workspaces) == 0

#        # Now get details for the first workspace
#        query = QueryRequest(
#            command=EcosystemCommand.GET_WORKSPACE,
#            params={
#                "workspace_name": workspaces[0]
#            }
#        )
#        response = self.test_client.post("/api/query", json=query.to_dict())
#        assert response.status_code == 200
#        data = cast(Dict[str, Any], response.json())
#        assert "workspace" in data

    def test_invalid_command(self) -> None:
        """Test handling of invalid commands."""
        response = self.test_client.post("/api/query", json={
            "command": "INVALID_COMMAND",
            "params": {}
        })
        assert response.status_code == 400  # Bad request for invalid command
        data = cast(Dict[str, str], response.json())
        assert "Invalid command" in data["detail"]

    def test_version_mismatch(self) -> None:
        """Test version mismatch handling."""
        query = QueryRequest(
            command=EcosystemCommand.LIST_WORKSPACES,
            params={}
        )
        response = self.test_client.post(
            "/api/query",
            json=query.to_dict(),
            headers={"model-version": "invalid_version"}
        )
        assert response.status_code == 400
        data = cast(Dict[str, str], response.json())
        assert "Version mismatch" in data["detail"]
