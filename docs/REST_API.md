# DataSurface REST APIs

DataSurface provides a comprehensive REST API for querying various aspects of the Ecosystem model. The API implements a stateless service that provides read-only access to model data including producers, datastores, workspaces, teams, and their relationships.

## Overview

### Read Only API

This API provides a read-only interface to the DataSurface ecosystem model. All updates must be made through the standard GitHub process and CI/CD workflows. The API servers can be restarted and pointed at newer branches to reflect model updates.

### Implementation

The REST API is implemented using FastAPI and follows a command-based architecture. The API accepts structured JSON commands and returns JSON responses containing the requested model data.

**Key Features:**
- In-memory model loading for fast query performance
- No persistent database dependencies
- Version-aware queries with optional version checking
- Comprehensive error handling and validation
- Auto-generated OpenAPI documentation

### Base URL and Versioning

The API runs on a configurable port (typically 8000) and provides the following base endpoints:

- **Health Check**: `GET /health`
- **Query Interface**: `POST /api/query`
- **API Documentation**: `GET /docs` (auto-generated Swagger UI)

## Health Check Endpoint

### `GET /health`

Returns the current health status and version of the model server.

**Response:**
```json
{
  "status": "healthy",
  "version": "unknown"
}
```

**Note:** The version field returns the value from the model's `VERSION` attribute, defaulting to `"unknown"` if not set. When the server is not ready, the version field will contain `"not_loaded"`.

## Query Interface

All data queries are performed through a single endpoint that accepts command-based JSON requests.

### `POST /api/query`

**Headers:**
- `Content-Type: application/json` (required)
- `session_id: string` (optional) - Client session identifier
- `model_version: string` (optional) - Expected model version for validation

**Request Format:**
```json
{
  "command": "command_name",
  "params": {
    "param1": "value1",
    "param2": "value2"
  }
}
```

**Response Format:**
```json
{
  "field_name": "response_data"
}
```

**Note:** Each command returns a single field in the response JSON object. The field name corresponds to the specific command (e.g., `list_workspaces` returns a `workspaces` field, `get_dataset` returns a `dataset` field, etc.).

**Parameter Validation:** All required parameters are validated server-side. Missing or empty required parameters will result in a 500 error with a descriptive message (e.g., "store_name is required").

## Available Commands

### Workspace Operations

#### List All Workspaces

**Command:** `list_workspaces`

Lists all workspaces in the ecosystem.

**Request:**
```json
{
  "command": "list_workspaces",
  "params": {}
}
```

**Response:**
```json
{
  "workspaces": [
    {
      "name": "analytics_workspace",
      "documentation": {
        "text": "Analytics workspace for business intelligence"
      },
      "datasetGroups": [...],
      "key": {
        "workspaceName": "analytics_workspace",
        "teamName": "analytics_team",
        "governanceZoneName": "production"
      }
    }
  ]
}
```

#### Get Specific Workspace

**Command:** `get_workspace`

Retrieves detailed information about a specific workspace.

**Parameters:**
- `workspace_name` (string, required) - Name of the workspace

**Request:**
```json
{
  "command": "get_workspace",
  "params": {
    "workspace_name": "analytics_workspace"
  }
}
```

**Response:**
```json
{
  "workspace": {
    "name": "analytics_workspace",
    "documentation": {
      "text": "Analytics workspace for business intelligence"
    },
    "datasetGroups": [
      {
        "name": "live_data",
        "datasetSinks": [
          {
            "datastoreName": "customer_data",
            "datasetName": "customers"
          }
        ]
      }
    ],
    "dataContainer": {
      "name": "analytics_db",
      "type": "PostgresDatabase"
    }
  }
}
```

### Dataset Operations

#### Get Dataset Details

**Command:** `get_dataset`

Retrieves detailed schema and metadata for a specific dataset.

**Parameters:**
- `store_name` (string, required) - Name of the datastore containing the dataset
- `dataset_name` (string, required) - Name of the dataset

**Request:**
```json
{
  "command": "get_dataset",
  "params": {
    "store_name": "customer_data",
    "dataset_name": "customers"
  }
}
```

**Response:**
```json
{
  "dataset": {
    "name": "customers",
    "schema": {
      "type": "DDLTable",
      "columns": [
        {
          "name": "customer_id",
          "type": "VarChar(5)",
          "nullable": false,
          "primaryKey": true
        },
        {
          "name": "company_name",
          "type": "VarChar(40)",
          "nullable": false,
          "primaryKey": false
        }
      ]
    },
    "dataClassification": "PC3",
    "documentation": {
      "text": "Customer information with PII data"
    }
  }
}
```

### Datastore Operations

#### List All Datastores

**Command:** `list_datastores`

Returns a list of all datastore names in the ecosystem.

**Request:**
```json
{
  "command": "list_datastores",
  "params": {}
}
```

**Response:**
```json
{
  "datastores": [
    "customer_data",
    "product_catalog",
    "order_history",
    "financial_data"
  ]
}
```

#### Get Datastore Details

**Command:** `get_datastore`

Retrieves comprehensive information about a specific datastore including its datasets and capture metadata.

**Parameters:**
- `store_name` (string, required) - Name of the datastore

**Request:**
```json
{
  "command": "get_datastore",
  "params": {
    "store_name": "customer_data"
  }
}
```

**Response:**
```json
{
  "datastore": {
    "name": "customer_data",
    "datasets": {
      "customers": {
        "name": "customers",
        "schema": {...},
        "dataClassification": "PC3"
      },
      "addresses": {
        "name": "addresses",
        "schema": {...},
        "dataClassification": "PC2"
      }
    },
    "captureMetadata": {
      "type": "CDCCaptureIngestion",
      "sourceInfo": {
        "serverHost": "tcp:nwdb.database.windows.net,1433",
        "databaseName": "nwdb"
      },
      "schedule": {
        "cronExpression": "*/10 * * * *"
      }
    },
    "productionStatus": "PRODUCTION",
    "deprecationStatus": "NOT_DEPRECATED"
  }
}
```

### Team Operations

#### List All Teams

**Command:** `list_teams`

Returns a list of all team names across all governance zones.

**Request:**
```json
{
  "command": "list_teams",
  "params": {}
}
```

**Response:**
```json
{
  "teams": [
    "analytics_team",
    "data_engineering",
    "product_team",
    "finance_team"
  ]
}
```

#### Get Team Details

**Command:** `get_team`

Retrieves detailed information about a specific team including its datastores and workspaces.

**Parameters:**
- `governance_zone` (string, required) - Name of the governance zone containing the team
- `team_name` (string, required) - Name of the team

**Request:**
```json
{
  "command": "get_team",
  "params": {
    "governance_zone": "production",
    "team_name": "analytics_team"
  }
}
```

**Response:**
```json
{
  "team": {
    "name": "analytics_team",
    "datastores": {
      "customer_data": {...}
    },
    "workspaces": {
      "analytics_workspace": {...}
    },
    "documentation": {
      "text": "Team responsible for analytics infrastructure"
    }
  }
}
```

### Dependency Analysis

#### Get Datastore Dependencies

**Command:** `get_dependencies`

Calculates and returns the dependency graph for a specific datastore, showing which other datastores and transformers depend on it.

**Parameters:**
- `store_name` (string, required) - Name of the datastore to analyze

**Request:**
```json
{
  "command": "get_dependencies",
  "params": {
    "store_name": "customer_data"
  }
}
```

**Response:**
```json
{
  "dependencies": [
    {
      "type": "Workspace",
      "name": "analytics_workspace",
      "team": "analytics_team",
      "dependencyType": "consumer"
    },
    {
      "type": "DataTransformer",
      "name": "customer_anonymizer",
      "team": "privacy_team",
      "dependencyType": "transformer"
    }
  ]
}
```

## Error Responses

The API returns standard HTTP status codes with detailed error messages:

### 400 Bad Request

**Possible error messages:**
- Missing request fields:
```json
{
  "detail": "Missing required fields: command, params"
}
```

- Invalid command:
```json
{
  "detail": "Invalid command: unknown_command"
}
```

- Version mismatch:
```json
{
  "detail": "Version mismatch. Server version: unknown"
}
```

- Empty request body:
```json
{
  "detail": "Input must be a non-empty dictionary"
}
```

### 404 Not Found
**Note:** The current server implementation does not return 404 errors. Missing resources are handled as 500 errors instead.

### 503 Service Unavailable
```json
{
  "detail": "Server not ready"
}
```

### 500 Internal Server Error

**Note:** Most application errors (including missing datasets, teams, workspaces, etc.) return 500 errors, not 404 errors.

```json
{
  "detail": "Query execution failed: [error details]"
}
```

**Common 500 error scenarios:**
- Missing required parameters:
  - `"store_name is required"`
  - `"workspace_name is required"`
  - `"governance_zone is required"`
  - `"team_name is required"`
- Dataset not found: `"Dataset 'dataset_name' not found"`
- Unknown command: `"Unknown command: EcosystemCommand.INVALID_COMMAND"`
- General query failures and ecosystem method exceptions

## Usage Examples

### Python Client Example

```python
import requests
import json

# Base URL for the DataSurface API server
BASE_URL = "http://localhost:8000"

def query_datasurface(command, params=None):
    """Helper function to query the DataSurface API"""
    if params is None:
        params = {}
    
    response = requests.post(
        f"{BASE_URL}/api/query",
        headers={"Content-Type": "application/json"},
        json={"command": command, "params": params}
    )
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API Error: {response.status_code} - {response.text}")

# List all workspaces
workspaces = query_datasurface("list_workspaces")
print(f"Found {len(workspaces['workspaces'])} workspaces")

# Get specific dataset details
dataset = query_datasurface("get_dataset", {
    "store_name": "customer_data",
    "dataset_name": "customers"
})
print(f"Dataset schema: {dataset['dataset']['schema']}")

# Check datastore dependencies
deps = query_datasurface("get_dependencies", {
    "store_name": "customer_data"
})
print(f"Found {len(deps['dependencies'])} dependencies")
```

### cURL Examples

```bash
# Health check
curl -X GET http://localhost:8000/health

# List all datastores
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"command": "list_datastores", "params": {}}'

# Get workspace details
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{
    "command": "get_workspace",
    "params": {
      "workspace_name": "analytics_workspace"
    }
  }'
```

## Server Configuration

### Environment Variables

- `MODEL_PATH` - Path to the directory containing the `eco.py` model file (default: `/app/model`)
- `PORT` - Port number for the API server (default: 8000)

### Docker Deployment

```dockerfile
FROM python:3.11-slim

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . /app
WORKDIR /app

ENV MODEL_PATH=/app/model
EXPOSE 8000

CMD ["python", "-m", "uvicorn", "datasurface.md.server:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: datasurface-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: datasurface-api
  template:
    metadata:
      labels:
        app: datasurface-api
    spec:
      containers:
      - name: api-server
        image: datasurface/api:latest
        ports:
        - containerPort: 8000
        env:
        - name: MODEL_PATH
          value: "/app/model"
        volumeMounts:
        - name: model-volume
          mountPath: /app/model
      volumes:
      - name: model-volume
        configMap:
          name: datasurface-model
```

## Interactive Documentation

The API server automatically generates interactive documentation using Swagger UI, available at:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **OpenAPI Schema**: `http://localhost:8000/openapi.json`

This provides a user-friendly interface for exploring the API, viewing schemas, and testing queries directly in the browser.
