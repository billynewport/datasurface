# MVP Dynamic DAG Factory Implementation Plan

## Overview

**Objective:** Transform YellowDataPlatform from static DAG file generation to a dynamic DAG factory approach that creates ingestion DAGs at runtime based on database configuration.

**🎯 IMPLEMENTATION STATUS: Foundation Complete - Ready for Factory Development**

**✅ EXISTING FOUNDATION:**
- Database schema implemented (`getAirflowDAGTable()`)
- Table creation integrated into bootstrap process (line 722)
- Template infrastructure established (Jinja2 environment)
- Factory template file already started (`yellow_platform_factory_dag.py.j2`)

**Current Architecture:**
- Static Jinja2 templates generate individual DAG files per ingestion stream
- Each ingestion stream gets its own physical DAG file (e.g., `yellowlive__Store1_ingestion.py`)
- DAG configuration is embedded in the generated Python files
- Changes require regenerating and redeploying DAG files

**Target Architecture:**
- Single factory DAG template generated during `generateBootstrapArtifacts`
- Factory DAG queries database table for ingestion stream configurations
- Ingestion DAGs created dynamically at Airflow runtime
- Configuration changes only require database updates (no DAG file regeneration)

**Benefits:**
- Simplified DAG management (single factory file vs. multiple static files)
- Dynamic configuration updates without file regeneration
- Better separation of configuration from code
- Easier scaling for large numbers of ingestion streams
- Reduced file system complexity in Airflow DAG folder

**💡 ACCELERATED TIMELINE:** Due to existing database infrastructure, estimated timeline reduced from 9-15 weeks to 7-12 weeks.

## Prerequisites

**Current Working Components:**
- ✅ Static DAG generation working in MVP (`yellowlive__Store1_ingestion.py`, `yellowforensic__Store1_ingestion.py`)
- ✅ YellowDataPlatform `generateBootstrapArtifacts` method functional
- ✅ Database infrastructure operational (PostgreSQL)
- ✅ Jinja2 template system established
- ✅ Kubernetes infrastructure and Airflow deployment working

**Dependencies:**
- PostgreSQL database for configuration storage
- Airflow 2.x dynamic DAG capabilities
- Existing Jinja2 template infrastructure

## Phase 1: Database Schema Design ✅ **COMPLETED**

### Task 1.1: Design Configuration Storage Schema ✅ **COMPLETED**

**Objective:** ✅ Define database schema for storing ingestion stream configurations.

**✅ EXISTING IMPLEMENTATION:**
The database schema has already been implemented in `YellowDataPlatform.getAirflowDAGTable()`:

```python
def getAirflowDAGTable(self) -> Table:
    """This constructs the sqlalchemy table for the batch metrics table. The key is either the data store name or the
    data store name and the dataset name."""
    t: Table = Table(self.getDAGTableName(), MetaData(),
                     Column("stream_key", sqlalchemy.String(length=255), primary_key=True),
                     Column("config_json", sqlalchemy.String(length=2048)),
                     Column("status", sqlalchemy.String(length=50)),
                     Column("created_at", TIMESTAMP()),
                     Column("updated_at", TIMESTAMP()))
    return t
```

**✅ EXISTING FEATURES:**
- ✅ Table name: `{platform_name}_airflow_dsg` (via `getDAGTableName()`)
- ✅ Primary key: `stream_key` (ingestion stream identifier)
- ✅ Configuration storage: `config_json` (2048 chars for JSON configuration)
- ✅ Status tracking: `status` column for DAG state management
- ✅ Audit columns: `created_at`, `updated_at` timestamps
- ✅ Automatic table creation: Called in `generateBootstrapArtifacts()` line 722

**✅ TABLE CREATION:**
The table is automatically created during bootstrap:
```python
# Line 722 in generateBootstrapArtifacts
mergeUser, mergePassword = self.credStore.getAsUserPassword(self.postgresCredential)
mergeEngine: Engine = createEngine(self.mergeStore, mergeUser, mergePassword)
createOrUpdateTable(mergeEngine, self.getAirflowDAGTable())
```

### Task 1.2: Map Current Template Variables to JSON Schema ⏳ **REMAINING**

**Objective:** Identify all variables currently used in `ingestion_stream_dag.py.j2` and define JSON structure.

**Analysis Required:**
- ✅ Database schema ready for configuration storage
- Extract all `{{ variable_name }}` references from current template (`ingestion_stream_dag.py.j2`)
- Document variable types and sources (ecosystem, platform, credentials) 
- Create JSON schema that encompasses all current template capabilities
- Ensure JSON fits within existing 2048 character `config_json` column limit

**Key Variable Categories from Current Implementation:**
- Platform configuration (name, namespace, credentials) - **Available in `createAirflowDAGs` method**
- Ingestion stream details (store name, datasets, ingestion type) - **Available in current DAG generation**
- Kubernetes configuration (image, secrets, volumes) - **Available in current templates**
- Scheduling configuration (cron, triggers) - **Available in current templates**
- Database connection details - **Available from platform configuration**

### Task 1.3: Design Configuration JSON Structure ⏳ **REMAINING**

**Objective:** Define standardized JSON schema that maps to existing template variables.

**Based on Existing `createAirflowDAGs` Method:**
Looking at the current `stream_context` in `createAirflowDAGs()` (line ~520), the JSON structure should include:

```json
{
  "stream_key": "Store1",
  "single_dataset": false,
  "datasets": ["customers", "addresses"],
  "store_name": "Store1", 
  "ingestion_type": "sql_snapshot",
  "namespace_name": "ns-kub-pg-test",
  "platform_name": "yellowlive",
  "original_platform_name": "YellowLive",
  "postgres_hostname": "pg-data",
  "postgres_database": "datasurface_merge",
  "postgres_port": 5432,
  "postgres_credential_secret_name": "postgres",
  "git_credential_secret_name": "git",
  "slack_credential_secret_name": "slack",
  "datasurface_docker_image": "datasurface/datasurface:latest",
  "git_repo_url": "https://github.com/billynewport/mvpmodel",
  "git_repo_branch": "main",
  "source_credential_secret_name": "postgres"
}
```

**Size Validation:** Current JSON structure fits well within 2048 character limit of `config_json` column.

## Phase 2: Factory DAG Template Development

### Task 2.1: Create Dynamic DAG Factory Template ⚡ **IN PROGRESS**

**Objective:** Design Jinja2 template for the factory DAG that generates ingestion DAGs dynamically.

**🔍 EXISTING WORK DETECTED:**
According to git status, there's already an untracked file:
- `src/datasurface/platforms/yellow/templates/jinja/yellow_platform_factory_dag.py.j2`

**Template Requirements:**
- ✅ Single template file location identified: `yellow_platform_factory_dag.py.j2`
- Database connection configuration using platform credentials
- Dynamic DAG creation logic using Airflow's DAG factory patterns  
- Error handling for database connectivity issues
- Logging for configuration loading and DAG creation

**Key Template Features:**
- Database query function to load configurations from existing `{platform_name}_airflow_dsg` table
- DAG creation function that transforms JSON config to DAG objects
- Error handling and logging for debugging
- Configuration validation before DAG creation
- Integration with existing `getAirflowDAGTable()` schema

### Task 2.2: Implement Dynamic DAG Creation Logic

**Objective:** Design the core logic for creating DAGs from database configuration.

**Components Needed:**
- Database connection function using platform credentials
- Configuration loader function with error handling
- DAG factory function that creates DAG objects from JSON config
- Validation function to ensure configuration completeness
- Caching mechanism to avoid excessive database queries

**DAG Creation Pattern:**
```python
def create_ingestion_dag(config_json):
    """Create a DAG object from JSON configuration"""
    # Parse JSON configuration
    # Validate required fields
    # Create DAG with dynamic configuration
    # Return DAG object for Airflow registration
```

### Task 2.3: Handle Configuration-to-DAG Transformation

**Objective:** Implement logic to transform JSON configuration into DAG task definitions.

**Transformation Requirements:**
- Convert JSON credentials to KubernetesPodOperator environment variables
- Transform scheduling configuration to Airflow schedule parameters
- Map Kubernetes configuration to pod operator settings
- Handle dynamic task naming and dependencies
- Preserve all functionality from current static DAGs

## Phase 3: Bootstrap Integration

### Task 3.1: Modify generateBootstrapArtifacts Method

**Objective:** Update `YellowDataPlatform.generateBootstrapArtifacts` to generate factory DAG instead of individual DAGs.

**Changes Required:**
- Replace calls to `createAirflowDAG()` for each ingestion stream
- Add call to `createFactoryDAG()` method (single factory per platform)
- Ensure factory DAG gets platform-specific configuration
- Maintain existing bootstrap artifact generation for other components

**Method Signature:**
```python
def createFactoryDAG(self) -> str:
    """Generate dynamic DAG factory for this platform"""
    # Use yellow_platform_factory_dag.py.j2 template
    # Include platform-specific database configuration
    # Return factory DAG content as string
```

### Task 3.2: Database Configuration Population

**Objective:** Implement logic to populate database configuration table during bootstrap.

**Population Strategy:**
- Extract current ingestion stream configurations from ecosystem model
- Transform to JSON format matching schema design
- Insert/update configuration records in database table
- Handle configuration versioning and updates
- Provide migration path from static to dynamic approach

**Integration Points:**
- Call during `generateBootstrapArtifacts` after factory DAG creation
- Use existing database connection infrastructure
- Leverage current ecosystem model parsing logic
- Maintain transaction consistency for multiple streams

### Task 3.3: Template Context Enhancement

**Objective:** Enhance template context for factory DAG generation.

**Context Requirements:**
- Platform database connection details
- Credentials configuration for database access
- Namespace and Kubernetes configuration
- Platform name and identification
- Error handling and logging configuration

## Phase 4: Migration Strategy

### Task 4.1: Backward Compatibility Planning

**Objective:** Design migration approach that maintains system functionality during transition.

**Migration Phases:**
1. **Parallel Operation**: Factory DAG generates same DAGs as static approach
2. **Validation Phase**: Compare dynamic vs static DAG functionality
3. **Cutover Phase**: Disable static DAG generation, enable factory only
4. **Cleanup Phase**: Remove static DAG generation code

**Compatibility Requirements:**
- Factory DAGs must produce identical functionality to current static DAGs
- Configuration JSON must capture all current template variables
- Database population must match current ecosystem model parsing
- Error handling must maintain current behavior

### Task 4.2: Configuration Migration Utilities

**Objective:** Create utilities to migrate existing static configurations to database format.

**Utilities Needed:**
- Configuration extractor: Parse existing generated DAGs to extract configuration
- Database populator: Load extracted configurations into database tables
- Validation tool: Compare static vs dynamic DAG configurations
- Rollback mechanism: Restore static DAGs if needed

### Task 4.3: Testing and Validation Framework

**Objective:** Establish comprehensive testing for dynamic DAG functionality.

**Testing Components:**
- Unit tests for configuration JSON schema validation
- Integration tests for database configuration loading
- End-to-end tests comparing static vs dynamic DAG behavior
- Performance tests for configuration loading overhead
- Error handling tests for database connectivity issues

## Phase 5: Database Infrastructure

### Task 5.1: Database Table Creation

**Objective:** Implement database schema creation during platform initialization.

**Implementation Requirements:**
- Add table creation to platform initialization sequence
- Handle existing table detection and schema migration
- Implement proper indexing for performance
- Add database connection validation
- Include table cleanup for platform decommissioning

### Task 5.2: Configuration Management Interface

**Objective:** Provide interface for managing ingestion stream configurations.

**Interface Options:**
- Command-line utility for configuration CRUD operations
- Database direct access documentation
- Web interface integration (future enhancement)
- Configuration validation and testing tools
- Bulk configuration import/export capabilities

### Task 5.3: Database Connection Management

**Objective:** Implement robust database connectivity for factory DAGs.

**Connection Requirements:**
- Use existing platform credential infrastructure
- Implement connection pooling and retry logic
- Handle database unavailability gracefully
- Provide configuration caching to reduce database load
- Include proper connection cleanup and resource management

## Phase 6: Performance and Monitoring

### Task 6.1: Configuration Loading Optimization

**Objective:** Optimize database queries and caching for factory DAG performance.

**Optimization Strategies:**
- Implement configuration caching with TTL
- Optimize database queries for configuration loading
- Add configuration change detection mechanisms
- Implement lazy loading for unused configurations
- Monitor and measure configuration loading performance

### Task 6.2: Factory DAG Monitoring

**Objective:** Add monitoring and observability for dynamic DAG creation.

**Monitoring Components:**
- Configuration loading metrics and timing
- DAG creation success/failure rates
- Database connectivity monitoring
- Configuration change tracking and auditing
- Error rate monitoring and alerting

### Task 6.3: Debugging and Troubleshooting Tools

**Objective:** Provide tools for debugging dynamic DAG issues.

**Debugging Features:**
- Configuration validation and testing utilities
- DAG creation simulation and testing
- Database configuration inspection tools
- Error logging and trace collection
- Configuration diff and change tracking

## Phase 7: Documentation and Training

### Task 7.1: Architecture Documentation

**Objective:** Document the dynamic DAG factory architecture and design decisions.

**Documentation Requirements:**
- Architecture overview and design rationale
- Database schema documentation and examples
- Configuration JSON schema reference
- Migration guide from static to dynamic approach
- Troubleshooting guide for common issues

### Task 7.2: Operational Procedures

**Objective:** Create operational procedures for managing dynamic DAG configurations.

**Procedure Documentation:**
- Configuration update procedures
- Database maintenance and monitoring
- Emergency rollback procedures
- Performance tuning guidelines
- Backup and recovery procedures

### Task 7.3: Developer Guidelines

**Objective:** Provide guidelines for developers working with dynamic DAG system.

**Developer Resources:**
- Configuration JSON schema and validation
- Testing procedures for configuration changes
- Development environment setup
- Debugging techniques and tools
- Best practices for configuration management

## Success Criteria

### Functional Requirements
- ✅ Factory DAG generates identical functionality to current static DAGs
- ✅ Database configuration loading works reliably
- ✅ Configuration updates don't require DAG file regeneration
- ✅ All existing ingestion streams work with dynamic approach
- ✅ Performance is comparable to static DAG approach

### Non-Functional Requirements
- ✅ Configuration loading adds minimal overhead (<1 second per DAG)
- ✅ Database connectivity failures are handled gracefully
- ✅ System maintains backward compatibility during migration
- ✅ Error handling and logging provide adequate debugging information
- ✅ Configuration management is intuitive for operators

### Operational Requirements
- ✅ Configuration changes can be made without Airflow restart
- ✅ Database schema supports platform scaling (multiple platforms)
- ✅ Migration from static to dynamic approach is reversible
- ✅ Monitoring and alerting provide operational visibility
- ✅ Documentation supports operational procedures

## Risk Mitigation

### Technical Risks
- **Database Dependency**: Factory DAGs depend on database availability
  - *Mitigation*: Implement configuration caching and graceful degradation
- **Performance Impact**: Database queries may slow DAG loading
  - *Mitigation*: Optimize queries, implement caching, monitor performance
- **Configuration Complexity**: JSON configuration may be error-prone
  - *Mitigation*: Implement validation, provide tools, create clear documentation

### Operational Risks
- **Migration Complexity**: Moving from static to dynamic approach is complex
  - *Mitigation*: Phased migration, extensive testing, rollback procedures
- **Debugging Difficulty**: Dynamic DAGs may be harder to debug
  - *Mitigation*: Enhanced logging, debugging tools, clear error messages
- **Configuration Management**: Managing configurations in database vs. files
  - *Mitigation*: Configuration management tools, version control integration

## Timeline Estimate

### Phase 1: Database Design ✅ **COMPLETED** (0 weeks remaining)
- ✅ Schema design and validation (DONE - existing `getAirflowDAGTable()`)
- ⏳ JSON configuration structure definition (0.5 weeks)
- ⏳ Template variable mapping (0.5 weeks)

### Phase 2: Factory Template (2-3 weeks)
- Factory DAG template development
- Dynamic DAG creation logic
- Configuration transformation implementation

### Phase 3: Bootstrap Integration (1-2 weeks)
- generateBootstrapArtifacts modification (database table already created)
- Database population logic
- Template context enhancement

### Phase 4: Migration Strategy (2-3 weeks)
- Backward compatibility implementation
- Migration utilities development
- Testing framework creation

### Phase 5: Database Infrastructure (0.5-1 weeks)
- ✅ Table creation and management (DONE - existing implementation)
- Connection handling (existing patterns available)
- Configuration interface

### Phase 6: Performance and Monitoring (1-2 weeks)
- Optimization implementation
- Monitoring setup
- Debugging tools

### Phase 7: Documentation (1 week)
- Architecture documentation
- Operational procedures
- Developer guidelines

**Total Estimated Effort: 7-12 weeks** (reduced from 9-15 weeks due to existing database implementation)

## Dependencies and Prerequisites

### External Dependencies
- PostgreSQL database infrastructure
- Airflow 2.x dynamic DAG support
- Kubernetes infrastructure operational
- Existing YellowDataPlatform functionality

### Internal Dependencies
- Current Jinja2 template system
- Existing credential management
- Database connection infrastructure
- Bootstrap artifact generation pipeline

### Team Dependencies
- Platform team for architecture decisions
- Operations team for migration planning
- Development team for implementation
- Testing team for validation

---

**Document Status:** Draft - Ready for Review
**Last Updated:** 2025-07-18
**Dependencies:** July_MVP_Plan.md, MVP_Kubernetes_Infrastructure_Setup.md
**Next Steps:** Review and prioritize phases, assign implementation team, begin Phase 1 database design 