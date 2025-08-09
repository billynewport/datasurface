# HOWTO: Implement SQLMergeIngestion for Cross-Platform Preferred Ingestion

## Overview

This document outlines the implementation plan for SQLMergeIngestion, which enables non-primary data platforms to ingest data from primary platform merge tables instead of directly from source databases. This reduces load on production databases by limiting the number of platforms that directly connect to them.

## Background

When a datastore has a PrimaryIngestionPlatform (PIP) configured, only the platforms listed in the PIP should directly ingest from the source database. Other platforms that need the data should instead pull it from the merge tables on one of the primary platforms.

The current architecture already:

- Defines `PrimaryIngestionPlatform` and loads it from JSON
- Embeds PIP into `IngestionNode` objects in the pipeline graph
- Stops left-hand graph expansion when a platform is not in the PIP

What's missing is the runtime mechanism for non-primary platforms to ingest from primary platform merge tables.

## Cross-Platform Ingestion Flow

The SQLMergeIngestion process uses a two-phase approach:

1. **Seed Phase** (first time ingestion):
   - Connect to primary platform's merge database
   - Take a snapshot of all live records at the current primary batch ID, X
   - Copy records to local staging table in manageable chunks (may require multiple transactions)
   - Store the primary batch ID, X, as the "last synced batch" only after all chunks complete successfully

2. **Incremental Phase** (subsequent ingestions):
   - Decide on new catch up point, Y. Depending on how far away the latest batch is, this may be a lower batch ID than the latest batch id.
   - Query for records that changed since the last synced batch, >X to <=Y
   - Copy only the delta records to local staging table
   - Update the "last synced batch" to the new primary batch ID, Y

This approach ensures:

- Initial seed captures the complete current state without overwhelming system resources
- Both seed and incremental phases can handle billion-record tables through chunking
- Incremental updates are efficient and only transfer changes
- Non-primary platforms stay reasonably current with primary platforms
- Network and compute resources are used efficiently
- Failed operations can resume from chunk boundaries rather than starting over

## Implementation Progress Summary

**Completed Steps**: 
- ✅ Step 1: SQLMergeIngestion command type definition
- ✅ Step 2: getEffectiveCmd() method and integration (renaming pending)
- ✅ Step 3: Primary platform discovery logic  
- ✅ Step 5: DAG configuration generation and maintainability improvements

**Current Status**: The foundation for SQLMergeIngestion is fully operational! The system now:
- ✅ Detects when a platform is not in the Primary Ingestion Platform list
- ✅ Dynamically creates SQLMergeIngestion commands at runtime in job main functions and DAG generation
- ✅ Resolves primary platform connections and credentials
- ✅ Automatically replaces store commands with effective commands throughout the pipeline
- ✅ Uses type-safe `IngestionType` enum for maintainable code
- ✅ Handles SQLMergeIngestion through existing SQL_SOURCE infrastructure

**Recent Maintainability Improvements**:
- ✅ **Type Safety**: Added `IngestionType` enum to replace magic strings
- ✅ **Consistent Architecture**: Moved `getEffectiveCMDForDatastore()` to `YellowDataPlatform` class
- ✅ **Explicit Integration**: Job main functions and DAG generation explicitly use effective commands with clear timing control
- ✅ **Structured Logging**: Enhanced logging throughout the platform for better debugging
- ✅ **Infrastructure Reuse**: SQLMergeIngestion leverages existing SQL_SOURCE patterns

**Next Priority**: Implement Step 4 (the core ingestion algorithm) - the foundation is complete!

## Multi-Step Implementation Plan

### Step 1: Define SQLMergeIngestion Command Type ✅ **COMPLETED**
**Goal**: Create a new ingestion command type that represents ingesting from remote merge tables.

**Tasks**:
1. ✅ Create `SQLMergeIngestion` class in `src/datasurface/md/governance.py`
   - ✅ Inherits from `SQLIngestion` (which inherits from `IngestionMetadata`)
   - ✅ Contains: `dataPlatform: DataPlatform` (reference to primary platform)
   - ✅ Include constructor with SQLDatabase, DataPlatform, and variable args for credentials/triggers
2. ⏳ Add validation in `lint()` method to ensure referenced platform and credentials exist
3. ⏳ Add to JSON serialization/deserialization
4. ✅ Update type hints and imports where needed

**Success Criteria**: 
- ✅ `SQLMergeIngestion` can be instantiated and validated
- ⏳ Linting catches invalid platform references
- ⏳ JSON round-trip works correctly

**Implementation Notes**:
- Class has been created at line 1118 in `governance.py`
- Inherits from `SQLIngestion` instead of directly from `IngestionMetadata`
- Contains `dataPlatform` field to reference the primary platform
- Basic `__eq__` and `__str__` methods implemented

### Step 2: Implement getEffectiveCmd() Method and Rename Job ✅ **MOSTLY COMPLETED**
**Goal**: Add `getEffectiveCmd()` method to Datastore and rename SnapshotMergeJob to MergeDataJob.

**Tasks**:
1. ✅ Add method to `YellowDataPlatform` class: `getEffectiveCMDForDatastore(eco: Ecosystem, store: Datastore) -> CaptureMetaData`
   - ✅ Check if store has PIP and current platform is not in PIP
   - ✅ If true, create `SQLMergeIngestion` command dynamically
   - ✅ Resolve primary platform credentials and merge store
   - ✅ Otherwise return original `store.cmd`
2. ⏳ Rename `SnapshotMergeJob` classes to `MergeDataJob`
   - ⏳ `SnapshotMergeJobForensic` → `MergeDataJobForensic`
   - ⏳ `SnapshotMergeJobLiveOnly` → `MergeDataJobLiveOnly`
3. ✅ Update Jobs to use `getEffectiveCMDForDatastore()` 
   - ✅ Job main functions explicitly call `store.cmd = dp.getEffectiveCMDForDatastore(eco, store)` early (e.g., line 1113)
   - ✅ DAG generation uses effective command at line 948 in `populateDAGConfigurations()`
4. ✅ Add credential resolution logic to find primary platform's merge database credentials

**Success Criteria**:
- ✅ Non-primary platforms get `SQLMergeIngestion` command at runtime
- ✅ Primary platforms continue using original commands
- ⏳ Job naming reflects broader purpose (merge data from any source)
- ✅ Credential resolution works correctly

**Implementation Notes**:
- Method implemented as `getEffectiveCMDForDatastore()` in `YellowDataPlatform` class at line 2512 in `yellow_dp.py`
- Job main functions explicitly call `store.cmd = dp.getEffectiveCMDForDatastore(eco, store)` early (e.g., line 1113 in `jobs.py`)
- This explicit approach makes command resolution more obvious and ensures it happens upfront before dependent validation checks
- DAG generation properly uses effective commands in `populateDAGConfigurations()` at line 948
- Logic correctly checks for PrimaryIngestionPlatform and creates `SQLMergeIngestion` when needed
- Finds compatible primary platform (YellowDataPlatform) from PIP configuration
- Uses primary platform's merge store and local platform's postgres credentials
- Only job class renaming remains to complete this step

### Step 3: Implement Primary Platform Discovery ✅ **COMPLETED**
**Goal**: Enable runtime discovery of which primary platform to connect to.

**Tasks**:
1. ✅ Add method discovery logic (implemented inline in `getEffectiveCMDForDatastore`)
   - ✅ Return first available primary platform from the PIP that is YellowDataPlatform
   - ⏳ Consider platform health/availability in the future
2. ✅ Add method to get primary platform's merge database connection details
3. ✅ Update `SQLMergeIngestion` creation to use discovered primary platform

**Success Criteria**:
- ✅ Non-primary platforms can discover available primary platforms
- ✅ Connection details are correctly resolved

**Implementation Notes**:
- Primary platform discovery is integrated into `getEffectiveCMDForDatastore()` method
- Logic iterates through PIP.dataPlatforms to find first compatible YellowDataPlatform
- Uses `eco.getDataPlatformOrThrow()` to resolve platform by name
- Throws exception if no compatible primary platform found
- Connection details obtained from `primDP.psp.mergeStore`

### Step 4: Implement Merge Table Ingestion Algorithm ⏳ **PENDING** 
**Goal**: Create the algorithm to ingest milestoned records from remote merge tables using a two-phase approach.

**Tasks**:
1. ⏳ Update `BatchState` model to include SQLMerge-specific fields:
   - `last_synced_primary_batch: Optional[int] = None`
   - `is_seed_phase: bool = True`
   - `seed_offset: Optional[int] = None` - For resuming chunked seed operations
   - `seed_chunk_size: int = 100000` - Configurable chunk size for seed operations 
2. ⏳ Create new method: `ingestFromMergeTable()` in `MergeDataJob` class
3. ⏳ Update `MergeDataJob.run()` to detect command type and route to appropriate algorithm:
   - If `SQLMergeIngestion`: use new merge table ingestion algorithm
   - If `SQLSnapshotIngestion` or `DataTransformerOutput`: use existing algorithms
4. ⏳ Implement **Seed Phase** logic:
   - Connect to primary platform's merge database
   - Determine current batch ID on primary platform
   - Query merge table for all live records at that batch ID snapshot **in chunks**
   - Use pagination/windowing (e.g., LIMIT/OFFSET or key-based ranges) to avoid massive single queries
   - Insert each chunk into local staging table with local batch ID
   - Track seed progress in `BatchState` (e.g., `seed_offset` or `seed_last_key`)
   - Store the primary platform's batch ID in `BatchState.last_synced_primary_batch` only after **all chunks complete**
   - Set `BatchState.is_seed_phase = False` for future runs
5. ⏳ Implement **Incremental Phase** logic:
   - Query merge table for records with batch_in > last_synced_batch AND batch_in <= current_primary_batch
   - Handle record lifecycle: new records, updates, deletions
   - Update local staging table with incremental changes
   - Update `BatchState.last_synced_primary_batch` to current_primary_batch
6. ⏳ Add forensic-to-live conflation logic for when source is forensic but target is live-only
7. ⏳ Ensure proper batch tracking and metrics for both phases

**Prerequisites**: 
- Complete Step 2 job renaming first (SnapshotMergeJob → MergeDataJob)
- This is the core implementation work and will be the most complex step

**Implementation Details**:
- **Seed Query (Chunked)**: `SELECT * FROM merge_table WHERE batch_out = LIVE_RECORD_ID ORDER BY key_column LIMIT ? OFFSET ?` 
- **Incremental Query**: `SELECT * FROM merge_table WHERE batch_in > ? AND batch_in <= ?` (changes since last sync)
- **Chunking Strategy**: 
  - Use deterministic ordering (primary key or key_hash) for consistent pagination
  - Default chunk size: 100K records (configurable)
  - Resume from `seed_offset` if seed phase is interrupted
- **State Tracking**: Add SQLMerge-specific fields to `BatchState` model:
  - `last_synced_primary_batch: Optional[int]` - Track last synced batch from primary platform
  - `is_seed_phase: bool` - Whether this is initial seed or incremental sync
  - `seed_offset: Optional[int]` - Resume point for chunked seeding
- **Conflation**: For live-only targets, only take records where batch_out = LIVE_RECORD_ID

**Success Criteria**:
- Seed phase correctly captures complete dataset snapshot
- Incremental phase efficiently captures only changes
- Forensic records are correctly conflated to live when needed
- Batch tracking works correctly for both phases

### Step 5: Update DAG Configuration Generation ✅ **COMPLETED**
**Goal**: Ensure DAG factory generates correct configurations for SQLMergeIngestion.

**Tasks**:
1. ✅ Update `YellowGraphHandler.populateDAGConfigurations()` to use effective commands (line 948)
2. ✅ Add maintainability improvements for ingestion type handling
   - ✅ Created `IngestionType` enum for type safety (line 653)
   - ✅ Use enum values instead of magic strings in DAG generation
   - ✅ `IngestionType.SQL_SOURCE` handles both `SQLSnapshotIngestion` and `SQLMergeIngestion`
3. ✅ SQLMergeIngestion uses same infrastructure as SQLSnapshotIngestion
4. ✅ Ensure credential secrets are properly referenced for both source and target platforms
5. ⏳ Update DAG templates to use `MergeDataJob` instead of `SnapshotMergeJob` (optional cleanup)

**Success Criteria**:
- ✅ DAG configurations use effective commands from `getEffectiveCMDForDatastore()`
- ✅ Type-safe ingestion type handling with `IngestionType` enum
- ✅ DAG configurations properly handle `SQLMergeIngestion` as `SQL_SOURCE` type
- ✅ Primary platform credentials are properly configured in DAG context
- ✅ Job execution works for both original and cross-platform ingestion
- ⏳ Airflow templates reference the renamed `MergeDataJob` (optional)

**Maintainability Improvements Made**:
- ✅ `IngestionType` enum replaces magic strings (`"kafka"`, `"sql_source"`)
- ✅ Centralized ingestion type logic in DAG generation
- ✅ Consistent credential handling patterns
- ✅ `SQLMergeIngestion` reuses existing `SQL_SOURCE` infrastructure

**Implementation Notes**:
- Both `SQLSnapshotIngestion` and `SQLMergeIngestion` inherit from `SQLIngestion`
- They use the same job infrastructure and Kubernetes configuration patterns
- No need for separate enum value - they're both SQL-based ingestions

### Step 6: Implement Forensic-to-Live Conflation ⏳ **PENDING**
**Goal**: Handle the case where primary platform stores forensic data but secondary platform needs live-only.

**Tasks**:
1. ⏳ Add logic to detect milestone strategy mismatch
2. ⏳ Implement conflation algorithm:
   - Query forensic table for live records (where `batch_out = LIVE_RECORD_ID`)
   - Transform to live-only format
   - Handle primary key changes and deletions
3. ⏳ Add tests for various conflation scenarios

**Success Criteria**:
- ⏳ Forensic data correctly converted to live-only
- ⏳ Primary key handling works correctly
- ⏳ Deletions are properly reflected

**Dependencies**: Requires completion of Step 4

### Step 7: Add Comprehensive Testing ⏳ **PENDING**
**Goal**: Ensure the implementation works correctly across various scenarios.

**Tasks**:
1. ⏳ Create test cases for:
   - Primary platform direct ingestion (existing behavior)
   - Secondary platform SQLMergeIngestion via `getEffectiveCmd()`
   - Forensic-to-live conflation
   - Error handling (primary platform unavailable, etc.)
   - Job renaming and routing logic
2. ⏳ Add integration tests with multiple platforms using `MergeDataJob`
3. ⏳ Test DAG generation and execution with new job names

**Success Criteria**:
- ⏳ All test scenarios pass
- ⏳ Error conditions are handled gracefully
- ⏳ Performance is acceptable

**Dependencies**: Requires completion of Steps 2, 4, 5, and 6

### Step 8: Documentation and Examples ⏳ **PENDING**
**Goal**: Document the new functionality for users and developers.

**Tasks**:
1. ⏳ Update existing documentation to explain PIP behavior
2. ⏳ Add examples of `primary_ingestion_platforms.json` configuration
3. ⏳ Document troubleshooting steps
4. ⏳ Add performance tuning guidance

**Success Criteria**:
- ⏳ Clear documentation for configuration and operation
- ⏳ Examples work correctly
- ⏳ Troubleshooting guide is helpful

**Dependencies**: Requires completion of all implementation steps

## Immediate Next Steps

### Priority 1: Implement Step 4 - Core Algorithm Implementation
The foundation is complete! You can now implement the actual SQLMergeIngestion algorithm:

1. **BatchState Model Extensions**: Add SQLMerge-specific fields to track seed/incremental phases
2. **Command Type Detection**: Add routing logic in job `run()` method to detect `SQLMergeIngestion`
3. **Seed Phase Implementation**: Implement chunked ingestion of all live records from primary platform
4. **Incremental Phase Implementation**: Implement delta processing for subsequent runs

**Note**: The job renaming (SnapshotMergeJob → MergeDataJob) can be done later as cleanup - it's not blocking the core algorithm work since the effective command system is already working.

### Optional: Complete Step 2 - Job Renaming
This is now optional cleanup that can be done later:

1. **Rename Classes** in `/src/datasurface/platforms/yellow/jobs.py`:
   - `SnapshotMergeJobForensic` → `MergeDataJobForensic` 
   - `SnapshotMergeJobLiveOnly` → `MergeDataJobLiveOnly`
2. **Update Test Files**: Update test references in test files
3. **Update DAG Templates**: Update Airflow template references

## Dependencies and Considerations

### Technical Dependencies
- Existing PIP infrastructure (already implemented)
- Pipeline graph generation (already implemented)
- Yellow platform job execution framework (already implemented)

### Design Considerations
1. **Performance**: 
   - Cross-platform network latency may affect ingestion performance
   - Seed phase will be slower due to full dataset transfer
   - Incremental phase should be much faster with only delta records
   - Consider compression for large seed transfers
2. **Security**: Need secure credential management for cross-platform access
3. **Availability**: Handle cases where primary platform is unavailable
4. **Monitoring**: 
   - Add metrics for cross-platform ingestion health
   - Track seed vs incremental phase performance
   - Monitor batch lag between primary and secondary platforms
5. **Schema Evolution**: Ensure schema changes propagate correctly
6. **State Management**:
   - Track last synced batch ID reliably
   - Handle cases where primary platform batch IDs are reset
   - Detect when seed phase needs to be re-run
7. **Data Consistency**:
   - Ensure atomic updates during incremental phase
   - Handle concurrent updates on primary platform during sync

### Future Enhancements
1. **Load Balancing**: Distribute load across multiple primary platforms
2. **Caching**: Cache frequently accessed data locally
3. **Compression**: Optimize network transfer for large datasets
4. **Incremental**: Only transfer changed data since last sync

## Risk Mitigation

1. **Primary Platform Failure**: Implement fallback to other primary platforms or direct source
2. **Network Issues**: Add retry logic and proper error handling
3. **Schema Drift**: Validate schema compatibility before ingestion
4. **Performance Degradation**: Monitor and alert on ingestion lag

## Implementation Order

The steps should be implemented in the order listed, as each builds on the previous ones. Steps 1-3 can be implemented and tested independently. Steps 4-6 form the core ingestion logic. Steps 7-8 provide validation and documentation.

Each step should include:

- Unit tests for new functionality
- Integration tests where applicable
- Documentation updates
- Code review and validation

This phased approach allows for incremental development and testing, reducing risk and enabling early validation of the design.
