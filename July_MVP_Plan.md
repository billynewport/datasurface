# July MVP Plan

## Overview

Based on your 2025/07/16 diary entry, this plan outlines the tasks needed to achieve your MVP goals. The MVP consists of a complete data pipeline with a producer database containing customer/address data, dual consumption patterns (live vs forensic), and a change simulator to demonstrate the system working end-to-end.

## Current State Assessment

✅ **Completed:**

- YellowDataPlatform with SQL snapshot ingestion working
- SnapshotMergeJob implemented for both live_only and forensic milestoning
- ✅ **NEW:** Complete MVP ecosystem model with customer/address schema
- ✅ **NEW:** Dual DataPlatform configuration (YellowLive + YellowForensic)
- ✅ **NEW:** Dual consumer DSG configuration (LiveDSG + ForensicDSG)
- ✅ **NEW:** DSG-to-DataPlatform assignment mapping implemented
- Command line view reconciler utility
- PostgreSQL 16 compatibility fixes
- Comprehensive test coverage for merge jobs

🎯 **Priority 1 Status: COMPLETED** - All core data model and configuration tasks done
🎯 **Priority 2 Status: 50% COMPLETE** - Producer database setup ✅ completed

## Priority-Ordered Tasks

### **Priority 1: Core Data Model Implementation**

#### Task 1.1: Enhance Ecosystem Model Schema

**Description:** Expand the current simple test model to support the customer/address use case described in your MVP goals. This involves creating a more realistic data schema that includes customer information and related address entities.

**Details:**

- Replace the current simple "people" dataset with a comprehensive customer schema
- Add customer table with fields: id, firstName, lastName, email, phone, dateCreated, lastUpdated
- Add address table with fields: id, customerId, addressType (billing/shipping), street1, street2, city, state, zipCode, country
- Ensure proper foreign key relationships between customer and address tables
- Update the test ecosystem in `eco.py` to reflect this new schema
- Maintain data classifications for all personally identifiable information

#### Task 1.2: Create Dual DataPlatform Configuration

**Description:** Implement two separate YellowDataPlatform instances to support both live and forensic processing requirements as specified in your MVP.

**Details:**

- Create "YellowLive" DataPlatform with LIVE_ONLY milestone strategy and 1-minute refresh
- Create "YellowForensic" DataPlatform with FORENSIC milestone strategy and 10-minute refresh
- Ensure both platforms use separate Kubernetes namespaces to avoid conflicts
- Configure appropriate credentials and connection strings for each platform
- Update the ecosystem model to include both platforms in the data_platforms list

#### Task 1.3: Implement Dual Consumer DSG Configuration

**Description:** Modify the consumer workspace to include two separate DatasetGroups, each configured for different consumption patterns and refresh frequencies.

**Details:**

- Create "LiveDataDSG" with live_only milestone strategy, 1-minute latency requirement
- Create "ForensicDataDSG" with forensic milestone strategy, 10-minute latency requirement
- Both DSGs should have DatasetSinks for all tables (customer and address)
- Configure appropriate platform_chooser settings for each DSG
- Ensure both DSGs reference the same source datastore but with different processing requirements

#### Actions ✅ **PRIORITY 1 COMPLETED**

I have created the new model in the 'mvp_model' directory. The platform assignment mapping file was also created.

**Progress Assessment:**

- ✅ **Task 1.1 COMPLETED:** Enhanced ecosystem model with customer/address schema in `src/tests/yellow_dp_tests/mvp_model/eco.py`
  - Customer table: id, firstName, lastName, dob, email, phone, primaryAddressId, billingAddressId
  - Address table: id, customerId, streetName, city, state, zipCode
  - Proper PII data classifications maintained
- ✅ **Task 1.2 COMPLETED:** Dual DataPlatform configuration implemented
  - "YellowLive" with LIVE_ONLY milestone strategy
  - "YellowForensic" with BATCH_MILESTONED (forensic) strategy
  - Both platforms properly configured with credentials and namespaces
- ✅ **Task 1.3 COMPLETED:** Dual consumer DSG configuration implemented
  - "LiveDSG" with live_only milestone strategy
  - "ForensicDSG" with forensic milestone strategy
  - Both DSGs include DatasetSinks for customers and addresses
- ✅ **Task 2.3 COMPLETED:** DSG-to-DataPlatform assignment mapping created
  - JSON mapping file at `src/tests/yellow_dp_tests/dsg_platform_mapping.json`
  - LiveDSG → YellowLive (PROVISIONED)
  - ForensicDSG → YellowForensic (PROVISIONED)

**Minor Optimizations for Later:**

- Consider adjusting cron triggers: 1-minute for live, 10-minute for forensic (currently both 10-min)
- Database name could be "producer_db" instead of "test_db" for clarity

### **Priority 2: Infrastructure and Database Setup**

#### Task 2.1: Data Producer Database Implementation ✅ **COMPLETED**

**Description:** ✅ Create and populate the actual producer database with the customer/address schema that will serve as the data source for ingestion. This database will be called 'customer_db'.

**Details:**

- ✅ Create PostgreSQL database named "customer_db"
- ✅ Implement DDL scripts for customer and address tables with proper constraints
- ✅ Create initial seed data with realistic customer and address information (5 customers, 8 addresses)
- ✅ Set up proper foreign key relationships between customers and addresses
- ✅ Create performance indexes on key columns
- ✅ Database accessible from localhost:5432 for ingestion pipeline
- ✅ Setup script created: `src/tests/yellow_dp_tests/mvp_model/setupcustomerdb.sql`

**Database Structure:**

- **customers table:** id, firstName, lastName, dob, email, phone, primaryAddressId, billingAddressId
- **addresses table:** id, customerId, streetName, city, state, zipCode
- **Sample data:** 5 customers across different US cities with realistic address relationships

#### Task 2.2: Consumer Database Setup

**Description:** Establish the consumer database that will host the views for accessing processed data from both live and forensic data platforms.

**Details:**

- Create PostgreSQL database named "consumer_db"
- Configure appropriate user permissions for view creation and data access
- Ensure connectivity from the YellowDataPlatform view reconciler
- Set up schema structure to support views from both live and forensic processing
- Document access patterns and query capabilities

#### Task 2.3: DSG-to-DataPlatform Assignment Mapping ✅ **COMPLETED**

**Description:** ✅ Implement and configure the DSG-to-DataPlatform assignment mapping system using the JSON-based approach you've designed.

**Details:**

- ✅ Create `dsg_platform_mapping.json` file in the ecosystem repository root
- ✅ Map "LiveDSG" to "YellowLive" DataPlatform with PROVISIONED status
- ✅ Map "ForensicDSG" to "YellowForensic" DataPlatform with PROVISIONED status
- 🔄 **REMAINING:** Ensure the ecosystem loader correctly reads and validates these mappings
- 🔄 **REMAINING:** Test that platform assignment logic respects the mapping configuration
- 🔄 **REMAINING:** Verify that changes to mappings are properly validated and secured

### **Priority 3: Data Change Simulation and Testing**

#### Task 3.1: Change Simulator Implementation

**Description:** Create a data change simulator that continuously modifies the producer database to demonstrate real-time ingestion and processing capabilities.

**Details:**

- Implement a Python script that runs continuously in the background
- Simulate realistic customer lifecycle: new customer creation, address updates, customer information changes
- Include operations: INSERT new customers, UPDATE existing customer details, INSERT/UPDATE/DELETE addresses
- Randomize timing and change patterns to simulate realistic business operations
- Ensure changes are substantial enough to trigger hash differences in forensic mode
- Log all changes for debugging and verification purposes
- Make the simulator configurable for change frequency and volume

#### Task 3.2: End-to-End Pipeline Validation

**Description:** Verify that the complete pipeline from producer database through ingestion, processing, and consumer views works correctly for both live and forensic scenarios.

**Details:**

- Test that SQL snapshot ingestion correctly captures schema and data from producer database
- Verify that live processing provides near-real-time data availability (within 1-minute SLA)
- Confirm that forensic processing maintains complete change history (within 10-minute SLA)
- Test that merge jobs handle concurrent changes during batch processing
- Validate that consumer views provide appropriate data access for both processing modes
- Ensure that view reconciler correctly creates and updates views after schema changes

### **Priority 4: Orchestration and Automation**

#### Task 4.1: Airflow DAG Generation and Testing

**Description:** Ensure that the Airflow DAG generation works correctly for both DataPlatforms and that the generated DAGs can be successfully deployed and executed.

**Details:**

- Generate ingestion DAGs for both YellowLive and YellowForensic platforms
- Verify DAG naming follows the <platformname,ingestionstreamname> convention
- Test that generated DAGs can be parsed and loaded by Airflow
- Ensure that credential management works correctly for both platforms
- Validate that job parameters and Docker container configuration are correct
- Test DAG execution with actual data to verify job orchestration

#### Task 4.2: MERGE Handler Integration Testing

**Description:** Validate that the GitHub-based MERGE Handler correctly triggers infrastructure updates when the ecosystem model changes.

**Details:**

- Test that commits to main branch trigger the MERGE Handler process
- Verify that DAG generation happens automatically when DSG-to-DataPlatform mappings change
- Ensure that infrastructure provisioning occurs in the correct sequence
- Test rollback scenarios when MERGE Handler operations fail
- Validate that concurrent changes to the ecosystem model are handled safely
- Document the complete CI/CD workflow from model change to operational pipeline

#### Task 4.3: View Reconciler Automation

**Description:** Integrate the view reconciler command-line utility into the automated workflow to ensure consumer views are always up-to-date.

**Details:**

- Configure the view reconciler to run automatically after merge operations complete
- Ensure view updates handle schema evolution gracefully
- Test that view reconciler can handle multiple concurrent DataPlatforms for the same workspace
- Validate that view naming conventions avoid conflicts between live and forensic views
- Implement monitoring and alerting for view reconciler failures
- Document view access patterns and query optimization guidelines

### **Priority 5: Monitoring and Documentation**

#### Task 5.1: Operational Monitoring Setup

**Description:** Implement basic monitoring and observability for the MVP pipeline to ensure reliability and provide debugging capabilities.

**Details:**

- Add logging and metrics to all ingestion and merge jobs
- Monitor ingestion latency and success rates for both platforms
- Track data freshness and consistency metrics
- Implement alerting for pipeline failures or SLA violations
- Create operational dashboards for system health visibility
- Document troubleshooting procedures for common failure scenarios

#### Task 5.2: MVP Documentation and Demonstration

**Description:** Create comprehensive documentation for the MVP system and prepare demonstration materials to showcase the complete pipeline.

**Details:**

- Update the ecosystem model documentation to reflect the customer/address use case
- Document the dual-platform architecture and its benefits
- Create step-by-step setup instructions for reproducing the MVP environment
- Prepare demonstration scripts showing live vs forensic data access patterns
- Document performance characteristics and scaling considerations
- Create troubleshooting guide for common operational issues

## Success Criteria

The MVP will be considered complete when:

1. **Data Flow:** Producer database → SQL ingestion → Merge processing → Consumer views works end-to-end
2. **Dual Processing:** Both live (1-minute) and forensic (10-minute) processing pipelines operate simultaneously
3. **Change Simulation:** Continuous data changes demonstrate real-time processing capabilities
4. **Automation:** Complete CI/CD pipeline from model changes to operational infrastructure
5. **Observability:** Basic monitoring and alerting provide operational visibility
6. **Documentation:** Comprehensive setup and operation guides enable system replication

## Estimated Timeline

- ✅ **Priority 1 tasks:** COMPLETED (Core model and configuration)
- **Priority 2 tasks:** 0.5-1 day (Infrastructure setup - consumer DB mapping validation remaining)
- **Priority 3 tasks:** 2-3 days (Simulation and testing)
- **Priority 4 tasks:** 3-4 days (Orchestration and automation)
- **Priority 5 tasks:** 2-3 days (Monitoring and documentation)

**Total remaining effort:** 7-10 days for complete MVP implementation

**Progress:** ~40% complete with core foundation and producer database ready
