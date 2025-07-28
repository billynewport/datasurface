# Data Change Simulator

This CLI tool continuously simulates realistic customer lifecycle changes in the producer database to demonstrate real-time ingestion and processing capabilities for the MVP.

## Prerequisites

1. **Database Setup:** Ensure the `customer_db` database is created and populated with initial data using:
   ```bash
   psql -h localhost -p 5432 -U postgres -f src/tests/yellow_dp_tests/mvp_model/setupcustomerdb.sql
   ```

2. **Python Dependencies:** Install psycopg2 if not already available:
   ```bash
   pip install psycopg2-binary
   ```

## Usage

### Basic Usage

Run with default settings (connects to localhost:5432/customer_db, changes every 5-30 seconds):

```bash
cd src/tests
python data_change_simulator.py
```

### Custom Configuration

```bash
python data_change_simulator.py \
    --host localhost \
    --port 5432 \
    --database customer_db \
    --user postgres \
    --password postgres \
    --min-interval 10 \
    --max-interval 60 \
    --verbose
```

### Testing Mode

For testing purposes, use `--max-changes` to run a limited number of changes:

```bash
# Run exactly 20 changes then stop
python data_change_simulator.py --max-changes 20 --verbose

# Quick test with faster changes
python data_change_simulator.py --max-changes 5 --min-interval 1 --max-interval 3
```

### Command Line Options

- `--host HOST`: Database host (default: localhost)
- `--port PORT`: Database port (default: 5432)
- `--database DATABASE`: Database name (default: customer_db)
- `--user USER`: Database user (default: postgres)
- `--password PASSWORD`: Database password (default: postgres)
- `--min-interval MIN`: Minimum seconds between changes (default: 5)
- `--max-interval MAX`: Maximum seconds between changes (default: 30)
- `--max-changes MAX`: Maximum number of changes before stopping (default: unlimited)
- `--verbose`: Enable verbose logging for debugging
- `--help`: Show help message

## What It Does

The simulator performs these realistic operations with weighted probability:

1. **CREATE new customers** (15% weight)
   - Generates realistic names, emails, phone numbers, dates of birth
   - Creates a primary address for each new customer
   - Uses the address as both primary and billing address initially

2. **UPDATE customer information** (25% weight)
   - Updates email addresses and/or phone numbers
   - Uses realistic email formats and phone number patterns

3. **ADD new addresses** (20% weight)
   - Creates additional addresses for existing customers
   - Sometimes sets new address as the billing address (30% chance)

4. **UPDATE existing addresses** (30% weight)
   - Changes street addresses, city/state combinations, or ZIP codes
   - Simulates customers moving or correcting address information

5. **DELETE addresses** (10% weight)
   - Removes non-primary, non-billing addresses only
   - Only deletes from customers who have multiple addresses
   - Preserves referential integrity

## Sample Output

```
2024-07-20 10:15:23 - INFO - 🚀 Starting Data Change Simulator
2024-07-20 10:15:23 - INFO -    Database: customer_db at localhost:5432
2024-07-20 10:15:23 - INFO -    Change interval: 5-30 seconds
2024-07-20 10:15:23 - INFO -    Press Ctrl+C to stop
2024-07-20 10:15:23 - INFO - Connected to database customer_db at localhost:5432
2024-07-20 10:15:23 - INFO - ✅ CREATED customer CUST_1721471723456_789 (Alice Johnson) with address ADDR_1721471723457_123
2024-07-20 10:15:35 - INFO - 📝 UPDATED customer CUST_1721471723456_789 (email)
2024-07-20 10:15:48 - INFO - 🏠 ADDED address ADDR_1721471748234_567 for customer CUST_1721471723456_789
2024-07-20 10:16:12 - INFO - 🏠 UPDATED address ADDR_1721471748234_567 (moved to Chicago, IL)
```

## Testing the Simulator

A test script is provided to verify the simulator works correctly:

```bash
# Run comprehensive test
python test_data_simulator.py
```

This test script will:
1. Record the initial database state
2. Run the simulator for 8 changes
3. Verify that changes were actually made to the database
4. Report success or failure

Sample test output:
```
🚀 Testing Data Change Simulator
1. Recording initial database state...
   Initial customers: 6
   Initial addresses: 8
2. Running data change simulator...
   ✅ Simulator completed successfully
3. Checking final database state...
   Final customers: 7
   Final addresses: 9
4. Verifying changes...
   ✅ Customer count changed: 6 → 7
   ✅ Address count changed: 8 → 9
🎉 TEST PASSED: Data Change Simulator is working correctly!
```

## Stopping the Simulator

Press **Ctrl+C** to stop the simulator gracefully. It will:
- Complete any current database operation
- Close the database connection cleanly
- Display a stop message

## Docker Usage

To run in a Docker container (for future containerized deployments):

```bash
# Build image with datasurface and psycopg2
docker run -it --network host \
    -e DATABASE_HOST=localhost \
    -e DATABASE_PORT=5432 \
    -e DATABASE_NAME=customer_db \
    -e DATABASE_USER=postgres \
    -e DATABASE_PASSWORD=postgres \
    datasurface:latest \
    python src/tests/data_change_simulator.py --verbose
```

## Purpose in MVP

This simulator supports **Task 3.1** of the MVP plan by:

1. **Demonstrating Real-Time Processing:** Continuous changes trigger the ingestion pipeline
2. **Testing Both Processing Modes:** Changes are visible in both live (1-minute) and forensic (10-minute) processing
3. **Forensic Validation:** Substantial changes ensure hash differences are detectable
4. **System Load Testing:** Realistic change patterns stress-test the pipeline
5. **End-to-End Verification:** Provides observable data flow from producer to consumer

The simulator generates enough variety and volume to thoroughly test the ingestion and merge job capabilities while maintaining realistic business scenarios. 