#!/usr/bin/env python3
"""
Data Change Simulator for MVP Testing

This CLI tool continuously simulates realistic customer lifecycle changes in the producer database
to demonstrate real-time ingestion and processing capabilities.

Usage:
    python data_change_simulator.py [options]

Options:
    --host HOST          Database host (default: localhost)
    --port PORT          Database port (default: 5432)
    --database DATABASE  Database name (default: customer_db)
    --user USER          Database user (default: postgres)
    --password PASSWORD  Database password (default: postgres)
    --min-interval MIN   Minimum seconds between changes (default: 5)
    --max-interval MAX   Maximum seconds between changes (default: 30)
    --max-changes MAX    Maximum number of changes before stopping (default: unlimited)
    --verbose           Enable verbose logging
    --create-tables     Create tables if they don't exist
    --help              Show this help message

The simulator performs these operations:
- INSERT new customers with addresses
- UPDATE existing customer details (email, phone)
- INSERT new addresses for existing customers
- UPDATE existing addresses
- DELETE old addresses (rarely)

All changes are logged for debugging and verification.
Press Ctrl+C to stop gracefully.
"""

import argparse
import logging
import random
import signal
import sys
import time
from datetime import datetime, date
from typing import List, Optional, Any
import psycopg2
from psycopg2.extras import RealDictCursor


class DataChangeSimulator:
    """Simulates realistic customer data changes in the producer database."""

    def __init__(self, host: str, port: int, database: str, user: str, password: str,
                 min_interval: int, max_interval: int, max_changes: Optional[int] = None, verbose: bool = False, create_tables: bool = False):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.max_changes = max_changes
        self.verbose = verbose
        self.create_tables = create_tables
        self.running = True
        self.changes_made = 0
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.connection_retry_count = 0
        self.max_connection_retries = 5
        self.connection_retry_delay = 5  # seconds
        self.consecutive_failures = 0
        self.max_consecutive_failures = 10  # Stop after 10 consecutive operation failures

        # Setup logging
        log_level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)

        # Sample data for generating realistic changes
        self.first_names = [
            "Alice", "Bob", "Carol", "David", "Emma", "Frank", "Grace", "Henry",
            "Isabella", "Jack", "Kate", "Liam", "Maya", "Noah", "Olivia", "Paul",
            "Quinn", "Rachel", "Sam", "Tina", "Ulysses", "Victoria", "William", "Xara", "Yuki", "Zoe"
        ]

        self.last_names = [
            "Anderson", "Brown", "Clark", "Davis", "Evans", "Fisher", "Garcia", "Harris",
            "Jackson", "Johnson", "King", "Lee", "Martinez", "Nelson", "O'Connor", "Parker",
            "Quinn", "Rodriguez", "Smith", "Taylor", "Underwood", "Valdez", "Wilson", "Xavier", "Young", "Zhang"
        ]

        self.streets = [
            "Main St", "Oak Ave", "Pine Dr", "Elm St", "Maple Way", "Cedar Ln", "Birch Rd",
            "Willow Ct", "Cherry St", "Poplar Ave", "Ash Dr", "Hickory Ln", "Walnut St",
            "Chestnut Ave", "Sycamore Dr", "Dogwood Ct", "Magnolia St", "Redwood Ave"
        ]

        self.cities_states = [
            ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"),
            ("Philadelphia", "PA"), ("Phoenix", "AZ"), ("San Antonio", "TX"), ("San Diego", "CA"),
            ("Dallas", "TX"), ("San Jose", "CA"), ("Austin", "TX"), ("Jacksonville", "FL"),
            ("Fort Worth", "TX"), ("Columbus", "OH"), ("Charlotte", "NC"), ("San Francisco", "CA"),
            ("Indianapolis", "IN"), ("Seattle", "WA"), ("Denver", "CO"), ("Boston", "MA")
        ]

        self.email_domains = [
            "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
            "icloud.com", "protonmail.com", "company.com", "business.org"
        ]

    def connect_to_database(self) -> None:
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            self.connection.autocommit = True
            self.logger.info(f"Connected to database {self.database} at {self.host}:{self.port}")

            # Create tables if requested
            if self.create_tables:
                self.create_tables_if_needed()

        except psycopg2.Error as e:
            self.logger.error(f"Failed to connect to database: {e}")
            sys.exit(1)

    def create_tables_if_needed(self) -> None:
        """Create customers and addresses tables if they don't exist."""
        try:
            with self.connection.cursor() as cursor:
                self.logger.info("🗄️ Creating tables if they don't exist...")

                # Create customers table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS customers (
                        id VARCHAR(20) PRIMARY KEY,
                        firstName VARCHAR(100) NOT NULL,
                        lastName VARCHAR(100) NOT NULL,
                        dob DATE NOT NULL,
                        email VARCHAR(100),
                        phone VARCHAR(100),
                        primaryAddressId VARCHAR(20),
                        billingAddressId VARCHAR(20)
                    );
                """)

                # Create addresses table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS addresses (
                        id VARCHAR(20) PRIMARY KEY,
                        customerId VARCHAR(20) NOT NULL,
                        streetName VARCHAR(100) NOT NULL,
                        city VARCHAR(100) NOT NULL,
                        state VARCHAR(100) NOT NULL,
                        zipCode VARCHAR(30) NOT NULL
                    );
                """)

                # Insert initial test data if tables are empty
                cursor.execute("SELECT COUNT(*) as count FROM customers")
                result = cursor.fetchone()
                customer_count = result['count'] if result else 0  # type: ignore

                if customer_count == 0:
                    self.logger.info("📊 Inserting initial test data...")
                    cursor.execute("""
                        INSERT INTO customers (id, firstName, lastName, dob, email, phone)
                        VALUES ('CUST001', 'John', 'Doe', '1990-01-15', 'john.doe@email.com', '555-1234')
                    """)

                    cursor.execute("""
                        INSERT INTO addresses (id, customerId, streetName, city, state, zipCode)
                        VALUES ('ADDR001', 'CUST001', '123 Main St', 'New York', 'NY', '10001')
                    """)

                    self.logger.info("✅ Initial test data inserted")

                self.logger.info("✅ Tables are ready!")

        except psycopg2.Error as e:
            self.logger.error(f"Failed to create tables: {e}")
            sys.exit(1)

    def is_connection_healthy(self) -> bool:
        """Check if the database connection is healthy."""
        if not self.connection:
            return False

        try:
            # Try a simple query to test the connection
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except (psycopg2.Error, psycopg2.InterfaceError, psycopg2.OperationalError):
            return False

    def ensure_connection(self) -> bool:
        """Ensure we have a healthy database connection, reconnecting if necessary."""
        if self.is_connection_healthy():
            self.connection_retry_count = 0  # Reset retry count on successful connection
            return True

        if self.connection_retry_count >= self.max_connection_retries:
            self.logger.error(f"❌ Maximum connection retry attempts ({self.max_connection_retries}) exceeded. Stopping simulator.")
            self.running = False
            return False

        self.logger.warning("⚠️  Database connection lost. Attempting to reconnect... " +
                            f"(attempt {self.connection_retry_count + 1}/{self.max_connection_retries})")

        # Close existing connection if it exists
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass  # Ignore errors when closing a broken connection
            self.connection = None

        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            self.connection.autocommit = True
            self.logger.info(f"✅ Successfully reconnected to database {self.database} at {self.host}:{self.port}")
            self.connection_retry_count = 0
            return True

        except psycopg2.Error as e:
            self.connection_retry_count += 1
            self.logger.error(f"❌ Failed to reconnect to database (attempt {self.connection_retry_count}/{self.max_connection_retries}): {e}")

            if self.connection_retry_count < self.max_connection_retries:
                self.logger.info(f"⏳ Waiting {self.connection_retry_delay} seconds before next retry...")
                time.sleep(self.connection_retry_delay)

            return False

    def close_connection(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")

    def signal_handler(self, signum: int, frame) -> None:
        """Handle Ctrl+C gracefully."""
        self.logger.info("Received interrupt signal. Stopping simulator...")
        self.running = False

    def generate_customer_id(self) -> str:
        """Generate a unique customer ID."""
        # Use last 8 digits of timestamp + 3 digit random for shorter IDs
        timestamp = int(time.time()) % 100000000  # last 8 digits
        random_num = random.randint(100, 999)
        return f"C{timestamp}{random_num}"

    def generate_address_id(self) -> str:
        """Generate a unique address ID."""
        # Use last 8 digits of timestamp + 3 digit random for shorter IDs
        timestamp = int(time.time()) % 100000000  # last 8 digits
        random_num = random.randint(100, 999)
        return f"A{timestamp}{random_num}"

    def generate_phone_number(self) -> str:
        """Generate a realistic phone number."""
        area_code = random.randint(200, 999)
        exchange = random.randint(200, 999)
        number = random.randint(1000, 9999)
        return f"{area_code}-{exchange}-{number}"

    def generate_email(self, first_name: str, last_name: str) -> str:
        """Generate a realistic email address."""
        domain = random.choice(self.email_domains)
        formats = [
            f"{first_name.lower()}.{last_name.lower()}@{domain}",
            f"{first_name.lower()}{last_name.lower()}@{domain}",
            f"{first_name.lower()}{random.randint(1, 999)}@{domain}",
            f"{first_name[0].lower()}{last_name.lower()}@{domain}"
        ]
        return random.choice(formats)

    def generate_zip_code(self) -> str:
        """Generate a realistic ZIP code."""
        return f"{random.randint(10000, 99999)}"

    def get_existing_customers(self) -> List[Any]:
        """Get list of existing customers."""
        if not self.ensure_connection():
            return []

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT * FROM customers")
                return cursor.fetchall()
        except psycopg2.Error as e:
            self.logger.error(f"Failed to fetch customers: {e}")
            return []

    def get_existing_addresses(self) -> List[Any]:
        """Get list of existing addresses."""
        if not self.ensure_connection():
            return []

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT * FROM addresses")
                return cursor.fetchall()
        except psycopg2.Error as e:
            self.logger.error(f"Failed to fetch addresses: {e}")
            return []

    def insert_new_customer(self) -> bool:
        """Insert a new customer with a primary address."""
        if not self.ensure_connection():
            return False

        try:
            # Generate customer data
            customer_id = self.generate_customer_id()
            first_name = random.choice(self.first_names)
            last_name = random.choice(self.last_names)

            # Generate date of birth (age 18-80)
            current_year = datetime.now().year
            birth_year = random.randint(current_year - 80, current_year - 18)
            birth_month = random.randint(1, 12)
            birth_day = random.randint(1, 28)  # Safe day for all months
            dob = date(birth_year, birth_month, birth_day)

            email = self.generate_email(first_name, last_name)
            phone = self.generate_phone_number()

            # Generate primary address
            address_id = self.generate_address_id()
            street_num = random.randint(1, 9999)
            street_name = f"{street_num} {random.choice(self.streets)}"
            city, state = random.choice(self.cities_states)
            zip_code = self.generate_zip_code()

            with self.connection.cursor() as cursor:
                # Insert customer first without address references
                cursor.execute("""
                    INSERT INTO customers (id, firstName, lastName, dob, email, phone, primaryAddressId, billingAddressId)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (customer_id, first_name, last_name, dob, email, phone, None, None))

                # Insert address with customer reference
                cursor.execute("""
                    INSERT INTO addresses (id, customerId, streetName, city, state, zipCode)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (address_id, customer_id, street_name, city, state, zip_code))

                # Update customer with address references
                cursor.execute("""
                    UPDATE customers SET primaryAddressId = %s, billingAddressId = %s WHERE id = %s
                """, (address_id, address_id, customer_id))

            self.logger.info(f"✅ CREATED customer {customer_id} ({first_name} {last_name}) with address {address_id}")
            return True

        except psycopg2.Error as e:
            self.logger.error(f"❌ Failed to insert new customer: {e}")
            return False

    def update_customer_info(self, customers: List[Any]) -> bool:
        """Update existing customer information."""
        if not customers:
            return False

        if not self.ensure_connection():
            return False

        try:
            customer = random.choice(customers)
            customer_id = customer['id']

            # Randomly choose what to update
            update_type = random.choice(['email', 'phone', 'both'])

            updates = []
            values = []

            if update_type in ['email', 'both']:
                new_email = self.generate_email(customer['firstname'], customer['lastname'])
                updates.append("email = %s")
                values.append(new_email)

            if update_type in ['phone', 'both']:
                new_phone = self.generate_phone_number()
                updates.append("phone = %s")
                values.append(new_phone)

            values.append(customer_id)

            with self.connection.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE customers SET {', '.join(updates)}
                    WHERE id = %s
                """, values)

            update_fields = ', '.join([u.split(' = ')[0] for u in updates])
            self.logger.info(f"📝 UPDATED customer {customer_id} ({update_fields})")
            return True

        except psycopg2.Error as e:
            self.logger.error(f"❌ Failed to update customer: {e}")
            return False

    def insert_new_address(self, customers: List[Any]) -> bool:
        """Insert a new address for an existing customer."""
        if not customers:
            return False

        if not self.ensure_connection():
            return False

        try:
            customer = random.choice(customers)
            customer_id = customer['id']

            # Generate new address
            address_id = self.generate_address_id()
            street_num = random.randint(1, 9999)
            street_name = f"{street_num} {random.choice(self.streets)}"
            city, state = random.choice(self.cities_states)
            zip_code = self.generate_zip_code()

            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO addresses (id, customerId, streetName, city, state, zipCode)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (address_id, customer_id, street_name, city, state, zip_code))

                # Sometimes make this the new billing address
                if random.random() < 0.3:  # 30% chance
                    cursor.execute("""
                        UPDATE customers SET billingAddressId = %s WHERE id = %s
                    """, (address_id, customer_id))
                    self.logger.info(f"🏠 ADDED address {address_id} for customer {customer_id} (set as billing)")
                else:
                    self.logger.info(f"🏠 ADDED address {address_id} for customer {customer_id}")

            return True

        except psycopg2.Error as e:
            self.logger.error(f"❌ Failed to insert new address: {e}")
            return False

    def update_address(self, addresses: List[Any]) -> bool:
        """Update an existing address."""
        if not addresses:
            return False

        if not self.ensure_connection():
            return False

        try:
            address = random.choice(addresses)
            address_id = address['id']

            # Randomly choose what to update
            update_type = random.choice(['street', 'city_state', 'zip'])

            with self.connection.cursor() as cursor:
                if update_type == 'street':
                    street_num = random.randint(1, 9999)
                    new_street = f"{street_num} {random.choice(self.streets)}"
                    cursor.execute("""
                        UPDATE addresses SET streetName = %s WHERE id = %s
                    """, (new_street, address_id))
                    self.logger.info(f"🏠 UPDATED address {address_id} (street: {new_street})")

                elif update_type == 'city_state':
                    new_city, new_state = random.choice(self.cities_states)
                    new_zip = self.generate_zip_code()
                    cursor.execute("""
                        UPDATE addresses SET city = %s, state = %s, zipCode = %s WHERE id = %s
                    """, (new_city, new_state, new_zip, address_id))
                    self.logger.info(f"🏠 UPDATED address {address_id} (moved to {new_city}, {new_state})")

                elif update_type == 'zip':
                    new_zip = self.generate_zip_code()
                    cursor.execute("""
                        UPDATE addresses SET zipCode = %s WHERE id = %s
                    """, (new_zip, address_id))
                    self.logger.info(f"🏠 UPDATED address {address_id} (zip: {new_zip})")

            return True

        except psycopg2.Error as e:
            self.logger.error(f"❌ Failed to update address: {e}")
            return False

    def delete_address(self, addresses: List[Any], customers: List[Any]) -> bool:
        """Delete an address (only if customer has multiple addresses)."""
        if not addresses or not customers:
            return False

        if not self.ensure_connection():
            return False

        try:
            # Find customers with multiple addresses
            customer_address_counts = {}
            for address in addresses:
                customer_id = address['customerid']
                customer_address_counts[customer_id] = customer_address_counts.get(customer_id, 0) + 1

            # Get customers with more than one address
            multi_address_customers = [cid for cid, count in customer_address_counts.items() if count > 1]

            if not multi_address_customers:
                return False

            # Pick a customer with multiple addresses
            customer_id = random.choice(multi_address_customers)
            customer_addresses = [a for a in addresses if a['customerid'] == customer_id]

            # Don't delete primary or billing addresses
            customer = next((c for c in customers if c['id'] == customer_id), None)
            if not customer:
                return False

            deletable_addresses = [
                a for a in customer_addresses
                if a['id'] != customer.get('primaryaddressid') and a['id'] != customer.get('billingaddressid')
            ]

            if not deletable_addresses:
                return False

            address_to_delete = random.choice(deletable_addresses)
            address_id = address_to_delete['id']

            with self.connection.cursor() as cursor:
                cursor.execute("DELETE FROM addresses WHERE id = %s", (address_id,))

            self.logger.info(f"🗑️  DELETED address {address_id} for customer {customer_id}")
            return True

        except psycopg2.Error as e:
            self.logger.error(f"❌ Failed to delete address: {e}")
            return False

    def perform_random_operation(self) -> None:
        """Perform a random database operation."""
        customers = self.get_existing_customers()
        addresses = self.get_existing_addresses()

        # Define operation weights (higher = more likely)
        operations = [
            ('insert_customer', 15),      # Create new customers
            ('update_customer', 25),      # Update customer info
            ('insert_address', 20),       # Add new addresses
            ('update_address', 30),       # Update existing addresses
            ('delete_address', 10)        # Delete addresses (rare)
        ]

        # Choose operation based on weights
        total_weight = sum(weight for _, weight in operations)
        r = random.uniform(0, total_weight)
        current_weight = 0

        for operation, weight in operations:
            current_weight += weight
            if r <= current_weight:
                break

        # Execute the chosen operation
        success = False
        if operation == 'insert_customer':
            success = self.insert_new_customer()
        elif operation == 'update_customer':
            success = self.update_customer_info(customers)
        elif operation == 'insert_address':
            success = self.insert_new_address(customers)
        elif operation == 'update_address':
            success = self.update_address(addresses)
        elif operation == 'delete_address':
            success = self.delete_address(addresses, customers)

        if success:
            self.changes_made += 1
            self.consecutive_failures = 0  # Reset failure counter on success
            if self.max_changes and self.changes_made >= self.max_changes:
                self.logger.info(f"🎯 Reached maximum changes limit ({self.max_changes}). Stopping...")
                self.running = False
        else:
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.max_consecutive_failures:
                self.logger.error(f"❌ Too many consecutive failures ({self.consecutive_failures}). Stopping simulator to prevent infinite loop.")
                self.running = False
            elif self.verbose:
                self.logger.debug(f"Operation {operation} could not be performed (no suitable data) - consecutive failures: {self.consecutive_failures}")

    def run(self) -> None:
        """Main simulation loop."""
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.info("🚀 Starting Data Change Simulator")
        self.logger.info(f"   Database: {self.database} at {self.host}:{self.port}")
        self.logger.info(f"   Change interval: {self.min_interval}-{self.max_interval} seconds")
        if self.max_changes:
            self.logger.info(f"   Max changes: {self.max_changes}")
        self.logger.info("   Press Ctrl+C to stop")

        self.connect_to_database()

        try:
            while self.running:
                # Perform a random operation
                self.perform_random_operation()

                # Wait for random interval
                if self.running:
                    wait_time = random.randint(self.min_interval, self.max_interval)
                    if self.verbose:
                        self.logger.debug(f"Waiting {wait_time} seconds until next change...")

                    for _ in range(wait_time):
                        if not self.running:
                            break
                        time.sleep(1)

        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            self.close_connection()
            self.logger.info("🛑 Data Change Simulator stopped")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Data Change Simulator for MVP Testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('--host', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432, help='Database port (default: 5432)')
    parser.add_argument('--database', default='customer_db', help='Database name (default: customer_db)')
    parser.add_argument('--user', default='postgres', help='Database user (default: postgres)')
    parser.add_argument('--password', default='postgres', help='Database password (default: postgres)')
    parser.add_argument('--min-interval', type=int, default=5, help='Minimum seconds between changes (default: 5)')
    parser.add_argument('--max-interval', type=int, default=30, help='Maximum seconds between changes (default: 30)')
    parser.add_argument('--max-changes', type=int, help='Maximum number of changes before stopping (default: unlimited)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--create-tables', action='store_true', help='Create tables if they don\'t exist')

    args = parser.parse_args()

    # Validate arguments
    if args.min_interval <= 0 or args.max_interval <= 0:
        parser.error("Intervals must be positive")
    if args.min_interval > args.max_interval:
        parser.error("min-interval must be <= max-interval")
    if args.max_changes is not None and args.max_changes <= 0:
        parser.error("max-changes must be positive")

    # Create and run simulator
    simulator = DataChangeSimulator(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
        min_interval=args.min_interval,
        max_interval=args.max_interval,
        max_changes=args.max_changes,
        verbose=args.verbose,
        create_tables=getattr(args, 'create_tables', False)
    )

    simulator.run()


if __name__ == '__main__':
    main()
