import os
import csv
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from typing import Dict, List, Optional, Tuple, Any


class PostgresDatabase:
    """
    A class to handle PostgreSQL database operations including
    database creation, table creation, and data import.
    """

    def __init__(self, env_file: str = '.env'):
        """
        Initialize the database connection parameters from environment variables.

        Args:
            env_file: Path to the .env file containing database credentials
        """
        # Load environment variables from .env file
        load_dotenv(env_file)

        # Database connection parameters from environment variables
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5433")
        self.name = os.getenv("DB_NAME", "rag_to_sql")
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "1234")

        # Connection objects
        self.conn = None
        self.cursor = None

    def connect(self, database: str = None) -> None:
        """
        Connect to the PostgreSQL database.

        Args:
            database: Database name to connect to. If None, uses self.name

        Returns:
            None
        """
        db_name = database if database else self.name

        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=db_name
            )
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return False

    def disconnect(self) -> None:
        """Close the database connection and cursor."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.cursor = None
        self.conn = None

    def execute_query(self, query: str, params: Tuple = None, commit: bool = False) -> Optional[List]:
        """
        Execute a SQL query.

        Args:
            query: SQL query to execute
            params: Parameters for the query
            commit: Whether to commit the transaction

        Returns:
            Query results if any
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            if commit:
                self.conn.commit()

            # Try to fetch results, return None if not a SELECT query
            try:
                return self.cursor.fetchall()
            except psycopg2.ProgrammingError:
                return None
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def create_database(self) -> bool:
        """
        Create the database if it doesn't exist.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Connect to default postgres database to create our database
            if not self.connect("postgres"):
                return False

            self.conn.autocommit = True

            # Check if database exists
            result = self.execute_query("SELECT 1 FROM pg_database WHERE datname = %s", (self.name,))
            exists = len(result) > 0 if result else False

            if not exists:
                print(f"Creating database {self.name}...")
                self.execute_query(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.name)),
                    commit=True
                )
                print(f"Database {self.name} created successfully!")
            else:
                print(f"Database {self.name} already exists.")

            self.disconnect()
            return True
        except Exception as e:
            print(f"Error creating database: {e}")
            self.disconnect()
            return False

    def create_customer_table(self) -> bool:
        """
        Create the customer table in the database.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self.connect(self.name):
                return False

            self.conn.autocommit = True

            # Create customer table
            self.execute_query("""
            CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                customer_index INTEGER,
                customer_id VARCHAR(16) UNIQUE,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                company VARCHAR(100),
                city VARCHAR(100),
                country VARCHAR(100),
                phone_1 VARCHAR(50),
                phone_2 VARCHAR(50),
                email VARCHAR(100),
                subscription_date DATE,
                website VARCHAR(255)
            )
            """)

            # Create indexes for better query performance
            self.execute_query("CREATE INDEX IF NOT EXISTS idx_customer_id ON customers(customer_id)")
            self.execute_query("CREATE INDEX IF NOT EXISTS idx_customer_email ON customers(email)")
            self.execute_query("CREATE INDEX IF NOT EXISTS idx_customer_name ON customers(last_name, first_name)")
            self.execute_query("CREATE INDEX IF NOT EXISTS idx_subscription_date ON customers(subscription_date)")
            self.execute_query("CREATE INDEX IF NOT EXISTS idx_country ON customers(country)")

            print("Customer table created successfully!")

            self.disconnect()
            return True
        except Exception as e:
            print(f"Error creating customer table: {e}")
            self.disconnect()
            return False

    def import_customer_data(self, csv_path: str = 'data/customer.csv') -> bool:
        """
        Import data from CSV file into the customer table.

        Args:
            csv_path: Path to the CSV file

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self.connect(self.name):
                return False

            # Check if data already exists
            result = self.execute_query("SELECT COUNT(*) FROM customers")
            count = result[0][0] if result else 0

            if count > 0:
                print(f"Customer table already contains {count} records. Skipping import.")
                self.disconnect()
                return True

            # Read CSV file and insert data
            with open(csv_path, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header row

                for row in reader:
                    self.execute_query(
                        """
                        INSERT INTO customers (
                            customer_index, customer_id, first_name, last_name,
                            company, city, country, phone_1, phone_2,
                            email, subscription_date, website
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            row[0], row[1], row[2], row[3], row[4],
                            row[5], row[6], row[7], row[8], row[9],
                            row[10], row[11]
                        )
                    )

            self.conn.commit()
            print("Customer data imported successfully!")

            self.disconnect()
            return True
        except Exception as e:
            print(f"Error importing customer data: {e}")
            self.disconnect()
            return False

    def setup(self) -> bool:
        """
        Set up the database, create tables, and import data.

        Returns:
            bool: True if all operations were successful, False otherwise
        """
        try:
            if not self.create_database():
                return False

            if not self.create_customer_table():
                return False

            if not self.import_customer_data():
                return False

            print("Database setup completed successfully!")
            return True
        except Exception as e:
            print(f"Error during database setup: {e}")
            return False

    def execute_sample_queries(self) -> Dict[str, Any]:
        """
        Run some sample queries to test the database.

        Returns:
            Dict containing query results
        """
        results = {}

        try:
            if not self.connect(self.name):
                return {"error": "Failed to connect to database"}

            # Query 1: Count total number of customers
            count_result = self.execute_query("SELECT COUNT(*) FROM customers")
            results["total_customers"] = count_result[0][0] if count_result else 0

            # Query 2: Get customers by country (top 5 countries)
            country_result = self.execute_query("""
            SELECT country, COUNT(*) as customer_count
            FROM customers
            GROUP BY country
            ORDER BY customer_count DESC
            LIMIT 5
            """)
            results["top_countries"] = country_result if country_result else []

            # Query 3: Get customers by subscription year
            year_result = self.execute_query("""
            SELECT EXTRACT(YEAR FROM subscription_date) as year, COUNT(*)
            FROM customers
            GROUP BY year
            ORDER BY year
            """)
            results["customers_by_year"] = year_result if year_result else []

            # Query 4: Get 5 random customers
            customer_result = self.execute_query("""
            SELECT customer_id, first_name, last_name, email, country
            FROM customers
            ORDER BY RANDOM()
            LIMIT 5
            """)
            results["random_customers"] = customer_result if customer_result else []

            self.disconnect()
            return results
        except Exception as e:
            print(f"Error executing sample queries: {e}")
            self.disconnect()
            return {"error": str(e)}

if __name__ == "__main__":
    db = PostgresDatabase()
    db.setup()
