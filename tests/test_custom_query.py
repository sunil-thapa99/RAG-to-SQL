import pytest
from src.core.database import PostgresDatabase
from datetime import datetime

@pytest.fixture(scope="module")
def db():
    """Fixture to create and manage database connection"""
    database = PostgresDatabase()
    if not database.connect():
        pytest.fail("Failed to connect to database")
    yield database
    database.disconnect()

class TestCustomQueries:
    def test_customers_by_country(self, db):
        """Test querying customers from a specific country"""
        country = "China"
        results = db.execute_query(
            "SELECT customer_id, first_name, last_name FROM customers WHERE country = %s",
            (country,)
        )
        
        assert len(results) == 9

    def test_customers_by_year(self, db):
        """Test querying customers who subscribed in a specific year"""
        year = 2021
        results = db.execute_query(
            "SELECT customer_id, first_name, last_name, subscription_date FROM customers WHERE EXTRACT(YEAR FROM subscription_date) = %s",
            (year,)
        )
        
        assert len(results) == 404

    def test_customers_by_month(self, db):
        """Test counting customers by subscription month"""
        results = db.execute_query("""
            SELECT 
                EXTRACT(MONTH FROM subscription_date) as month,
                COUNT(*) as count
            FROM customers
            GROUP BY month
            ORDER BY month
        """)
        
        month_counts = {int(row[0]): row[1] for row in results}
        assert month_counts[1] == 107  # January
        assert month_counts[6] == 57  # June
        assert month_counts[12] == 72  # December

    def test_customers_by_email_domain(self, db):
        """Test finding customers with specific email domain"""
        email_domain = "%leonard.com"
        results = db.execute_query(
            "SELECT customer_id, first_name, last_name, email FROM customers WHERE email LIKE %s",
            (email_domain,)
        )
        
        assert len(results) == 1

    def test_database_connection(self, db):
        """Test database connection and disconnection"""
        assert db.conn is not None
        db.disconnect()
        assert db.conn is None
        assert db.connect()
        assert db.conn is not None

    @pytest.mark.parametrize("invalid_country", ["InvalidCountry", "", None])
    def test_no_customers_for_invalid_country(self, db, invalid_country):
        """Test querying customers from a non-existent country"""
        results = db.execute_query(
            "SELECT customer_id FROM customers WHERE country = %s",
            (invalid_country,)
        )
        assert len(results) == 0
