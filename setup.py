
from src.core.database import PostgresDatabase
from dotenv import load_dotenv
import os

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    # Initialize database with environment variables
    db = PostgresDatabase()
    
    # Run the setup
    if db.setup():
        print("Database setup completed successfully!")
    else:
        print("Database setup failed!")

if __name__ == "__main__":
    main()

