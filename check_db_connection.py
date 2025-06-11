import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

# 1) Load .env
load_dotenv()

# 2) Read the full URL
database_url = os.getenv("DATABASE_URL")

if not database_url:
    print("❌ DATABASE_URL not found in .env")
    exit(1)

try:
    # 3) Create the engine with pymssql
    engine = create_engine(database_url)
    # 4) Verify the connection
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        print("✅ Database connection successful!")
        print("Result:", result.scalar())
except OperationalError as e:
    print("❌ Database connection failed:")
    print(e)
except Exception as e:
    print("❌ Unexpected error:")
    print(e)
