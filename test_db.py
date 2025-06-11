# test_db.py

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set in .env")

engine = create_engine(DATABASE_URL)
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1 AS TestValue"))
    row = result.fetchone()
    # Since `row` is a tuple like (1,), index by 0:
    print("✅ Connected! TestValue =", row[0])
