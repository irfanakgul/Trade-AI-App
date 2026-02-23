from app.infrastructure.database.connection import Database
from sqlalchemy import text

db = Database()
engine = db.connect()

with engine.connect() as conn:
    result = conn.execute(text("SELECT version();"))
    print(result.fetchone())