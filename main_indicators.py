import os
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.services.ind_frv_poc_profile_service import IndFrvPocProfileService

def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    # periods = ["2year","1year","9months","6months","4months","1month","2weeks"] # 4ay, 6 ay,1yil, 2yil
    periods = ["2year","1year","6months","4months"]

    cutt_off_date = None  # or "2026-02-25 23:59:00"

    svc = IndFrvPocProfileService(repo=repo)

    # print("\n[IND] FRVP POC/VAL/VAH started (BIST)...\n")
    # svc.run(exchange="BIST", periods=periods, cutt_off_date=cutt_off_date, is_truncate_scope=True)

    print("\n[IND] FRVP POC/VAL/VAH started (USA)...\n")
    svc.run(exchange="USA", periods=periods, cutt_off_date=cutt_off_date, is_truncate_scope=True)

if __name__ == "__main__":
    main()