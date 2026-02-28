from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.indicators_pipeline import run_indicators_for_exchange, IndicatorsFlags


def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = IndicatorsFlags(
        frvp=True,
        truncate_scope=True,
        periods=["2year", "1year", "6months", "4months"],
        cutt_off_date=None,
    )

    run_indicators_for_exchange(repo, "BIST", flags)


if __name__ == "__main__":
    main()