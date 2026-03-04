from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.indicators_pipeline import run_indicators_for_exchange, IndicatorsFlags


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
        # --- FRVP ---
        frvp=True,
        truncate_scope=True,
        periods=["2year", "1year", "6months", "4months"],
        cutt_off_date=None,

        # --- Converted Daily (EMA/RSI input) ---
        build_converted_daily=True,

        converted_daily_input_schema="silver",
        converted_daily_input_table="FRVP_USA_FOCUS_DATASET",
        converted_daily_input_interval="1min",  # USA focus şu an dakikalık

        converted_daily_output_schema="silver",
        converted_daily_output_table="usa_focus_2e_indicators_converted_daily",

        converted_daily_start_trading_days_back=30,
    )

    run_indicators_for_exchange(repo, "USA", flags)


if __name__ == "__main__":
    main()