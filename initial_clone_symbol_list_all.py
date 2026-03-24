from dotenv import load_dotenv
from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from sqlalchemy import text,bindparam
from datetime import datetime
from app.services.telegram_bot_chat_service import telegram_send_message # type: ignore
import os

load_dotenv()

db = Database()
engine = db.connect()
repo = PostgresRepository(engine)

def clone_symbol_list_for_focus(
    main_symbol_schema: str,
    main_symbol_table: str,
) -> dict:
    """
    Clones the main symbol table into silver."cloned_focus_symbol_list" once per day.
    Adds OUT_OF_SCOPE (default False) and OOS_REASON (default NULL) columns.
    Always drops and recreates — meant to run once at the very start of the daily schedule.
    """

    src_fqn = f'{main_symbol_schema}."{main_symbol_table}"'
    tgt_fqn = 'silver."cloned_focus_symbol_list"'

    q_drop = text(f'DROP TABLE IF EXISTS {tgt_fqn};')

    q_create = text(f"""
        CREATE TABLE {tgt_fqn} AS
        SELECT
            s.*,
            FALSE::boolean AS "OUT_OF_SCOPE",
            NULL::text     AS "OOS_REASON",
            NOW() AS CREATED_AT
        FROM {src_fqn} s;
    """)

    q_counts = text(f"""
        SELECT
            COUNT(*)                        AS total,
            COUNT(DISTINCT "EXCHANGE")      AS exchanges
        FROM {tgt_fqn};
    """)

    with engine.begin() as conn:
        conn.execute(q_drop)
        conn.execute(q_create)
        counts = conn.execute(q_counts).mappings().one()

    total     = int(counts["total"])
    exchanges = int(counts["exchanges"])

    print(
        f"[CLONE SYMBOLS] all exchange/symbols has been cloned | "
        f"Total symbols: {total} | "
        f"Exchanges: {exchanges} | {datetime.now()}"
    )

    return {
        "source":    f"{main_symbol_schema}.{main_symbol_table}",
        "target":    "silver.cloned_focus_symbol_list",
        "total":     total,
        "exchanges": exchanges,
    }


if __name__ == "__main__":
    try:
        clone_symbol_list_for_focus(
            main_symbol_schema="prod",
            main_symbol_table="FOCUS_SYMBOLS_ALL",
        )
        if os.getenv("ENV_TELEGRAM_NOTIF")=="True":
            telegram_send_message(
                title="INITIAL CLONE",
                text="✅ Initial symbols cloned succesfuly")
    except Exception as e:
        if os.getenv("ENV_TELEGRAM_NOTIF")=="True":
            telegram_send_message(
                title="INITIAL CLONE ERROR!",
                text=f"❌ Initial symbols couldn't clone!\nERROR: {e}")