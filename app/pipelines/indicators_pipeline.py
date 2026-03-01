from dataclasses import dataclass
from typing import Optional, List

from app.infrastructure.database.repository import PostgresRepository
from app.services.ind_frv_poc_profile_service import IndFrvPocProfileService
from datetime import datetime


@dataclass(frozen=True)
class IndicatorsFlags:
    frvp: bool = True
    truncate_scope: bool = True
    periods: List[str] = None
    cutt_off_date: Optional[str] = None


def run_indicators_for_exchange(repo: PostgresRepository, exchange: str, flags: IndicatorsFlags) -> None:
    if flags.periods is None:
        periods = ["2year", "1year", "6months", "4months"]
    else:
        periods = flags.periods

    if not flags.frvp:
        print(f"[IND] FRVP skipped for exchange={exchange}")
        return

    svc = IndFrvPocProfileService(repo=repo)

    print(
        f"\n[IND] FRVP POC/VAL/VAH started ({exchange})... "
        f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
    )
    svc.run(
        exchange=exchange,
        periods=periods,
        cutt_off_date=flags.cutt_off_date,
        is_truncate_scope=flags.truncate_scope,
        )
    
    print(
        f"\n[IND] FRVP POC/VAL/VAH ended ({exchange})... "
        f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
    )