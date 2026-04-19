from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ExchangeAllocationInput:
    EXCHANGE: str
    EXCHANGE_MAX_OPEN_POSITIONS: int
    CURRENT_OPEN_POSITION_COUNT: int
    ALLOCATED_BUDGET_PCT: float


@dataclass
class ExchangeAllocationResult:
    EXCHANGE: str
    EXCHANGE_MAX_OPEN_POSITIONS: int
    CURRENT_OPEN_POSITION_COUNT: int
    REMAINING_OPEN_POSITION_SLOTS: int
    ALLOCATED_BUDGET_PCT: float
    ALLOCATED_BUDGET_AMOUNT: float
    SLOT_BUDGET_AMOUNT: float
    PLANNED_BUY_COUNT: int
    IS_ENABLED: bool


@dataclass
class GlobalBuyPreparationResult:
    TOTAL_MAX_OPEN_POSITIONS: int
    MAX_DAILY_TRADE_COUNT: int
    GLOBAL_CURRENT_OPEN_POSITION_COUNT: int
    GLOBAL_REMAINING_OPEN_POSITION_SLOTS: int
    TODAY_BUY_COUNT_USED: int
    TODAY_BUY_COUNT_REMAINING: int
    AVAILABLE_FUNDS: float
    EXCHANGE_RESULTS: list[ExchangeAllocationResult]


class BudgetService:
    def calculate_global_buy_preparation(
        self,
        TOTAL_MAX_OPEN_POSITIONS: int,
        MAX_DAILY_TRADE_COUNT: int,
        GLOBAL_CURRENT_OPEN_POSITION_COUNT: int,
        TODAY_BUY_COUNT_USED: int,
        AVAILABLE_FUNDS: float,
        EXCHANGE_INPUTS: list[ExchangeAllocationInput],
        EXCHANGE_PRIORITY: list[str],
    ) -> GlobalBuyPreparationResult:
        global_remaining_open_position_slots = max(
            TOTAL_MAX_OPEN_POSITIONS - GLOBAL_CURRENT_OPEN_POSITION_COUNT,
            0,
        )

        today_buy_count_remaining = max(
            MAX_DAILY_TRADE_COUNT - TODAY_BUY_COUNT_USED,
            0,
        )

        global_remaining_buy_capacity = min(
            global_remaining_open_position_slots,
            today_buy_count_remaining,
        )

        inputs_by_exchange = {
            item.EXCHANGE.upper(): item
            for item in EXCHANGE_INPUTS
        }

        ordered_exchange_codes: list[str] = []

        for exchange_code in EXCHANGE_PRIORITY:
            normalized = exchange_code.strip().upper()
            if normalized in inputs_by_exchange and normalized not in ordered_exchange_codes:
                ordered_exchange_codes.append(normalized)

        for exchange_code in inputs_by_exchange.keys():
            if exchange_code not in ordered_exchange_codes:
                ordered_exchange_codes.append(exchange_code)

        planned_buy_counts: dict[str, int] = {}
        remaining_global_capacity = global_remaining_buy_capacity

        for exchange_code in ordered_exchange_codes:
            item = inputs_by_exchange[exchange_code]

            remaining_exchange_slots = max(
                item.EXCHANGE_MAX_OPEN_POSITIONS - item.CURRENT_OPEN_POSITION_COUNT,
                0,
            )

            planned_for_exchange = min(
                remaining_exchange_slots,
                remaining_global_capacity,
            )

            planned_buy_counts[exchange_code] = planned_for_exchange
            remaining_global_capacity -= planned_for_exchange

        exchange_results: list[ExchangeAllocationResult] = []

        for exchange_code in ordered_exchange_codes:
            item = inputs_by_exchange[exchange_code]

            remaining_exchange_slots = max(
                item.EXCHANGE_MAX_OPEN_POSITIONS - item.CURRENT_OPEN_POSITION_COUNT,
                0,
            )

            allocated_budget_amount = max(
                AVAILABLE_FUNDS * item.ALLOCATED_BUDGET_PCT,
                0.0,
            )

            planned_buy_count = planned_buy_counts.get(exchange_code, 0)

            if planned_buy_count > 0:
                slot_budget_amount = allocated_budget_amount / planned_buy_count
            else:
                slot_budget_amount = 0.0

            is_enabled = planned_buy_count > 0 and allocated_budget_amount > 0

            exchange_results.append(
                ExchangeAllocationResult(
                    EXCHANGE=exchange_code,
                    EXCHANGE_MAX_OPEN_POSITIONS=item.EXCHANGE_MAX_OPEN_POSITIONS,
                    CURRENT_OPEN_POSITION_COUNT=item.CURRENT_OPEN_POSITION_COUNT,
                    REMAINING_OPEN_POSITION_SLOTS=remaining_exchange_slots,
                    ALLOCATED_BUDGET_PCT=item.ALLOCATED_BUDGET_PCT,
                    ALLOCATED_BUDGET_AMOUNT=allocated_budget_amount,
                    SLOT_BUDGET_AMOUNT=slot_budget_amount,
                    PLANNED_BUY_COUNT=planned_buy_count,
                    IS_ENABLED=is_enabled,
                )
            )

        return GlobalBuyPreparationResult(
            TOTAL_MAX_OPEN_POSITIONS=TOTAL_MAX_OPEN_POSITIONS,
            MAX_DAILY_TRADE_COUNT=MAX_DAILY_TRADE_COUNT,
            GLOBAL_CURRENT_OPEN_POSITION_COUNT=GLOBAL_CURRENT_OPEN_POSITION_COUNT,
            GLOBAL_REMAINING_OPEN_POSITION_SLOTS=global_remaining_open_position_slots,
            TODAY_BUY_COUNT_USED=TODAY_BUY_COUNT_USED,
            TODAY_BUY_COUNT_REMAINING=today_buy_count_remaining,
            AVAILABLE_FUNDS=AVAILABLE_FUNDS,
            EXCHANGE_RESULTS=exchange_results,
        )