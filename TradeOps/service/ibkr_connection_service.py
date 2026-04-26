from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ib_insync import IB

from config.settings import AppSettings


@dataclass
class IbkrConnectionResult:
    SUCCESS: bool
    MESSAGE: str


class IbkrConnectionService:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.ib: Optional[IB] = None

    def connect(self, readonly: bool = False) -> IbkrConnectionResult:
        print("[IBKR] CONNECT START")

        try:
            if self.ib is None:
                self.ib = IB()

            if self.ib.isConnected():
                message = "Already connected to IBKR."
                print(f"[IBKR] CONNECT SUCCESS - {message}")
                return IbkrConnectionResult(SUCCESS=True, MESSAGE=message)

            self.ib.connect(
                host=self.settings.IBKR_HOST,
                port=self.settings.IBKR_PORT,
                clientId=self.settings.IBKR_CLIENT_ID,
                readonly=readonly,
                timeout=10,
            )

            if not self.ib.isConnected():
                message = "Connection attempt finished but IBKR is not connected."
                print(f"[IBKR] CONNECT FAIL - {message}")
                return IbkrConnectionResult(SUCCESS=False, MESSAGE=message)

            message = (
                f"Connected to IBKR "
                f"({self.settings.IBKR_HOST}:{self.settings.IBKR_PORT}, "
                f"clientId={self.settings.IBKR_CLIENT_ID})"
            )
            print(f"[IBKR] CONNECT SUCCESS - {message}")
            return IbkrConnectionResult(SUCCESS=True, MESSAGE=message)

        except Exception as exc:
            message = f"Failed to connect to IBKR: {exc}"
            print(f"[IBKR] CONNECT FAIL - {message}")
            return IbkrConnectionResult(SUCCESS=False, MESSAGE=message)

    def disconnect(self) -> None:
        print("[IBKR] DISCONNECT START")

        try:
            if self.ib and self.ib.isConnected():
                self.ib.disconnect()
                print("[IBKR] DISCONNECT SUCCESS")
            else:
                print("[IBKR] DISCONNECT SKIPPED - not connected")
        except Exception as exc:
            print(f"[IBKR] DISCONNECT FAIL - {exc}")

    def is_connected(self) -> bool:
        return self.ib is not None and self.ib.isConnected()

    def reconnect(self, readonly: bool = False) -> IbkrConnectionResult:
        print("[IBKR] RECONNECT START")
        self.disconnect()
        result = self.connect(readonly=readonly)

        if result.SUCCESS:
            print("[IBKR] RECONNECT SUCCESS")
        else:
            print("[IBKR] RECONNECT FAIL")

        return result

    def get_client(self) -> IB:
        if self.ib is None or not self.ib.isConnected():
            raise RuntimeError("IBKR client is not connected.")
        return self.ib