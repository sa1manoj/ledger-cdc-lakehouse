"""Runtime settings, read from environment variables (see .env.example)."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    lake_root: str
    kafka_bootstrap: str
    postgres_dsn: str
    azure_account: str | None = None
    azure_key: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            lake_root=os.getenv("LAKE_ROOT", "./lake").rstrip("/"),
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            postgres_dsn=os.getenv(
                "POSTGRES_DSN", "postgresql://ledger:ledger@localhost:5432/ledger"
            ),
            azure_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
            azure_key=os.getenv("AZURE_STORAGE_KEY"),
        )

    @property
    def is_local(self) -> bool:
        return "://" not in self.lake_root

    @property
    def bronze_path(self) -> str:
        return f"{self.lake_root}/bronze/cdc_events"

    def silver_path(self, table: str) -> str:
        return f"{self.lake_root}/silver/{table}"

    def checkpoint(self, name: str) -> str:
        return f"{self.lake_root}/_checkpoints/{name}"

    def delta_storage_options(self) -> dict | None:
        """Options for delta-rs (reconciliation reads) when the lake is on ADLS Gen2."""
        if self.is_local:
            return None
        return {"account_name": self.azure_account or "", "account_key": self.azure_key or ""}
