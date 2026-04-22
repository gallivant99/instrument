from __future__ import annotations

import shutil
import sys
import time
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "traceability.db"
sys.path.insert(0, str(ROOT))

from app.db import initialize_database, seed_demo_data  # noqa: E402


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rebuilt_in_place = False
    if DB_PATH.exists():
        backup_name = f"traceability.backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.db"
        backup_path = DATA_DIR / backup_name
        shutil.copy2(DB_PATH, backup_path)
        for attempt in range(20):
            try:
                DB_PATH.unlink()
                break
            except PermissionError:
                if attempt == 19:
                    rebuilt_in_place = True
                    _clear_existing_database()
                    break
                time.sleep(0.2)
        print(f"Backed up old database: {backup_path}")
    initialize_database(DB_PATH)
    seed_demo_data(DB_PATH)
    action = "Reset existing database" if rebuilt_in_place else "Created demo database"
    print(f"{action}: {DB_PATH}")


def _clear_existing_database() -> None:
    from app.db import get_connection  # noqa: PLC0415

    with get_connection(DB_PATH) as connection:
        connection.executescript(
            """
            PRAGMA foreign_keys = OFF;
            DELETE FROM audit_logs;
            DELETE FROM recall_impacts;
            DELETE FROM recall_cases;
            DELETE FROM scrap_requests;
            DELETE FROM procurements;
            DELETE FROM trace_events;
            DELETE FROM maintenance_records;
            DELETE FROM clinical_usages;
            DELETE FROM stock_movements;
            DELETE FROM code_mappings;
            DELETE FROM devices;
            DELETE FROM users;
            DELETE FROM patients;
            DELETE FROM departments;
            DELETE FROM suppliers;
            DELETE FROM sqlite_sequence;
            PRAGMA foreign_keys = ON;
            """
        )


if __name__ == "__main__":
    main()
