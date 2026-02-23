import clickhouse_connect
import uuid
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class ClickHouseDumper:

    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "nikki17"),
            database=os.getenv("CLICKHOUSE_DB", "data_quality")
        )
        print("✅ ClickHouse connected")

    def dump_bulk(self, results: list):
        rows = []
        for r in results:
            total  = r.get("total_rows", 0)
            failed = r.get("failed_rows", 0)
            pct    = round(((total - failed) / total * 100), 2) if total > 0 else 0.0

            rows.append([
                str(uuid.uuid4()),
                datetime.utcnow(),
                os.getenv("DEPLOYMENT_ID", "manual"),
                r.get("database_name", ""),
                r.get("table_name", ""),
                r.get("check_name", ""),
                r.get("status", "UNKNOWN"),
                total,
                failed,
                pct,
                r.get("failure_reason", "")[:500],
                os.getenv("ENVIRONMENT", "production"),
                os.getenv("TRIGGERED_BY", "manual")
            ])

        self.client.insert(
            "results",
            rows,
            column_names=[
                "check_id", "run_timestamp", "deployment_id",
                "database_name", "table_name", "check_name",
                "status", "total_rows", "failed_rows",
                "success_percent", "failure_reason",
                "environment", "triggered_by"
            ]
        )
        print(f"📤 {len(rows)} results dumped to ClickHouse")
