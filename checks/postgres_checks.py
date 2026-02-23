import sys
import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
import great_expectations as gx
from great_expectations.dataset import PandasDataset

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.clickhouse_dumper import ClickHouseDumper

load_dotenv()

def run():
    print("\n" + "="*50)
    print("🔍 Running PostgreSQL Checks")
    print("="*50)

    dumper  = ClickHouseDumper()
    results = []

    # ── Fetch data ─────────────────────────────────────────
    engine = sqlalchemy.create_engine(os.getenv("POSTGRES_CONNECTION"))
    df = pd.read_sql("SELECT * FROM orders", engine)

    dataset = PandasDataset(df)
    total   = len(df)

    # ── Define checks ──────────────────────────────────────
    checks = [
        ("expect_column_to_exist",               dataset.expect_column_to_exist("order_id")),
        ("expect_order_id_not_null",             dataset.expect_column_values_to_not_be_null("order_id")),
        ("expect_customer_id_not_null",          dataset.expect_column_values_to_not_be_null("customer_id")),
        ("expect_order_id_unique",               dataset.expect_column_values_to_be_unique("order_id")),
        ("expect_amount_between_0_and_999999",   dataset.expect_column_values_to_be_between("amount", 0, 999999)),
        ("expect_status_valid_values",           dataset.expect_column_values_to_be_in_set("status", ["pending","completed","cancelled"])),
        ("expect_row_count_min_1",               dataset.expect_table_row_count_to_be_between(min_value=1)),
    ]

    # ── Parse results ──────────────────────────────────────
    for check_name, result in checks:
        success      = result.success
        failed_rows  = result.result.get("unexpected_count", 0)
        failure_info = str(result.result.get("unexpected_list", ""))

        status = "PASSED" if success else "FAILED"
        icon   = "✅" if success else "❌"
        print(f"{icon} {check_name}: {status}")

        results.append({
            "database_name":  "postgresql",
            "table_name":     "orders",
            "check_name":     check_name,
            "status":         status,
            "total_rows":     total,
            "failed_rows":    failed_rows,
            "failure_reason": failure_info if not success else ""
        })

    dumper.dump_bulk(results)

    failed = [r for r in results if r["status"] == "FAILED"]
    return len(failed) == 0

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
