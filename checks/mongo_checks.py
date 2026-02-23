import sys, os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from great_expectations.dataset import PandasDataset

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.clickhouse_dumper import ClickHouseDumper

load_dotenv()

def run():
    print("\n" + "="*50)
    print("🔍 Running MongoDB Checks")
    print("="*50)

    dumper  = ClickHouseDumper()
    results = []

    # ── Fetch MongoDB data ─────────────────────────────────
    client  = MongoClient(os.getenv("MONGO_CONNECTION"))
    db      = client["dqdb"]
    df      = pd.DataFrame(list(db.products.find({}, {"_id": 0})))
    dataset = PandasDataset(df)
    total   = len(df)

    checks = [
        ("expect_product_id_not_null",  dataset.expect_column_values_to_not_be_null("product_id")),
        ("expect_name_not_null",        dataset.expect_column_values_to_not_be_null("name")),
        ("expect_price_positive",       dataset.expect_column_values_to_be_between("price", min_value=0.01)),
        ("expect_stock_non_negative",   dataset.expect_column_values_to_be_between("stock", min_value=0)),
        ("expect_product_id_unique",    dataset.expect_column_values_to_be_unique("product_id")),
        ("expect_row_count_min_1",      dataset.expect_table_row_count_to_be_between(min_value=1)),
    ]

    for check_name, result in checks:
        success     = result.success
        failed_rows = result.result.get("unexpected_count", 0)
        status      = "PASSED" if success else "FAILED"
        icon        = "✅" if success else "❌"
        print(f"{icon} {check_name}: {status}")

        results.append({
            "database_name":  "mongodb",
            "table_name":     "products",
            "check_name":     check_name,
            "status":         status,
            "total_rows":     total,
            "failed_rows":    failed_rows,
            "failure_reason": str(result.result.get("unexpected_list", "")) if not success else ""
        })

    dumper.dump_bulk(results)

    failed = [r for r in results if r["status"] == "FAILED"]
    return len(failed) == 0

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
