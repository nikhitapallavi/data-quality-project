import sys, os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from great_expectations.dataset import PandasDataset

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.clickhouse_dumper import ClickHouseDumper

load_dotenv()

def run():
    print("\n" + "="*50)
    print("🔍 Running MySQL Checks")
    print("="*50)

    dumper  = ClickHouseDumper()
    results = []

    engine  = sqlalchemy.create_engine(os.getenv("MYSQL_CONNECTION"))
    df      = pd.read_sql("SELECT * FROM users", engine)
    dataset = PandasDataset(df)
    total   = len(df)

    checks = [
        ("expect_user_id_not_null",     dataset.expect_column_values_to_not_be_null("user_id")),
        ("expect_email_not_null",       dataset.expect_column_values_to_not_be_null("email")),
        ("expect_user_id_unique",       dataset.expect_column_values_to_be_unique("user_id")),
        ("expect_role_valid_values",    dataset.expect_column_values_to_be_in_set("role", ["admin","user","moderator","guest"])),
        ("expect_username_length_3_50", dataset.expect_column_value_lengths_to_be_between("username", 3, 50)),
        ("expect_row_count_min_1",      dataset.expect_table_row_count_to_be_between(min_value=1)),
    ]

    for check_name, result in checks:
        success     = result.success
        failed_rows = result.result.get("unexpected_count", 0)
        status      = "PASSED" if success else "FAILED"
        icon        = "✅" if success else "❌"
        print(f"{icon} {check_name}: {status}")

        results.append({
            "database_name":  "mysql",
            "table_name":     "users",
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
