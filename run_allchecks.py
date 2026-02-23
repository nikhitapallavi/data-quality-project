import sys
from checks import postgres_checks, mysql_checks, mongo_checks

print("\n" + "🚀 "*15)
print("   STARTING DATA QUALITY CHECKS")
print("🚀 "*15)

checks = [
    ("PostgreSQL", postgres_checks.run),
    ("MySQL",      mysql_checks.run),
    ("MongoDB",    mongo_checks.run),
]

summary = {}

for db_name, check_fn in checks:
    try:
        passed = check_fn()
        summary[db_name] = "✅ PASSED" if passed else "❌ FAILED"
    except Exception as e:
        print(f"💥 {db_name} crashed: {e}")
        summary[db_name] = "💥 ERROR"

# ── Print Summary ──────────────────────────────────────────
print("\n" + "="*50)
print("📊 FINAL SUMMARY")
print("="*50)
for db, status in summary.items():
    print(f"  {status}  →  {db}")

# ── Exit code for CI/CD ────────────────────────────────────
if all("PASSED" in s for s in summary.values()):
    print("\n🎉 ALL CHECKS PASSED — Deployment is clean!")
    sys.exit(0)
else:
    print("\n🚨 SOME CHECKS FAILED — Review ClickHouse results!")
    sys.exit(1)
