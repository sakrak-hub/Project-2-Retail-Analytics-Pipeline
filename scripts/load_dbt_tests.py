import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

import duckdb



parser = argparse.ArgumentParser(description="Load dbt test results into DuckDB.")
parser.add_argument("--input",  default="/opt/airflow/dbt/target/run_results.json", help="Path to run_results.json")
parser.add_argument("--db",     default="/opt/airflow/dbt/dbt_results.duckdb", help="DuckDB database file (use :memory: for in-memory)")
parser.add_argument("--table",  default="dbt_test_results", help="Target table name")
args = parser.parse_args()



print(f"Reading {args.input} ...")
with open(args.input) as f:
    raw = json.load(f)


if "stdout" in raw:
    data = json.loads(raw["stdout"])
else:
    data = raw

metadata = data.get("metadata", {})
results  = data.get("results", [])


test_results = [r for r in results if r.get("unique_id", "").startswith("test.")]
print(f"Found {len(test_results)} dbt test results (out of {len(results)} total).")



def get_timing(timing_list: list, phase: str, field: str):
    """Extract a timing field (started_at / completed_at) for a given phase."""
    for t in timing_list:
        if t.get("name") == phase:
            val = t.get(field)
            return datetime.fromisoformat(val.replace("Z", "+00:00")) if val else None
    return None


rows = []
for r in test_results:
    timing = r.get("timing", [])
    rows.append({
        
        "invocation_id":         metadata.get("invocation_id"),
        "invocation_started_at": datetime.fromisoformat(
            metadata["invocation_started_at"].replace("Z", "+00:00")
        ) if metadata.get("invocation_started_at") else None,
        "generated_at":          datetime.fromisoformat(
            metadata["generated_at"].replace("Z", "+00:00")
        ) if metadata.get("generated_at") else None,
        "dbt_version":           metadata.get("dbt_version"),

        
        "unique_id":    r.get("unique_id"),
        "test_name":    r.get("unique_id", "").split(".")[-1],   
        "thread_id":    r.get("thread_id"),

        
        "status":         r.get("status"),
        "failures":       r.get("failures"),
        "message":        r.get("message"),
        "adapter_message": (r.get("adapter_response") or {}).get("_message"),

        
        "execution_time_s": r.get("execution_time"),

        
        "compile_started_at":   get_timing(timing, "compile", "started_at"),
        "compile_completed_at": get_timing(timing, "compile", "completed_at"),

        
        "execute_started_at":   get_timing(timing, "execute", "started_at"),
        "execute_completed_at": get_timing(timing, "execute", "completed_at"),

        
        "compiled":      r.get("compiled"),
        "compiled_code": r.get("compiled_code"),
    })



print(f"Connecting to DuckDB: {args.db}")
con = duckdb.connect(args.db)

con.execute(f"""
    CREATE OR REPLACE TABLE {args.table} (
        -- Metadata
        invocation_id         VARCHAR,
        invocation_started_at TIMESTAMPTZ,
        generated_at          TIMESTAMPTZ,
        dbt_version           VARCHAR,

        -- Test identity
        unique_id             VARCHAR,
        test_name             VARCHAR,
        thread_id             VARCHAR,

        -- Outcome
        status                VARCHAR,
        failures              INTEGER,
        message               VARCHAR,
        adapter_message       VARCHAR,

        -- Performance
        execution_time_s      DOUBLE,

        -- Timing: compile phase
        compile_started_at    TIMESTAMPTZ,
        compile_completed_at  TIMESTAMPTZ,

        -- Timing: execute phase
        execute_started_at    TIMESTAMPTZ,
        execute_completed_at  TIMESTAMPTZ,

        -- SQL
        compiled              BOOLEAN,
        compiled_code         VARCHAR
    )
""")

con.executemany(
    f"""
    INSERT INTO {args.table} VALUES (
        $invocation_id, $invocation_started_at, $generated_at, $dbt_version,
        $unique_id, $test_name, $thread_id,
        $status, $failures, $message, $adapter_message,
        $execution_time_s,
        $compile_started_at, $compile_completed_at,
        $execute_started_at, $execute_completed_at,
        $compiled, $compiled_code
    )
    """,
    rows,
)

print(f"\n✅ Loaded {len(rows)} rows into '{args.table}'.\n")



summary = con.execute(f"""
    SELECT
        status,
        COUNT(*)            AS test_count,
        SUM(failures)       AS total_failures,
        ROUND(AVG(execution_time_s), 4) AS avg_exec_time_s
    FROM {args.table}
    GROUP BY status
    ORDER BY test_count DESC
""").fetchdf()

print("── Summary by status ──────────────────────────────")
print(summary.to_string(index=False))

slowest = con.execute(f"""
    SELECT test_name, status, failures, ROUND(execution_time_s, 4) AS exec_s
    FROM {args.table}
    ORDER BY execution_time_s DESC
    LIMIT 5
""").fetchdf()

print("\n── Top 5 slowest tests ────────────────────────────")
print(slowest.to_string(index=False))

con.close()
print(f"\nDatabase saved to: {args.db}")