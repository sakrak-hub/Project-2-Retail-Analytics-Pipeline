import duckdb
import os

def sync_bi_database():
    
    SOURCE_DB = "/mnt/d/Projects/Project-2-Retail-Analytics-Pipeline/retailitics_dbt/warehouse.duckdb"
    TARGET_DB = "/mnt/d/Projects/Project-2-Retail-Analytics-Pipeline/retailitics_dbt/warehouse_bi.duckdb"
    
    while True:
        try:
            target_conn = duckdb.connect(SOURCE_DB)
            print(target_conn.sql("SHOW TABLES").fetchall())
            print("DB unlocked!")
            target_conn.close()
            break
        except duckdb.IOException as e:
            pass
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

if __name__=="__main__":
    sync_bi_database()
    print
