import duckdb

con = duckdb.connect('meteopanda.duckdb')

print("Tablas en gold:")
result = con.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'gold' 
    ORDER BY table_name
""").fetchall()

for table in result:
    print(f"  - {table[0]}")

con.close()
