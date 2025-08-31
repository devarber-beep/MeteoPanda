import duckdb

# Conectar a la base de datos
con = duckdb.connect('meteopanda.duckdb')

print("=== ESQUEMAS DISPONIBLES ===")
schemas = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
for schema in schemas:
    print(f"  - {schema[0]}")

print("\n=== TABLAS EN WEATHER_RAW ===")
tables_raw = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'weather_raw'").fetchall()
if tables_raw:
    for table in tables_raw:
        print(f"  - {table[0]}")
        # Mostrar estructura de la tabla
        columns = con.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'weather_raw' AND table_name = '{table[0]}'").fetchall()
        for col in columns:
            print(f"    * {col[0]} ({col[1]})")
        # Mostrar número de registros
        count = con.execute(f"SELECT COUNT(*) FROM weather_raw.{table[0]}").fetchone()[0]
        print(f"    * Registros: {count}")
else:
    print("  ❌ No hay tablas en weather_raw")

print("\n=== TABLAS EN BRONZE ===")
tables_bronze = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").fetchall()
if tables_bronze:
    for table in tables_bronze:
        print(f"  - {table[0]}")
else:
    print("  ❌ No hay tablas en bronze")

print("\n=== TODAS LAS TABLAS ===")
all_tables = con.execute("SELECT table_schema, table_name FROM information_schema.tables ORDER BY table_schema, table_name").fetchall()
for schema, table in all_tables:
    print(f"  - {schema}.{table}")

con.close()
