import duckdb

# Conectar a la base de datos
con = duckdb.connect("data/meteopanda.duckdb")

print("🔍 Verificando base de datos...")

# Verificar esquemas
schemas = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
print("Esquemas:", schemas)

# Verificar tablas
for schema in schemas:
    schema_name = schema[0]
    tables = con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'").fetchall()
    print(f"Tablas en {schema_name}:", tables)

con.close()
