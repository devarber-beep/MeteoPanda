import duckdb
from src.transform.transform import generate_union_all_from_bronze

con = duckdb.connect('meteopanda.duckdb')

print("=== DEBUG UNION ALL ===")

# Verificar tablas en weather_raw
tables = con.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'weather_raw'
    ORDER BY table_name
""").fetchall()

print("Tablas en weather_raw:")
for table in tables:
    table_name = table[0]
    print(f"  - {table_name}")
    
    # Mostrar columnas de cada tabla
    columns = con.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'weather_raw' AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """).fetchall()
    
    print(f"    Columnas ({len(columns)}):")
    for col in columns:
        print(f"      * {col[0]} ({col[1]})")
    
    # Mostrar número de registros
    count = con.execute(f"SELECT COUNT(*) FROM weather_raw.{table_name}").fetchone()[0]
    print(f"    Registros: {count}")

# Generar el UNION ALL
print("\nGenerando UNION ALL...")
union_sql = generate_union_all_from_bronze(con)
print(f"SQL generado:\n{union_sql}")

# Probar el UNION ALL
print("\nProbando UNION ALL...")
try:
    result = con.execute(union_sql).fetchall()
    print(f"✅ UNION ALL exitoso: {len(result)} filas")
except Exception as e:
    print(f"❌ Error en UNION ALL: {e}")

con.close()
