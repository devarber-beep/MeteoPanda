import duckdb

con = duckdb.connect('meteopanda.duckdb')

print("=== LIMPIEZA DE WEATHER_RAW ===")

# Verificar tablas actuales en weather_raw
tables = con.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'weather_raw'
    ORDER BY table_name
""").fetchall()

print("Tablas actuales en weather_raw:")
for table in tables:
    table_name = table[0]
    count = con.execute(f"SELECT COUNT(*) FROM weather_raw.{table_name}").fetchone()[0]
    print(f"  - {table_name}: {count} registros")

# Eliminar weather_datas si existe
if any(table[0] == 'weather_datas' for table in tables):
    print("\nEliminando tabla weather_datas...")
    con.execute("DROP TABLE IF EXISTS weather_raw.weather_datas")
    print("✅ Tabla weather_datas eliminada")

# Verificar tablas finales
final_tables = con.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'weather_raw'
    ORDER BY table_name
""").fetchall()

print("\nTablas finales en weather_raw:")
for table in final_tables:
    table_name = table[0]
    count = con.execute(f"SELECT COUNT(*) FROM weather_raw.{table_name}").fetchone()[0]
    print(f"  - {table_name}: {count} registros")

con.close()
print("\n✅ Limpieza de weather_raw completada")
