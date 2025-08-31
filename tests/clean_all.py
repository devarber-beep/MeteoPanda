import duckdb

con = duckdb.connect('meteopanda.duckdb')

print("=== LIMPIEZA COMPLETA DE BASE DE DATOS ===")

# Obtener todos los esquemas
schemas = con.execute("""
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name NOT IN ('information_schema')
    ORDER BY schema_name
""").fetchall()

print("Esquemas a eliminar:")
for schema in schemas:
    print(f"  - {schema[0]}")

# Eliminar cada esquema
for schema in schemas:
    schema_name = schema[0]
    try:
        # Eliminar todas las tablas del esquema
        tables = con.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}'
        """).fetchall()
        
        for table in tables:
            table_name = table[0]
            con.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
            print(f"  ✅ Tabla {schema_name}.{table_name} eliminada")
        
        # Eliminar el esquema
        con.execute(f"DROP SCHEMA IF EXISTS {schema_name}")
        print(f"✅ Esquema {schema_name} eliminado")
        
    except Exception as e:
        print(f"❌ Error eliminando esquema {schema_name}: {e}")

# Verificar esquemas restantes
remaining_schemas = con.execute("""
    SELECT schema_name 
    FROM information_schema.schemata 
    ORDER BY schema_name
""").fetchall()

print("\nEsquemas restantes:")
for schema in remaining_schemas:
    print(f"  - {schema[0]}")

con.close()
print("\n✅ Limpieza completa finalizada")
print("La base de datos está lista para una nueva descarga con los campos actualizados")
