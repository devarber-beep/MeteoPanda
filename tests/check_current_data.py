import duckdb

con = duckdb.connect('meteopanda.duckdb')

print("=== VERIFICACIÓN DE DATOS ACTUALES ===\n")

# 1. Verificar esquemas disponibles
print("1️⃣ ESQUEMAS DISPONIBLES:")
try:
    schemas = con.execute("""
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name LIKE 'weather_raw_%'
        ORDER BY schema_name DESC
    """).fetchall()
    
    if schemas:
        print(f"   📁 {len(schemas)} esquemas DLT encontrados:")
        for schema in schemas:
            print(f"      - {schema[0]}")
    else:
        print("   ❌ No hay esquemas DLT")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

print()

# 2. Verificar datos en el esquema más reciente
print("2️⃣ DATOS EN ESQUEMA MÁS RECIENTE:")
try:
    if schemas:
        latest_schema = schemas[0][0]
        print(f"   📊 Esquema: {latest_schema}")
        
        # Contar registros
        count = con.execute(f"SELECT COUNT(*) FROM {latest_schema}.weather_data").fetchone()[0]
        print(f"   📊 Total registros: {count:,}")
        
        # Verificar coordenadas
        coords_count = con.execute(f"""
            SELECT COUNT(*) 
            FROM {latest_schema}.weather_data 
            WHERE lat IS NOT NULL AND lon IS NOT NULL
        """).fetchone()[0]
        
        print(f"   📍 Con coordenadas: {coords_count:,} ({coords_count/count*100:.1f}%)")
        
        # Verificar por ciudad
        city_data = con.execute(f"""
            SELECT 
                city,
                COUNT(*) as total,
                COUNT(CASE WHEN lat IS NOT NULL AND lon IS NOT NULL THEN 1 END) as con_coords
            FROM {latest_schema}.weather_data 
            GROUP BY city
            ORDER BY city
        """).fetchall()
        
        print("   📊 Por ciudad:")
        for city, total, con_coords in city_data:
            status = "✅" if con_coords == total else "⚠️" if con_coords > 0 else "❌"
            print(f"      {status} {city}: {con_coords}/{total} ({con_coords/total*100:.1f}%)")
        
    else:
        print("   ❌ No hay datos disponibles")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

print()

# 3. Verificar esquema weather_raw
print("3️⃣ ESQUEMA WEATHER_RAW:")
try:
    weather_raw_exists = con.execute("""
        SELECT COUNT(*) 
        FROM information_schema.schemata 
        WHERE schema_name = 'weather_raw'
    """).fetchone()[0]
    
    if weather_raw_exists:
        print("   ✅ Esquema weather_raw existe")
        
        # Verificar tabla
        table_exists = con.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'weather_raw' AND table_name = 'weather_data'
        """).fetchone()[0]
        
        if table_exists:
            print("   ✅ Tabla weather_data existe")
            
            # Contar registros
            count = con.execute("SELECT COUNT(*) FROM weather_raw.weather_data").fetchone()[0]
            print(f"   📊 Total registros: {count:,}")
            
            # Verificar coordenadas
            coords_count = con.execute("""
                SELECT COUNT(*) 
                FROM weather_raw.weather_data 
                WHERE lat IS NOT NULL AND lon IS NOT NULL
            """).fetchone()[0]
            
            print(f"   📍 Con coordenadas: {coords_count:,} ({coords_count/count*100:.1f}%)")
            
        else:
            print("   ❌ Tabla weather_data no existe")
    else:
        print("   ❌ Esquema weather_raw no existe")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

con.close()
print("\n✅ Verificación completada")
