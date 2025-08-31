import duckdb

con = duckdb.connect('meteopanda.duckdb')

print("=== VERIFICACIÓN DE DATOS PARA DASHBOARD ===\n")

# 1. Verificar esquemas disponibles
print("1️⃣ ESQUEMAS DISPONIBLES:")
schemas = con.execute("""
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name NOT IN ('information_schema', 'main', 'pg_catalog')
    ORDER BY schema_name
""").fetchall()

for schema in schemas:
    print(f"   - {schema[0]}")

print()

# 2. Verificar tablas en gold
print("2️⃣ TABLAS EN GOLD:")
try:
    gold_tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'gold'
        ORDER BY table_name
    """).fetchall()
    
    for table in gold_tables:
        print(f"   - {table[0]}")
        
        # Verificar columnas de cada tabla
        columns = con.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'gold' AND table_name = '{table[0]}'
            ORDER BY ordinal_position
        """).fetchall()
        
        for col in columns:
            print(f"     └─ {col[0]} ({col[1]})")
        print()
        
except Exception as e:
    print(f"   ❌ Error: {e}")

# 3. Verificar columnas en silver.weather_cleaned
print("3️⃣ COLUMNAS EN SILVER.WEATHER_CLEANED:")
try:
    silver_columns = con.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'silver' AND table_name = 'weather_cleaned'
        ORDER BY ordinal_position
    """).fetchall()
    
    for col in silver_columns:
        print(f"   - {col[0]} ({col[1]})")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

# 4. Verificar si hay coordenadas en alguna tabla
print("\n4️⃣ BUSCANDO COORDENADAS:")
try:
    # Buscar en todas las tablas
    all_tables = con.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'main', 'pg_catalog')
        ORDER BY table_schema, table_name
    """).fetchall()
    
    for schema, table in all_tables:
        try:
            columns = con.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                AND column_name IN ('lat', 'lon', 'latitude', 'longitude')
            """).fetchall()
            
            if columns:
                print(f"   ✅ {schema}.{table}: {[col[0] for col in columns]}")
                
        except:
            continue
            
except Exception as e:
    print(f"   ❌ Error: {e}")

# 5. Verificar datos de ejemplo
print("\n5️⃣ MUESTRA DE DATOS:")
try:
    # Datos de silver
    silver_sample = con.execute("""
        SELECT * FROM silver.weather_cleaned LIMIT 3
    """).fetchall()
    
    if silver_sample:
        print("   📊 silver.weather_cleaned (primeras 3 filas):")
        for i, row in enumerate(silver_sample):
            print(f"      Fila {i+1}: {row}")
    
    # Datos de gold
    if gold_tables:
        first_gold_table = gold_tables[0][0]
        gold_sample = con.execute(f"""
            SELECT * FROM gold.{first_gold_table} LIMIT 3
        """).fetchall()
        
        if gold_sample:
            print(f"   📊 gold.{first_gold_table} (primeras 3 filas):")
            for i, row in enumerate(gold_sample):
                print(f"      Fila {i+1}: {row}")
                
except Exception as e:
    print(f"   ❌ Error: {e}")

con.close()
print("\n✅ Verificación completada")
