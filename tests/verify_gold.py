import duckdb

con = duckdb.connect('meteopanda.duckdb')

print("=== VERIFICACIÓN DE CAPA GOLD ===")

# Verificar tablas en gold
tables = con.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'gold'
    ORDER BY table_name
""").fetchall()

print("Tablas en gold:")
for table in tables:
    table_name = table[0]
    count = con.execute(f"SELECT COUNT(*) FROM gold.{table_name}").fetchone()[0]
    print(f"  - {table_name}: {count} registros")

# Verificar distribución por fuente en algunos datamarts clave
print("\n=== VERIFICACIÓN DE FUENTES EN DATAMARTS ===")

# Verificar basics_datamarts
print("\n1. Basics Datamarts:")
try:
    basics_sources = con.execute("""
        SELECT 
            source,
            COUNT(*) as registros,
            COUNT(DISTINCT city) as ciudades
        FROM gold.basics_datamarts
        GROUP BY source
        ORDER BY source
    """).fetchall()
    
    for row in basics_sources:
        source, count, cities = row
        print(f"  - {source}: {count} registros, {cities} ciudades")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Verificar climate_profiles
print("\n2. Climate Profiles:")
try:
    climate_sources = con.execute("""
        SELECT 
            source,
            COUNT(*) as registros,
            COUNT(DISTINCT city) as ciudades
        FROM gold.climate_profiles
        GROUP BY source
        ORDER BY source
    """).fetchall()
    
    for row in climate_sources:
        source, count, cities = row
        print(f"  - {source}: {count} registros, {cities} ciudades")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Verificar monthly_stats
print("\n3. Monthly Stats:")
try:
    monthly_sources = con.execute("""
        SELECT 
            source,
            COUNT(*) as registros,
            COUNT(DISTINCT city) as ciudades
        FROM gold.monthly_stats
        GROUP BY source
        ORDER BY source
    """).fetchall()
    
    for row in monthly_sources:
        source, count, cities = row
        print(f"  - {source}: {count} registros, {cities} ciudades")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Verificar weather_trends
print("\n4. Weather Trends:")
try:
    trends_sources = con.execute("""
        SELECT 
            source,
            COUNT(*) as registros,
            COUNT(DISTINCT city) as ciudades
        FROM gold.weather_trends
        GROUP BY source
        ORDER BY source
    """).fetchall()
    
    for row in trends_sources:
        source, count, cities = row
        print(f"  - {source}: {count} registros, {cities} ciudades")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Mostrar ejemplos de datos de cada fuente en basics_datamarts
print("\n=== EJEMPLOS DE DATOS POR FUENTE ===")

try:
    meteostat_sample = con.execute("""
        SELECT city, source, region, year, month, avg_temp, min_temp, max_temp, total_precip
        FROM gold.basics_datamarts
        WHERE source = 'meteostat'
        ORDER BY city, year, month
        LIMIT 3
    """).fetchall()
    
    print("\nMeteostat (basics_datamarts):")
    for row in meteostat_sample:
        print(f"  {row}")
        
    aemet_sample = con.execute("""
        SELECT city, source, region, year, month, avg_temp, min_temp, max_temp, total_precip
        FROM gold.basics_datamarts
        WHERE source = 'aemet'
        ORDER BY city, year, month
        LIMIT 3
    """).fetchall()
    
    print("\nAEMET (basics_datamarts):")
    for row in aemet_sample:
        print(f"  {row}")
        
except Exception as e:
    print(f"❌ Error mostrando ejemplos: {e}")

con.close()
########################################################