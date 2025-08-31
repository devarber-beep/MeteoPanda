import duckdb

con = duckdb.connect('meteopanda.duckdb')

print("=== VERIFICACIÓN DE COORDENADAS ===\n")

# 1. Verificar coordenadas en weather_raw
print("1️⃣ COORDENADAS EN WEATHER_RAW:")
try:
    coords_raw = con.execute("""
        SELECT city, station, lat, lon, source, COUNT(*) as registros
        FROM weather_raw.weather_data 
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        GROUP BY city, station, lat, lon, source
        ORDER BY city, source
    """).fetchall()
    
    if coords_raw:
        print(f"   ✅ {len(coords_raw)} combinaciones ciudad-estación con coordenadas:")
        for row in coords_raw:
            print(f"      - {row[0]} ({row[1]}): {row[2]:.4f}, {row[3]:.4f} - {row[4]} ({row[5]} reg)")
    else:
        print("   ❌ No hay coordenadas en weather_raw")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

print()

# 2. Verificar coordenadas en silver
print("2️⃣ COORDENADAS EN SILVER:")
try:
    coords_silver = con.execute("""
        SELECT city, station, lat, lon, source, COUNT(*) as registros
        FROM silver.weather_cleaned 
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        GROUP BY city, station, lat, lon, source
        ORDER BY city, source
    """).fetchall()
    
    if coords_silver:
        print(f"   ✅ {len(coords_silver)} combinaciones ciudad-estación con coordenadas:")
        for row in coords_silver:
            print(f"      - {row[0]} ({row[1]}): {row[2]:.4f}, {row[3]:.4f} - {row[4]} ({row[5]} reg)")
    else:
        print("   ❌ No hay coordenadas en silver")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

print()

# 3. Resumen de datos
print("3️⃣ RESUMEN DE DATOS:")
try:
    # Contar registros totales
    total_raw = con.execute("SELECT COUNT(*) FROM weather_raw.weather_data").fetchone()[0]
    total_silver = con.execute("SELECT COUNT(*) FROM silver.weather_cleaned").fetchone()[0]
    
    # Contar registros con coordenadas
    with_coords_raw = con.execute("SELECT COUNT(*) FROM weather_raw.weather_data WHERE lat IS NOT NULL AND lon IS NOT NULL").fetchone()[0]
    with_coords_silver = con.execute("SELECT COUNT(*) FROM silver.weather_cleaned WHERE lat IS NOT NULL AND lon IS NOT NULL").fetchone()[0]
    
    print(f"   📊 Raw: {total_raw:,} registros totales, {with_coords_raw:,} con coordenadas ({with_coords_raw/total_raw*100:.1f}%)")
    print(f"   📊 Silver: {total_silver:,} registros totales, {with_coords_silver:,} con coordenadas ({with_coords_silver/total_silver*100:.1f}%)")
    
    # Verificar fuentes de datos
    sources_raw = con.execute("""
        SELECT source, COUNT(*) as total, 
               COUNT(CASE WHEN lat IS NOT NULL AND lon IS NOT NULL THEN 1 END) as con_coords
        FROM weather_raw.weather_data 
        GROUP BY source
    """).fetchall()
    
    print(f"   📊 Fuentes de datos:")
    for source, total, con_coords in sources_raw:
        percentage = (con_coords/total*100) if total > 0 else 0
        print(f"      - {source}: {total:,} total, {con_coords:,} con coordenadas ({percentage:.1f}%)")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

con.close()
print("\n✅ Verificación completada")
