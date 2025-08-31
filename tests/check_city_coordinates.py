import duckdb

con = duckdb.connect('meteopanda.duckdb')

print("=== VERIFICACIÓN DE COORDENADAS POR CIUDAD ===\n")

# 1. Verificar coordenadas por ciudad en weather_raw
print("1️⃣ COORDENADAS POR CIUDAD EN WEATHER_RAW:")
try:
    city_coords = con.execute("""
        SELECT city, source, COUNT(*) as registros,
               MIN(lat) as lat_min, MAX(lat) as lat_max,
               MIN(lon) as lon_min, MAX(lon) as lon_max
        FROM weather_raw.weather_data 
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        GROUP BY city, source
        ORDER BY city, source
    """).fetchall()
    
    if city_coords:
        print(f"   ✅ {len(city_coords)} combinaciones ciudad-fuente con coordenadas:")
        for row in city_coords:
            city, source, count, lat_min, lat_max, lon_min, lon_max = row
            print(f"      - {city} ({source}): {count:,} reg, lat: {lat_min:.4f}-{lat_max:.4f}, lon: {lon_min:.4f}-{lon_max:.4f}")
    else:
        print("   ❌ No hay coordenadas por ciudad en weather_raw")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

print()

# 2. Verificar que todas las ciudades tengan coordenadas
print("2️⃣ VERIFICACIÓN DE COBERTURA POR CIUDAD:")
try:
    city_coverage = con.execute("""
        SELECT 
            city,
            COUNT(*) as total_registros,
            COUNT(CASE WHEN lat IS NOT NULL AND lon IS NOT NULL THEN 1 END) as con_coordenadas,
            ROUND(COUNT(CASE WHEN lat IS NOT NULL AND lon IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as porcentaje
        FROM weather_raw.weather_data 
        GROUP BY city
        ORDER BY city
    """).fetchall()
    
    print("   📊 Cobertura de coordenadas por ciudad:")
    for row in city_coverage:
        city, total, con_coords, percentage = row
        status = "✅" if percentage == 100.0 else "⚠️" if percentage > 0 else "❌"
        print(f"      {status} {city}: {total:,} total, {con_coords:,} con coordenadas ({percentage}%)")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

print()

# 3. Verificar fuentes de datos
print("3️⃣ VERIFICACIÓN POR FUENTE:")
try:
    source_coverage = con.execute("""
        SELECT 
            source,
            COUNT(*) as total_registros,
            COUNT(CASE WHEN lat IS NOT NULL AND lon IS NOT NULL THEN 1 END) as con_coordenadas,
            ROUND(COUNT(CASE WHEN lat IS NOT NULL AND lon IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as porcentaje
        FROM weather_raw.weather_data 
        GROUP BY source
        ORDER BY source
    """).fetchall()
    
    print("   📊 Cobertura de coordenadas por fuente:")
    for row in source_coverage:
        source, total, con_coords, percentage = row
        status = "✅" if percentage == 100.0 else "⚠️" if percentage > 0 else "❌"
        print(f"      {status} {source}: {total:,} total, {con_coords:,} con coordenadas ({percentage}%)")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

print()

# 4. Resumen final
print("4️⃣ RESUMEN FINAL:")
try:
    total_records = con.execute("SELECT COUNT(*) FROM weather_raw.weather_data").fetchone()[0]
    total_with_coords = con.execute("""
        SELECT COUNT(*) FROM weather_raw.weather_data 
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """).fetchone()[0]
    
    print(f"   🎯 Total de registros: {total_records:,}")
    print(f"   🎯 Con coordenadas: {total_with_coords:,}")
    print(f"   🎯 Porcentaje: {total_with_coords/total_records*100:.1f}%")
    
    if total_with_coords == total_records:
        print("   🎉 ¡TODAS LAS CIUDADES TIENEN COORDENADAS!")
    else:
        print("   ⚠️ Algunas ciudades no tienen coordenadas completas")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

con.close()
print("\n✅ Verificación completada")
