#!/usr/bin/env python3
"""
Script de prueba para ejecutar el pipeline completo paso a paso.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from pathlib import Path
from src.utils.db import get_connection
from src.transform.transform import run_sql_transformations

def test_database_connection():
    """Prueba la conexión a la base de datos"""
    print("🔍 Probando conexión a la base de datos...")
    try:
        con = get_connection()
        print("✅ Conexión exitosa")
        
        # Verificar esquemas
        schemas = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
        print(f"📋 Esquemas disponibles: {[s[0] for s in schemas]}")
        
        con.close()
        return True
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return False

def test_data_availability():
    """Verifica si hay datos disponibles para transformar"""
    print("\n📊 Verificando disponibilidad de datos...")
    try:
        con = get_connection()
        
        # Verificar datos en weather_raw
        weather_tables = con.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'weather_raw'
        """).fetchall()
        
        if weather_tables:
            print(f"✅ Encontradas {len(weather_tables)} tablas en weather_raw:")
            for table in weather_tables:
                count = con.execute(f"SELECT COUNT(*) FROM weather_raw.{table[0]}").fetchone()[0]
                print(f"  - {table[0]}: {count} registros")
            con.close()
            return True
        else:
            print("⚠️  No hay tablas en weather_raw")
            
            # Verificar datos en bronze como fallback
            bronze_tables = con.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'bronze'
            """).fetchall()
            
            if bronze_tables:
                print(f"✅ Encontradas {len(bronze_tables)} tablas en bronze:")
                for table in bronze_tables:
                    count = con.execute(f"SELECT COUNT(*) FROM bronze.{table[0]}").fetchone()[0]
                    print(f"  - {table[0]}: {count} registros")
                con.close()
                return True
            else:
                print("❌ No hay datos disponibles para transformar")
                con.close()
                return False
                
    except Exception as e:
        print(f"❌ Error verificando datos: {e}")
        return False

def test_sql_transformations():
    """Prueba las transformaciones SQL"""
    print("\n🔄 Probando transformaciones SQL...")
    try:
        sql_path = Path("src/transform/sql")
        
        # Verificar que existen archivos SQL
        sql_files = list(sql_path.glob("*.sql"))
        if not sql_files:
            print("❌ No se encontraron archivos SQL en src/transform/sql")
            return False
        
        print(f"✅ Encontrados {len(sql_files)} archivos SQL:")
        for sql_file in sql_files:
            print(f"  - {sql_file.name}")
        
        # Ejecutar transformaciones
        print("\n🚀 Ejecutando transformaciones...")
        run_sql_transformations(sql_path)
        
        print("✅ Transformaciones SQL ejecutadas correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error en transformaciones SQL: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_gold_transformations():
    """Prueba las transformaciones de la capa gold"""
    print("\n✨ Probando transformaciones Gold...")
    try:
        sql_path = Path("src/transform/sql/datamarts")
        
        # Verificar que existen archivos SQL de datamarts
        sql_files = list(sql_path.glob("*.sql"))
        if not sql_files:
            print("❌ No se encontraron archivos SQL en src/transform/sql/datamarts")
            return False
        
        print(f"✅ Encontrados {len(sql_files)} archivos SQL de datamarts:")
        for sql_file in sql_files:
            print(f"  - {sql_file.name}")
        
        # Ejecutar transformaciones gold
        print("\n🚀 Ejecutando transformaciones Gold...")
        run_sql_transformations(sql_path)
        
        print("✅ Transformaciones Gold ejecutadas correctamente")
        return True
        
    except Exception as e:
        print(f"❌ Error en transformaciones Gold: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Ejecuta todas las pruebas del pipeline"""
    print("🧪 PRUEBAS DEL PIPELINE ELT")
    print("=" * 50)
    
    # Paso 1: Probar conexión
    if not test_database_connection():
        print("❌ Falló la prueba de conexión. Abortando.")
        return
    
    # Paso 2: Verificar datos
    if not test_data_availability():
        print("❌ No hay datos disponibles. Ejecuta primero --download")
        return
    
    # Paso 3: Probar transformaciones SQL
    if not test_sql_transformations():
        print("❌ Fallaron las transformaciones SQL")
        return
    
    # Paso 4: Probar transformaciones Gold
    if not test_gold_transformations():
        print("❌ Fallaron las transformaciones Gold")
        return
    
    print("\n🎉 ¡TODAS LAS PRUEBAS PASARON!")
    print("El pipeline está funcionando correctamente.")

if __name__ == "__main__":
    main()
