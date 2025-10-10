import duckdb

try:
    conn = duckdb.connect('data/meteopanda.duckdb')
    
    print("=== TODOS LOS ESQUEMAS ===")
    schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    for schema in schemas:
        print(f"- {schema[0]}")
    
    print("\n=== TODAS LAS TABLAS ===")
    all_tables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()
    for schema, table in all_tables:
        print(f"- {schema}.{table}")
    
    print("\n=== VERIFICACIÓN DE DATOS ===")
    # Verificar si hay datos en el esquema principal
    try:
        # Verificar si hay datos en weather_raw
        weather_raw_tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'weather_raw'").fetchall()
        print("Tablas en weather_raw:", [t[0] for t in weather_raw_tables])
        
        # Verificar si hay datos en silver
        silver_tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'").fetchall()
        print("Tablas en silver:", [t[0] for t in silver_tables])
        
        # Verificar si hay datos en gold
        gold_tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold'").fetchall()
        print("Tablas en gold:", [t[0] for t in gold_tables])
        
        # Verificar si hay datos en el esquema principal
        main_tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        print("Tablas en main:", [t[0] for t in main_tables])
        
        # Verificar si hay datos en el esquema principal
        if main_tables:
            print("\n=== MUESTRA DE DATOS EN MAIN ===")
            for table in main_tables:
                table_name = table[0]
                try:
                    sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                    columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
                    print(f"Tabla: {table_name}")
                    print("Columnas:", [col[0] for col in columns])
                    print("Muestra:")
                    for row in sample:
                        print(row)
                    print()
                except Exception as e:
                    print(f"Error accediendo a {table_name}: {e}")
        
        # Verificar si hay datos en el esquema principal
        if not main_tables and not weather_raw_tables and not silver_tables and not gold_tables:
            print("\n=== VERIFICACIÓN DE DATOS EN ESQUEMA PRINCIPAL ===")
            # Verificar si hay datos en el esquema principal
            try:
                # Verificar si hay datos en el esquema principal
                main_tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
                print("Tablas en main:", [t[0] for t in main_tables])
                
                # Verificar si hay datos en el esquema principal
                if main_tables:
                    print("\n=== MUESTRA DE DATOS EN MAIN ===")
                    for table in main_tables:
                        table_name = table[0]
                        try:
                            sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                            columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
                            print(f"Tabla: {table_name}")
                            print("Columnas:", [col[0] for col in columns])
                            print("Muestra:")
                            for row in sample:
                                print(row)
                            print()
                        except Exception as e:
                            print(f"Error accediendo a {table_name}: {e}")
                
            except Exception as e:
                print(f"Error verificando datos en main: {e}")
        
        # Verificar si hay datos en el esquema principal
        if not main_tables and not weather_raw_tables and not silver_tables and not gold_tables:
            print("\n=== VERIFICACIÓN DE DATOS EN ESQUEMA PRINCIPAL ===")
            # Verificar si hay datos en el esquema principal
            try:
                # Verificar si hay datos en el esquema principal
                main_tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
                print("Tablas en main:", [t[0] for t in main_tables])
                
                # Verificar si hay datos en el esquema principal
                if main_tables:
                    print("\n=== MUESTRA DE DATOS EN MAIN ===")
                    for table in main_tables:
                        table_name = table[0]
                        try:
                            sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                            columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
                            print(f"Tabla: {table_name}")
                            print("Columnas:", [col[0] for col in columns])
                            print("Muestra:")
                            for row in sample:
                                print(row)
                            print()
                        except Exception as e:
                            print(f"Error accediendo a {table_name}: {e}")
                
            except Exception as e:
                print(f"Error verificando datos en main: {e}")
        
    except Exception as e:
        print(f"Error verificando datos: {e}")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
