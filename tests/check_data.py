import duckdb
import pandas as pd
from itertools import product

def get_connection():
    # Asegúrate de que esta ruta sea correcta para tu base de datos DuckDB
    return duckdb.connect(database='data/meteopanda.duckdb', read_only=True)

con = get_connection()

try:
    df_extreme = con.execute("SELECT * FROM gold.city_extreme_days").df()
    print("--- Verificando df_extreme ---")

    # 1. Contar filas y columnas
    print(f"Filas: {df_extreme.shape[0]}, Columnas: {df_extreme.shape[1]}")

    # 2. Verificar valores nulos en columnas clave
    print("\nValores nulos por columna (en las columnas relevantes):")
    null_columns = ['max_temp_month', 'min_temp_month', 'total_precip_month']
    
    # Asegurarse de que las columnas existen antes de intentar acceder a ellas
    existing_null_columns = [col for col in null_columns if col in df_extreme.columns]
    
    if not existing_null_columns:
        print("Las columnas clave (max_temp_month, min_temp_month, total_precip_month) no se encontraron en el DataFrame.")
        print("Por favor, asegúrate de que gold.city_extreme_days ha sido actualizado en tu datamart.")
    else:
        null_counts = df_extreme[existing_null_columns].isnull().sum()
        null_counts_filtered = null_counts[null_counts > 0]
        
        if not null_counts_filtered.empty:
            print(null_counts_filtered)
        else:
            print("No hay valores nulos en columnas clave.")

    # 3. Verificar combinaciones (city, year, month) faltantes
    existing_combinations = df_extreme[['city', 'year', 'month']].drop_duplicates()

    all_cities = df_extreme['city'].unique()
    all_years = df_extreme['year'].unique()
    all_months = df_extreme['month'].unique()

    expected_combinations = pd.DataFrame(list(product(all_cities, all_years, all_months)), columns=['city', 'year', 'month'])

    # Encontrar combinaciones faltantes usando merge con indicator
    merged = pd.merge(expected_combinations, existing_combinations, on=['city', 'year', 'month'], how='left', indicator=True)
    missing_combinations = merged[merged['_merge'] == 'left_only'][['city', 'year', 'month']]

    print("\nCombinaciones (ciudad, año, mes) faltantes:")
    if not missing_combinations.empty:
        print(f"Se encontraron {len(missing_combinations)} combinaciones faltantes.")
        
        # Obtener los detalles de los valores nulos para estas combinaciones
        df_all_data = pd.merge(expected_combinations, df_extreme, on=['city', 'year', 'month'], how='left')
        
        # Filtrar para mostrar solo las combinaciones que faltan
        missing_data_details = df_all_data[
            df_all_data['city'].isin(missing_combinations['city']) & 
            df_all_data['year'].isin(missing_combinations['year']) & 
            df_all_data['month'].isin(missing_combinations['month'])
        ]

        # Verificar valores nulos en las columnas clave
        if existing_null_columns:
            missing_data_details = missing_data_details[missing_data_details[existing_null_columns].isnull().any(axis=1)]
            
            if not missing_data_details.empty:
                print("\nDetalle de valores nulos para las combinaciones faltantes (primeras 20 filas):")
                print(missing_data_details[['city', 'year', 'month'] + existing_null_columns].head(20))
            else:
                print("\nLas combinaciones faltantes están completamente ausentes (no son NaNs en un registro existente).")
        
        print("\nSi hay muchas combinaciones faltantes, esto podría ser la causa del problema de visualización.")
    else:
        print("No se encontraron combinaciones faltantes de (ciudad, año, mes).")

    # 4. Verificar específicamente el caso de Sevilla en enero de 2021
    print("\n--- Verificando caso específico de Sevilla en enero de 2021 ---")
    
    # Verificar en gold.city_extreme_days
    print("\n1. Datos en gold.city_extreme_days:")
    sevilla_jan_2021 = con.execute("""
        SELECT *
        FROM gold.city_extreme_days
        WHERE year = 2021
        AND month = 1
        AND city = 'sevilla'
        ORDER BY year, month
    """).df()
    
    if not sevilla_jan_2021.empty:
        print("\nDatos encontrados para Sevilla en enero de 2021:")
        print(sevilla_jan_2021)
    else:
        print("No se encontraron datos para Sevilla en enero de 2021 en gold.city_extreme_days")

    # Verificar en gold.extreme_days
    print("\n2. Datos en gold.city_extreme_days:")
    extreme_days_jan_2021 = con.execute("""
        SELECT *
        FROM gold.city_extreme_days
        WHERE year = 2021
        AND month = 1
        ORDER BY year, month
    """).df()
    
    if not extreme_days_jan_2021.empty:
        print("\nDatos encontrados en gold.city_extreme_days para enero de 2021:")
        print(extreme_days_jan_2021)
    else:
        print("No se encontraron datos para enero de 2021 en gold.city_extreme_days")
        
    # Verificar los datos originales en silver.weather_cleaned
    print("\n3. Verificando datos originales en silver.weather_cleaned:")
    original_data = con.execute("""
        SELECT 
            city,
            EXTRACT(YEAR FROM date) as year,
            EXTRACT(MONTH FROM date) as month,
            temp_min_c,
            temp_max_c,
            precip_mm
        FROM silver.weather_cleaned
        WHERE city = 'sevilla'
        AND EXTRACT(YEAR FROM date) = 2021
        AND EXTRACT(MONTH FROM date) = 1
        ORDER BY date
    """).df()
    print("\nDatos originales:")
    print(original_data)


finally:
    con.close()
