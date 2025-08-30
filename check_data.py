import duckdb
import pandas as pd
from itertools import product
import plotly.express as px
from src.utils.db import get_connection

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

def verificar_anomalias_granada():
    """
    Verifica anomalías en los datos de temperatura de Granada desde 2022 hasta 2024
    tanto en la capa silver como en gold.
    """
    con = get_connection()
    
    try:
        # Consulta para datos silver
        query_silver = """
        SELECT date, temp_max_c, temp_min_c, temp_avg_c
        FROM silver.weather_cleaned
        WHERE city = 'granada'
        AND date >= '2022-01-01'
        AND date <= '2024-12-31'
        ORDER BY date
        """
        
        # Consulta para datos gold (resumen mensual)
        query_gold = """
        SELECT CAST(year AS INTEGER) as year, month, max_temp, min_temp, avg_temp
        FROM gold.city_yearly_summary
        WHERE city = 'granada'
        AND CAST(year AS INTEGER) >= 2022
        AND CAST(year AS INTEGER) <= 2024
        ORDER BY year, month
        """
        
        # Obtener datos
        df_silver = con.execute(query_silver).df()
        df_gold = con.execute(query_gold).df()
        
        # Análisis de datos silver
        print("\n=== Análisis de datos diarios (Silver) ===")
        print("\nEstadísticas descriptivas:")
        print(df_silver.describe())
        
        # Detectar valores atípicos usando el método IQR
        def detectar_outliers(series):
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            return series[(series < lower_bound) | (series > upper_bound)]
        
        # Detectar outliers en cada tipo de temperatura
        outliers_max = detectar_outliers(df_silver['temp_max_c'])
        outliers_min = detectar_outliers(df_silver['temp_min_c'])
        outliers_avg = detectar_outliers(df_silver['temp_avg_c'])
        
        print("\nDías con temperaturas máximas anómalas:")
        print(df_silver[df_silver['temp_max_c'].isin(outliers_max)][['date', 'temp_max_c']])
        
        print("\nDías con temperaturas mínimas anómalas:")
        print(df_silver[df_silver['temp_min_c'].isin(outliers_min)][['date', 'temp_min_c']])
        
        print("\nDías con temperaturas promedio anómalas:")
        print(df_silver[df_silver['temp_avg_c'].isin(outliers_avg)][['date', 'temp_avg_c']])
        
        # Análisis de datos gold
        print("\n=== Análisis de datos mensuales (Gold) ===")
        print("\nEstadísticas descriptivas:")
        print(df_gold.describe())
        
        # Detectar outliers en datos mensuales
        outliers_max_gold = detectar_outliers(df_gold['max_temp'])
        outliers_min_gold = detectar_outliers(df_gold['min_temp'])
        outliers_avg_gold = detectar_outliers(df_gold['avg_temp'])
        
        print("\nMeses con temperaturas máximas anómalas:")
        print(df_gold[df_gold['max_temp'].isin(outliers_max_gold)][['year', 'month', 'max_temp']])
        
        print("\nMeses con temperaturas mínimas anómalas:")
        print(df_gold[df_gold['min_temp'].isin(outliers_min_gold)][['year', 'month', 'min_temp']])
        
        print("\nMeses con temperaturas promedio anómalas:")
        print(df_gold[df_gold['avg_temp'].isin(outliers_avg_gold)][['year', 'month', 'avg_temp']])
        
        # Crear visualizaciones
        # Gráfico de temperaturas diarias
        fig_daily = px.line(df_silver, x='date', y=['temp_max_c', 'temp_min_c', 'temp_avg_c'],
                           title='Temperaturas diarias en Granada (2022-2024)',
                           labels={'value': 'Temperatura (°C)', 'date': 'Fecha'},
                           color_discrete_map={'temp_max_c': 'red', 'temp_min_c': 'blue', 'temp_avg_c': 'green'})
        fig_daily.show()
        
        # Gráfico de temperaturas mensuales
        df_gold['fecha'] = pd.to_datetime(df_gold[['year', 'month']].assign(day=1))
        fig_monthly = px.line(df_gold, x='fecha', y=['max_temp', 'min_temp', 'avg_temp'],
                             title='Temperaturas mensuales en Granada (2022-2024)',
                             labels={'value': 'Temperatura (°C)', 'fecha': 'Fecha'},
                             color_discrete_map={'max_temp': 'red', 'min_temp': 'blue', 'avg_temp': 'green'})
        fig_monthly.show()
    
    finally:
        con.close()

if __name__ == "__main__":
    verificar_anomalias_granada()
