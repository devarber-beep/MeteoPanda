from src.extract.meteo_api import load_config, get_station_id, fetch_daily_data, save_to_parquet

def main():
    cities, start_date, end_date = load_config("config/config.yaml")

    for city in cities:
        print(f"📍 Procesando ciudad: {city.name}")
        station_id = get_station_id(city.latitude, city.longitude)
        if not station_id:
            print(f"No se encontró estación para {city.name}")
            continue
        df = fetch_daily_data(station_id, start_date, end_date)
        save_to_parquet(df, city.name)

if __name__ == "__main__":
    main()
