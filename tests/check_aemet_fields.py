import requests
import json
from datetime import datetime, timedelta

# API key de AEMET
AEMET_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkYW5pZWwuYmFyYmVyb2pAZ21haWwuY29tIiwianRpIjoiNzZjM2ZkODMtYjVlZi00YTQ3LWI4M2QtMTc5MjJjMzZlMjMxIiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE3NTYwMzE4MTcsInVzZXJJZCI6Ijc2YzNmZDgzLWI1ZWYtNGE0Ny1iODNkLTE3OTIyYzM2ZTIzMSIsInJvbGUiOiIifQ.rVEOBWGmTc3utFTnYY27SB_tVfJmIPXGIIn1Rp80MKI"

AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"
HEADERS = {"api_key": AEMET_API_KEY}

def check_aemet_fields():
    """Verifica qué campos están disponibles en la API de AEMET"""
    
    # Usar una estación conocida (Sevilla - 5790Y)
    station_id = "5790Y"
    start_date = "2024-01-01T00:00:00UTC"
    end_date = "2024-01-31T00:00:00UTC"
    
    # Primera petición para obtener la URL de datos
    url = f"{AEMET_BASE_URL}/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{station_id}"
    
    print(f"Verificando campos de AEMET para estación {station_id}")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        metadata = response.json()
        print(f"Respuesta inicial: {metadata}")
        
        if metadata.get('estado') == 200:
            data_url = metadata.get('datos')
            if data_url:
                print(f"URL de datos: {data_url}")
                
                # Segunda petición para obtener los datos reales
                data_response = requests.get(data_url)
                data_response.raise_for_status()
                
                raw_data = data_response.json()
                
                if raw_data and len(raw_data) > 0:
                    print(f"\nCampos disponibles en el primer registro:")
                    first_record = raw_data[0]
                    for key, value in first_record.items():
                        print(f"  - {key}: {value} (tipo: {type(value).__name__})")
                    
                    print(f"\nTotal de registros: {len(raw_data)}")
                    
                    # Buscar campos relacionados con humedad
                    humidity_fields = [key for key in first_record.keys() if 'hum' in key.lower() or 'hr' in key.lower()]
                    if humidity_fields:
                        print(f"\nCampos de humedad encontrados: {humidity_fields}")
                    else:
                        print(f"\nNo se encontraron campos específicos de humedad")
                        
                else:
                    print("No se obtuvieron datos")
            else:
                print("No se encontró URL de datos")
        else:
            print(f"Error en la respuesta: {metadata}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_aemet_fields()
