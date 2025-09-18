#!/usr/bin/env python3
"""
Script de prueba para el sistema de logging de MeteoPanda
Demuestra todas las funcionalidades implementadas
"""

import sys
import os
import time
from pathlib import Path

# Añadir el directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.logging_config import (
    setup_logging, 
    get_logger, 
    StructuredLogger,
    log_operation_start,
    log_operation_success,
    log_operation_error,
    log_data_loaded,
    log_api_request,
    log_database_operation,
    log_performance_warning,
    log_validation_warning,
    log_cache_operation,
    log_configuration_loaded,
    log_cleanup_operation
)

def test_basic_logging():
    """Prueba logging básico"""
    print("\n=== PRUEBA: Logging Básico ===")
    
    logger = get_logger("test_basic")
    
    logger.debug("Mensaje de debug (no visible en INFO)")
    logger.info("Mensaje informativo")
    logger.warning("Mensaje de advertencia")
    logger.error("Mensaje de error")
    logger.critical("Mensaje crítico")

def test_structured_logging():
    """Prueba logging estructurado con contexto"""
    print("\n=== PRUEBA: Logging Estructurado ===")
    
    structured_logger = StructuredLogger("test_structured")
    
    # Añadir contexto persistente
    structured_logger.add_context(
        user_id="test_user_123",
        operation="data_extraction",
        environment="testing"
    )
    
    # Los logs incluirán el contexto automáticamente
    structured_logger.info("Iniciando extracción de datos")
    structured_logger.info("Procesando ciudad Madrid")
    
    # Añadir contexto temporal
    structured_logger.info("Datos cargados", extra_data={
        "records_count": 1500,
        "processing_time": 2.5,
        "source": "AEMET"
    })
    
    # Limpiar contexto
    structured_logger.clear_context()
    structured_logger.info("Contexto limpiado")

def test_helper_functions():
    """Prueba funciones helper especializadas"""
    print("\n=== PRUEBA: Funciones Helper ===")
    
    logger = get_logger("test_helpers")
    
    # Simular operación completa
    start_time = time.time()
    log_operation_start(logger, "extracción de datos meteorológicos", 
                       config_file="test_config.yaml")
    
    # Simular procesamiento
    time.sleep(0.1)
    
    # Simular éxito
    duration = time.time() - start_time
    log_operation_success(logger, "extracción de datos meteorológicos", 
                         duration=duration,
                         records_processed=1500,
                         cities_processed=5)
    
    # Simular carga de datos
    log_data_loaded(logger, "weather_data", 1500, 
                   source="AEMET", city="Madrid")
    
    # Simular petición API
    log_api_request(logger, "AEMET", "https://api.aemet.es/data", 200,
                   description="obtener datos meteorológicos")
    
    # Simular operación de base de datos
    log_database_operation(logger, "consulta", "weather_data", 
                          affected_rows=1500, query_type="SELECT")
    
    # Simular advertencia de rendimiento
    log_performance_warning(logger, "carga de datos", 6.5, threshold=5.0)
    
    # Simular advertencia de validación
    log_validation_warning(logger, "coordenadas", 
                          "Solo 1200/1500 registros tienen coordenadas",
                          total_records=1500, records_with_coords=1200)
    
    # Simular operación de caché
    log_cache_operation(logger, "hit", "weather_data_madrid_2024")
    
    # Simular configuración cargada
    log_configuration_loaded(logger, "ciudades", cities_count=5)
    
    # Simular limpieza
    log_cleanup_operation(logger, "archivos temporales")

def test_error_handling():
    """Prueba manejo de errores"""
    print("\n=== PRUEBA: Manejo de Errores ===")
    
    logger = get_logger("test_errors")
    
    try:
        # Simular error
        raise ValueError("Error simulado para testing")
    except Exception as e:
        log_operation_error(logger, "procesamiento de datos", e,
                           config_file="test_config.yaml",
                           attempt=1,
                           data_source="AEMET")

def test_different_levels():
    """Prueba diferentes niveles de logging"""
    print("\n=== PRUEBA: Diferentes Niveles ===")
    
    # Configurar con nivel DEBUG para ver todos los mensajes
    setup_logging(level="DEBUG", log_file="logs/test_detailed.log", 
                 console_output=True, structured=True)
    
    logger = get_logger("test_levels")
    
    logger.debug("Mensaje de debug - información detallada")
    logger.info("Mensaje informativo - operación normal")
    logger.warning("Mensaje de advertencia - situación inusual")
    logger.error("Mensaje de error - problema que necesita atención")
    logger.critical("Mensaje crítico - problema grave")

def test_file_rotation():
    """Prueba rotación de archivos de log"""
    print("\n=== PRUEBA: Rotación de Archivos ===")
    
    # Configurar con archivo pequeño para probar rotación
    setup_logging(level="INFO", log_file="logs/test_rotation.log",
                 console_output=False, structured=True)
    
    logger = get_logger("test_rotation")
    
    # Generar muchos logs para probar rotación
    for i in range(100):
        logger.info(f"Mensaje de prueba {i:03d} - " + "x" * 100)

def main():
    """Función principal de prueba"""
    print("🧪 INICIANDO PRUEBAS DEL SISTEMA DE LOGGING METEOPANDA")
    print("=" * 60)
    
    # Configurar logging para las pruebas
    setup_logging(level="INFO", log_file="logs/test_system.log", 
                 console_output=True, structured=True)
    
    try:
        # Ejecutar todas las pruebas
        test_basic_logging()
        test_structured_logging()
        test_helper_functions()
        test_error_handling()
        test_different_levels()
        test_file_rotation()
        
        print("\n" + "=" * 60)
        print("✅ TODAS LAS PRUEBAS COMPLETADAS EXITOSAMENTE")
        print("\nArchivos de log generados:")
        print("- logs/test_system.log")
        print("- logs/test_detailed.log")
        print("- logs/test_rotation.log")
        print("\nRevisa los archivos para ver el logging estructurado en acción.")
        
    except Exception as e:
        print(f"\n❌ ERROR EN LAS PRUEBAS: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
