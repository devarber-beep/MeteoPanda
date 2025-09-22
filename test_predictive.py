#!/usr/bin/env python3
"""
Script de prueba para el sistema predictivo
Verifica que todas las funcionalidades estén funcionando correctamente
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Aplicar parche de NumPy antes de importar Prophet
from src.dashboard.predictive.numpy_patch import apply_numpy_patch
apply_numpy_patch()

def test_imports():
    """Probar importaciones del sistema predictivo"""
    print("🔍 Probando importaciones...")
    
    try:
        from src.dashboard.predictive import (
            WeatherPredictiveManager, 
            PredictionType, 
            PredictionHorizon,
            PredictionRequest
        )
        print("✅ Importaciones del sistema predictivo: OK")
        return True
    except Exception as e:
        print(f"❌ Error en importaciones: {e}")
        return False

def test_data_manager():
    """Probar DataManager con logger"""
    print("🔍 Probando DataManager...")
    
    try:
        # Simular streamlit para la prueba
        import sys
        from unittest.mock import MagicMock
        sys.modules['streamlit'] = MagicMock()
        
        from src.dashboard.data_manager import DataManager
        dm = DataManager()
        
        if hasattr(dm, 'logger'):
            print("✅ DataManager con logger: OK")
            return True
        else:
            print("❌ DataManager sin logger")
            return False
    except Exception as e:
        print(f"❌ Error en DataManager: {e}")
        return False

def test_predictive_manager():
    """Probar WeatherPredictiveManager"""
    print("🔍 Probando WeatherPredictiveManager...")
    
    try:
        # Simular streamlit para la prueba
        import sys
        from unittest.mock import MagicMock
        sys.modules['streamlit'] = MagicMock()
        
        from src.dashboard.predictive import WeatherPredictiveManager
        from src.dashboard.data_manager import DataManager
        
        # Crear instancias
        data_manager = DataManager()
        predictive_manager = WeatherPredictiveManager(data_manager)
        
        print("✅ WeatherPredictiveManager: OK")
        return True
    except Exception as e:
        print(f"❌ Error en WeatherPredictiveManager: {e}")
        return False

def test_prediction_request():
    """Probar creación de PredictionRequest"""
    print("🔍 Probando PredictionRequest...")
    
    try:
        from src.dashboard.predictive import PredictionRequest, PredictionType, PredictionHorizon
        
        request = PredictionRequest(
            prediction_type=PredictionType.TEMPERATURE,
            city="Sevilla",
            horizon=PredictionHorizon.MEDIUM_TERM
        )
        
        print(f"✅ PredictionRequest creado: {request.prediction_type.value}")
        return True
    except Exception as e:
        print(f"❌ Error en PredictionRequest: {e}")
        return False

def main():
    """Función principal de pruebas"""
    print("🚀 Iniciando pruebas del sistema predictivo...\n")
    
    tests = [
        test_imports,
        test_data_manager,
        test_predictive_manager,
        test_prediction_request
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"📊 Resultados: {passed}/{total} pruebas pasaron")
    
    if passed == total:
        print("🎉 ¡Todas las pruebas pasaron! El sistema predictivo está listo.")
        return True
    else:
        print("⚠️ Algunas pruebas fallaron. Revisar errores.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
