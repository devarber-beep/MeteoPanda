"""
Sistema de logging estructurado
"""
import logging
import logging.config
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import json


class MeteoPandaFormatter(logging.Formatter):
    """Formatter personalizado para MeteoPanda con información estructurada"""
    
    def __init__(self):
        super().__init__()
        self.base_format = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
        self.detailed_format = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(funcName)-15s:%(lineno)-4d | %(message)s"
    
    def format(self, record):
        # Usar formato detallado para DEBUG y ERROR
        if record.levelno in [logging.DEBUG, logging.ERROR]:
            formatter = logging.Formatter(self.detailed_format)
        else:
            formatter = logging.Formatter(self.base_format)
        
        return formatter.format(record)


class StructuredLogger:
    """Logger estructurado con contexto y métricas"""
    
    def __init__(self, name: str, level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        self.context = {}
    
    def add_context(self, **kwargs):
        """Añadir contexto al logger"""
        self.context.update(kwargs)
    
    def clear_context(self):
        """Limpiar contexto"""
        self.context.clear()
    
    def _format_message(self, message: str, extra_data: Optional[Dict[str, Any]] = None) -> str:
        """Formatear mensaje con contexto y datos adicionales"""
        if not self.context and not extra_data:
            return message
        
        context_data = {**self.context}
        if extra_data:
            context_data.update(extra_data)
        
        return f"{message} | Context: {json.dumps(context_data, default=str)}"
    
    def debug(self, message: str, extra_data: Optional[Dict[str, Any]] = None):
        """Log de debug con contexto"""
        self.logger.debug(self._format_message(message, extra_data))
    
    def info(self, message: str, extra_data: Optional[Dict[str, Any]] = None):
        """Log de información con contexto"""
        self.logger.info(self._format_message(message, extra_data))
    
    def warning(self, message: str, extra_data: Optional[Dict[str, Any]] = None):
        """Log de advertencia con contexto"""
        self.logger.warning(self._format_message(message, extra_data))
    
    def error(self, message: str, extra_data: Optional[Dict[str, Any]] = None, exc_info: bool = False):
        """Log de error con contexto"""
        self.logger.error(self._format_message(message, extra_data), exc_info=exc_info)
    
    def critical(self, message: str, extra_data: Optional[Dict[str, Any]] = None, exc_info: bool = False):
        """Log crítico con contexto"""
        self.logger.critical(self._format_message(message, extra_data), exc_info=exc_info)
    
    def log(self, level: int, message: str, extra_data: Optional[Dict[str, Any]] = None, exc_info: bool = False):
        """Log con nivel personalizado"""
        self.logger.log(level, self._format_message(message, extra_data), exc_info=exc_info)


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    console_output: bool = True,
    structured: bool = True
) -> None:
    """
    Configurar el sistema de logging
    
    Args:
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Archivo de log (opcional)
        console_output: Mostrar logs en consola
        structured: Usar formato estructurado
    """
    
    # Configuración base
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "meteopanda": {
                "()": MeteoPandaFormatter,
            },
            "simple": {
                "format": "%(levelname)s | %(name)s | %(message)s"
            }
        },
        "handlers": {},
        "loggers": {
            "meteopanda": {
                "level": level.upper(),
                "handlers": [],
                "propagate": False
            }
        },
        "root": {
            "level": level.upper(),
            "handlers": []
        }
    }
    
    # Handler para consola
    if console_output:
        config["handlers"]["console"] = {
            "class": "logging.StreamHandler",
            "level": level.upper(),
            "formatter": "meteopanda" if structured else "simple",
            "stream": sys.stdout
        }
        config["loggers"]["meteopanda"]["handlers"].append("console")
        config["root"]["handlers"].append("console")
    
    # Handler para archivo
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": level.upper(),
            "formatter": "meteopanda" if structured else "simple",
            "filename": str(log_path),
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5
        }
        config["loggers"]["meteopanda"]["handlers"].append("file")
        config["root"]["handlers"].append("file")
    
    # Aplicar configuración
    logging.config.dictConfig(config)


def get_logger(name: str, structured: bool = True) -> logging.Logger:
    """
    Obtener logger configurado
    
    Args:
        name: Nombre del logger
        structured: Usar logger estructurado
    
    Returns:
        Logger configurado
    """
    if structured:
        return StructuredLogger(name)
    else:
        return logging.getLogger(f"meteopanda.{name}")


# Funciones helper para logging común
def log_operation_start(logger: logging.Logger, operation: str, **context):
    """Log de inicio de operación"""
    logger.info(f"[START] Iniciando {operation}", extra_data=context)

def log_operation_success(logger: logging.Logger, operation: str, duration: Optional[float] = None, **context):
    """Log de operación exitosa"""
    message = f"[SUCCESS] {operation} completado exitosamente"
    if duration:
        message += f" en {duration:.2f}s"
    logger.info(message, extra_data=context)

def log_operation_error(logger: logging.Logger, operation: str, error: Exception, **context):
    """Log de error en operación"""
    logger.error(f"[ERROR] Error en {operation}: {str(error)}", extra_data=context, exc_info=True)

def log_data_loaded(logger: logging.Logger, data_type: str, count: int, **context):
    """Log de datos cargados"""
    logger.info(f"[DATA] Datos {data_type} cargados: {count} registros", extra_data=context)

def log_performance_warning(logger: logging.Logger, operation: str, duration: float, threshold: float = 5.0):
    """Log de advertencia de rendimiento"""
    if duration > threshold:
        logger.warning(f"[PERF] {operation} tardó {duration:.2f}s (umbral: {threshold}s)")

def log_cache_operation(logger: logging.Logger, operation: str, cache_key: str, **context):
    """Log de operación de caché"""
    logger.debug(f"[CACHE] Caché {operation}: {cache_key}", extra_data=context)

def log_api_request(logger: logging.Logger, api_name: str, endpoint: str, status_code: int, **context):
    """Log de petición API"""
    level = logging.INFO if 200 <= status_code < 300 else logging.WARNING
    logger.log(level, f"[API] {api_name}: {endpoint} -> {status_code}", extra_data=context)

def log_database_operation(logger: logging.Logger, operation: str, table: str, affected_rows: Optional[int] = None, **context):
    """Log de operación de base de datos"""
    message = f"[DB] {operation}: {table}"
    if affected_rows is not None:
        message += f" ({affected_rows} filas)"
    logger.info(message, extra_data=context)

def log_validation_warning(logger: logging.Logger, field: str, issue: str, **context):
    """Log de advertencia de validación"""
    logger.warning(f"[VALID] Validación {field}: {issue}", extra_data=context)

def log_configuration_loaded(logger: logging.Logger, config_type: str, **context):
    """Log de configuración cargada"""
    logger.info(f"[CONFIG] Configuración {config_type} cargada", extra_data=context)

def log_cleanup_operation(logger: logging.Logger, resource: str, **context):
    """Log de operación de limpieza"""
    logger.info(f"[CLEANUP] Limpieza {resource}", extra_data=context)

def log_and_show_warning(logger: logging.Logger, message: str, **context):
    """Log warning y mostrar en Streamlit - patrón común"""
    logger.warning(f"[WARNING] {message}", extra_data=context)
    import streamlit as st
    st.warning(message)

def log_and_show_error(logger: logging.Logger, message: str, **context):
    """Log error y mostrar en Streamlit - patrón común"""
    logger.error(f"[ERROR] {message}", extra_data=context)
    import streamlit as st
    st.error(message)

def log_and_show_success(logger: logging.Logger, message: str, **context):
    """Log success y mostrar en Streamlit - patrón común"""
    logger.info(f"[SUCCESS] {message}", extra_data=context)
    import streamlit as st
    st.success(message)
 