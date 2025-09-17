import duckdb
from pathlib import Path
from .logging_config import get_logger, log_database_operation, log_operation_error

# Configurar logger
logger = get_logger("database")

DB_PATH = Path("meteopanda.duckdb")

def get_connection():
    """Obtener conexión a la base de datos con logging"""
    try:
        connection = duckdb.connect(database=DB_PATH, read_only=False)
        log_database_operation(logger, "conectar", str(DB_PATH), db_path=str(DB_PATH))
        return connection
    except Exception as e:
        log_operation_error(logger, "conexión a base de datos", e, db_path=str(DB_PATH))
        raise
    