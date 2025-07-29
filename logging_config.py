from sqlalchemy import  text
import datetime
import logging
from config import cfg
from typing import Optional

# Configuração do postgresql
LogSession = cfg.get_postgres_session()

# --- Standard Logging Setup ---
LOG_LEVEL = logging.INFO # Default level, can be changed
LOG_LEVEL = logging.DEBUG # Uncomment for more detailed logs

# Basic configuration (can be enhanced with handlers, formatters)
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler() # Log to console
        # You could add FileHandler here if needed:
        # logging.FileHandler('app.log')
    ]
)

def log_to_db(level: str, logger_name: str, message: str, job_id: Optional[int] = None, user_name: Optional[str] = None, duration_ms: Optional[int] = None):
    """Writes a log entry to the SQLite database."""
    if not LogSession:
        print(f"DB Logging Error (no session): {level} - {logger_name} - {message}")
        return

    session = LogSession()
    try:
        # Use text() for raw SQL and parameter binding
        stmt = text("""
            INSERT INTO sql_scheduler.logs (timestamp, log_level, logger_name, job_id, user_name, log_text, duration_ms)
            VALUES (:ts, :lvl, :ln, :jid, :un, :txt, :dur)
        """)
        session.execute(stmt, {
            "ts": datetime.datetime.now(),
            "lvl": level,
            "ln": logger_name,
            "jid": job_id,
            "un": user_name,
            "txt": message,
            "dur": duration_ms
        })
        session.commit()
    except Exception as e:
        session.rollback()
        # Avoid infinite loops if logging the error itself fails
        print(f"CRITICAL: Failed to write log to database! Error: {e}")
        print(f"Original log message: {level} - {logger_name} - {message}")
    finally:
        session.close()

def get_logger(name: str):
    """Gets a logger instance."""
    return logging.getLogger(name)

# --- Convenience Logging Functions ---

def log_info(logger, message: str, job_id: Optional[int] = None, user: Optional[str] = None, duration_ms: Optional[int] = None):
    logger.info(message)
    log_to_db("INFO", logger.name, message, job_id, user, duration_ms)

def log_warning(logger, message: str, job_id: Optional[int] = None, user: Optional[str] = None, duration_ms: Optional[int] = None):
    logger.warning(message)
    log_to_db("WARNING", logger.name, message, job_id, user, duration_ms)

def log_error(logger, message, job_id=None, user=None, duration_ms=None, exc_info=False):
    # exc_info=True will add traceback info to console log
    logger.error(message, exc_info=exc_info)
    # For DB log, keep it concise unless you specifically want tracebacks there
    log_to_db("ERROR", logger.name, message, job_id, user, duration_ms)

def log_exception(logger, message: str, job_id: Optional[int] = None, user: Optional[str] = None, duration_ms: Optional[int] = None):
    # Logs message at ERROR level and includes exception info
    logger.exception(message)
    # Include exception details in the DB log as well
    import traceback
    exc_str = traceback.format_exc()
    full_message = f"{message}\nTraceback:\n{exc_str}"
    # Truncate if necessary for DB column size
    max_len = 4000 # Example max length
    if len(full_message) > max_len:
        full_message = full_message[:max_len-3] + "..."

    log_to_db("ERROR", logger.name, full_message, job_id, user, duration_ms)

def log_debug(logger, message: str, job_id: Optional[int] = None, user: Optional[str] = None, duration_ms: Optional[int] = None):
    logger.debug(message)
    # Optionally log DEBUG to DB, or only to console/file
    #log_to_db("DEBUG", logger.name, message, job_id, user, duration_ms)

print(f"Logging configured.")
