"""
Utilidades para manejo de zonas horarias con Lima, Perú
"""
import pytz
from datetime import datetime, timezone
import os

# Configurar zona horaria de Lima, Perú
LIMA_TIMEZONE = pytz.timezone('America/Lima')

def get_lima_timezone():
    """Obtener la zona horaria de Lima, Perú"""
    return LIMA_TIMEZONE

def get_current_lima_time():
    """Obtener la hora actual en Lima, Perú"""
    return datetime.now(LIMA_TIMEZONE)

def convert_to_lima_timezone(dt, source_timezone=None):
    """
    Convertir una fecha/hora a la zona horaria de Lima, Perú
    
    Args:
        dt: datetime object (puede ser naive o con timezone)
        source_timezone: zona horaria de origen (si dt es naive)
    
    Returns:
        datetime: fecha/hora en zona horaria de Lima
    """
    if dt is None:
        return None
    
    # Si dt no tiene timezone y se especifica source_timezone
    if dt.tzinfo is None and source_timezone:
        if isinstance(source_timezone, str):
            source_timezone = pytz.timezone(source_timezone)
        dt = source_timezone.localize(dt)
    
    # Si dt no tiene timezone y no se especifica source_timezone, asumir UTC
    elif dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    
    # Convertir a Lima
    return dt.astimezone(LIMA_TIMEZONE)

def format_lima_datetime(dt, format_str="%Y-%m-%d %H:%M:%S %Z"):
    """
    Formatear una fecha/hora en zona horaria de Lima
    
    Args:
        dt: datetime object
        format_str: formato de salida
    
    Returns:
        str: fecha/hora formateada
    """
    if dt is None:
        return None
    
    lima_dt = convert_to_lima_timezone(dt)
    return lima_dt.strftime(format_str)

def get_lima_date_string():
    """Obtener la fecha actual en Lima como string YYYY-MM-DD"""
    return get_current_lima_time().strftime('%Y-%m-%d')

def get_lima_datetime_string():
    """Obtener fecha y hora actual en Lima como string YYYY-MM-DD HH:MM:SS"""
    return get_current_lima_time().strftime('%Y-%m-%d %H:%M:%S')

def setup_environment_timezone():
    """Configurar la zona horaria del entorno para Lima, Perú"""
    # Configurar variables de entorno
    os.environ['TZ'] = 'America/Lima'
    
    # En Windows, también configurar la zona horaria del sistema
    try:
        import time
        time.tzset()
    except AttributeError:
        # Windows no tiene tzset(), pero TZ environment variable funciona
        pass

def create_lima_timestamp():
    """
    Crear un timestamp en zona horaria de Lima para usar en SQL
    
    Returns:
        str: timestamp formateado para PostgreSQL
    """
    return get_current_lima_time().strftime('%Y-%m-%d %H:%M:%S')

def parse_google_drive_timestamp(timestamp_str):
    """
    Parsear timestamp de Google Drive y convertirlo a Lima timezone
    
    Args:
        timestamp_str: timestamp de Google Drive (formato ISO 8601)
    
    Returns:
        datetime: datetime en zona horaria de Lima
    """
    if not timestamp_str:
        return None
    
    try:
        # Google Drive usa UTC, parsear y convertir
        utc_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return convert_to_lima_timezone(utc_dt)
    except Exception as e:
        print(f"Error parseando timestamp {timestamp_str}: {e}")
        return None

# Configurar zona horaria al importar el módulo
setup_environment_timezone()
