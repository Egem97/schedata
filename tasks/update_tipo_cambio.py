import pandas as pd
import logging
from datetime import datetime
from utils.get_token import get_access_token
from data.load.costos_load import tipo_cambio_load_data
from utils.suppress_warnings import setup_pandas_warnings
setup_pandas_warnings()

def ejecutar_proceso_update_tipo_cambio(access_token):
    logger = logging.getLogger(__name__)
    logger.info("🚀 Iniciando proceso de actualización de tipo de cambio...")
    
    try:
        logger.info("🚀 Iniciando proceso automatizado TC...")
        
        
        tipo_cambio_load_data(access_token)
    except Exception as e:
        logger.error(f"❌ Error en el proceso principal: {str(e)}")
        return False
        
        