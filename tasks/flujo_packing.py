import pandas as pd
import logging
from datetime import datetime
from utils.get_token import get_access_token
from utils.get_sheets import read_sheet
from utils.transform_data import recepcion_clean_data, tiempos_transform_packing_data
from constant import *
from utils.get_api import listar_archivos_en_carpeta_compartida, subir_archivo_con_reintento

from data.load.packing_load import *

from data.load.costos_load import *

def ejecutar_proceso_principal(access_token):
    """
    Función TIEMPOS PACKING
    """
    logger = logging.getLogger(__name__)
    
    try:
        inicio = datetime.now()
        logger.info("🚀 Iniciando proceso automatizado...")
        
        
        tiempos_proceso_packing_load_data(access_token,inicio)
        
            
    except Exception as e:
        logger.error(f"❌ Error en el proceso principal: {str(e)}")
        return False


def ejecutar_proceso_costos(access_token):
    """
    Función TIEMPOS PACKING
    """
    logger = logging.getLogger(__name__)
    
    try:
        inicio = datetime.now()
        logger.info("🚀 Iniciando proceso automatizado...")
        reporte_produccion_load_data(access_token,inicio)#KG PT
        ocupacion_transporte_packing_load_data(access_token,inicio)#OCUPACION TRANSPORTE
        kg_presupuesto_packing_load_data(access_token,inicio)#KG PPTO   
        mayor_analitico_packing_load_data(access_token,inicio)#MAYOR ANALITICO PACKING 
        presupuesto_packing_load_data(access_token,inicio)#PRESUPUESTO
        mayor_analitico_opex_load_data(access_token,inicio)#MAYOR ANALITICO OPEX
            
    except Exception as e:
        logger.error(f"❌ Error en el proceso costos: {str(e)}")
        return False

def ejecutar_proceso_bm_packing(access_token):
    """
    Función TIEMPOS PACKING
    """
    logger = logging.getLogger(__name__)
    
    try:
        inicio = datetime.now()
        logger.info("🚀 Iniciando proceso automatizado...")
        balance_masa_load_data(access_token,inicio)
        
    except Exception as e:
        logger.error(f"❌ Error en el proceso principal: {str(e)}")
        return False