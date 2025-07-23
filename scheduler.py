import pandas as pd
import schedule
import time
import logging
import sys
import os
from constant import *
from datetime import datetime
from utils.get_sheets import read_sheet
from utils.get_token import get_config_value, get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida, subir_archivo_con_reintento
from utils.transform_data import recepcion_clean_data, tiempos_transform_packing_data
from data.transform.packing_transform import tiempos_packing_data_transform
from utils.helpers import get_download_url_by_name
from tasks.flujo_packing import ejecutar_proceso_principal,ejecutar_proceso_costos,ejecutar_proceso_bm_packing
from tasks.update_tipo_cambio import ejecutar_proceso_update_tipo_cambio


# Configurar logging
def setup_logging():
    """Configura el sistema de logging según config.yaml"""
    log_level = get_config_value('logging', 'level') or 'INFO'
    
    # Configurar handlers con encoding UTF-8
    file_handler = logging.FileHandler('scheduler.log', encoding='utf-8')
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Configurar formato
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Configurar logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logging.getLogger(__name__)


    
 




def ejecutar_proceso_tipo_cambio(access_token):
    """
    Función wrapper para actualización de tipo de cambio
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🔑 Obteniendo token de acceso para tipo de cambio...")
        
        if not access_token:
            logger.error("❌ No se pudo obtener el token de acceso")
            return False
            
        return ejecutar_proceso_update_tipo_cambio(access_token)
        
    except Exception as e:
        logger.error(f"❌ Error en el proceso de tipo de cambio: {str(e)}")
        return False


def configurar_scheduler(access_token):
    """
    Configura el scheduler según los parámetros del config.yaml
    """
    logger = logging.getLogger(__name__)
    
    # Obtener configuración del scheduler
    interval = get_config_value('scheduler', 'interval') or 'minutes'
    time_config = get_config_value('scheduler', 'time') or '09:00'
    minutes = get_config_value('scheduler', 'minutes') or 15
    
    logger.info(f"📅 Configurando scheduler: interval={interval}")
    
    # Limpiar trabajos anteriores
    schedule.clear()

    try:
        schedule.every().day.at("08:00").do(ejecutar_proceso_tipo_cambio(access_token))
        logger.info(f"💱 Programado proceso de tipo de cambio para ejecutarse diariamente a las 08:00 AM")
        schedule.every(5).minutes.do(ejecutar_proceso_principal(access_token))
        schedule.every(17).minutes.do(ejecutar_proceso_costos(access_token))
        schedule.every(23).minutes.do(ejecutar_proceso_bm_packing(access_token))
        
    except Exception as e:
        logger.error(f"Ya fue ejecutado el proceso de tipo de cambio: {str(e)}")
        return False
    
        
    
def mostrar_configuracion():
    """
    Muestra la configuración actual del scheduler
    """
    logger = logging.getLogger(__name__)
    
    interval = get_config_value('scheduler', 'interval') or 'minutes'
    time_config = get_config_value('scheduler', 'time') or '09:00'
    minutes = get_config_value('scheduler', 'minutes') or 15
    
    logger.info("=" * 50)
    logger.info("📋 CONFIGURACIÓN DEL SCHEDULER")
    logger.info("=" * 50)
    logger.info(f"Intervalo: {interval}")
    if interval == 'daily':
        logger.info(f"Hora: {time_config}")
    elif interval == 'minutes':
        logger.info(f"Minutos: {minutes}")
    logger.info("=" * 50)

def is_interactive():
    """
    Detecta si el script está ejecutándose en modo interactivo
    """
    return sys.stdin.isatty() and sys.stdout.isatty()

def main():
    """
    Función principal que inicia el sistema automatizado
    """
    # Configurar logging
    logger = setup_logging()
    
    logger.info("🚀 Iniciando sistema automatizado con Schedule...")
    logger.info("📖 Leyendo configuración desde config.yaml...")
    access_token = get_access_token()
    # Mostrar configuración
    mostrar_configuracion()
    
    # Configurar el scheduler
    configurar_scheduler(access_token)
    
    # Decidir si ejecutar proceso inicial
    ejecutar_inicial = False
    
    if is_interactive():
        # Modo interactivo: preguntar al usuario
        print("\n¿Deseas ejecutar el proceso una vez al inicio? (s/n): ", end="")
        try:
            respuesta = input().lower().strip()
            if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
                ejecutar_inicial = True
        except (EOFError, KeyboardInterrupt):
            logger.info("🤖 Modo no interactivo detectado, continuando sin ejecución inicial")
    else:
        # Modo no interactivo (Docker): leer configuración o usar default
        ejecutar_inicial = get_config_value('scheduler', 'ejecutar_inicial') or False
        if ejecutar_inicial:
            logger.info("🤖 Modo no interactivo: ejecutando proceso inicial según configuración")
        else:
            logger.info("🤖 Modo no interactivo: omitiendo proceso inicial")
    
    if ejecutar_inicial:
        logger.info("🔄 Ejecutando proceso packing...")
        ejecutar_proceso_principal(access_token)
        ejecutar_proceso_costos(access_token)
        ejecutar_proceso_bm_packing(access_token)
        #ejecutar_proceso_proceso_packing()
        
    
    # Mantener el programa corriendo
    logger.info("⚡ Sistema automatizado en funcionamiento. Presiona Ctrl+C para detener.")
    print("\n" + "="*60)
    print("📅 SISTEMA AUTOMATIZADO ACTIVO")
    print("="*60)
    print("⏰ Próxima ejecución programada según config.yaml")
    print("🛑 Presiona Ctrl+C para detener el sistema")
    print("="*60)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Sistema automatizado detenido por el usuario")
        print("\n✅ Sistema detenido correctamente")
    except Exception as e:
        logger.error(f"❌ Error en el sistema automatizado: {str(e)}")

if __name__ == "__main__":
    main() 



"""

def configurar_scheduler(access_token):

    logger = logging.getLogger(__name__)
    
    # Obtener configuración del scheduler
    interval = get_config_value('scheduler', 'interval') or 'minutes'
    time_config = get_config_value('scheduler', 'time') or '09:00'
    minutes = get_config_value('scheduler', 'minutes') or 15
    
    logger.info(f"📅 Configurando scheduler: interval={interval}")
    
    # Limpiar trabajos anteriores
    schedule.clear()
    
    # Programar actualización de tipo de cambio diariamente a las 8:00 AM
    try:
        schedule.every().day.at("08:00").do(ejecutar_proceso_tipo_cambio(access_token))
        logger.info(f"💱 Programado proceso de tipo de cambio para ejecutarse diariamente a las 08:00 AM")
    except Exception as e:
        logger.error(f"Ya fue ejecutado el proceso de tipo de cambio: {str(e)}")
        return False
    
    if interval == 'daily':
        schedule.every().day.at(time_config).do(ejecutar_proceso_principal(access_token))
        #schedule.every().day.at(time_config).do(ejecutar_proceso_proceso_packing)
        logger.info(f"⏰ Programado para ejecutarse diariamente a las {time_config}")
        
    elif interval == 'hourly':
        schedule.every().hour.do(ejecutar_proceso_principal(access_token))
        #schedule.every().hour.do(ejecutar_proceso_proceso_packing)
        logger.info(f"⏰ Programado para ejecutarse cada hora")
        
    elif interval == 'minutes':
        schedule.every(minutes).minutes.do(ejecutar_proceso_principal(access_token))
        #schedule.every(minutes).minutes.do(ejecutar_proceso_proceso_packing)
        logger.info(f"⏰ Programado para ejecutarse cada {minutes} minutos")
        
    else:
        logger.warning(f"⚠️ Interval '{interval}' no reconocido, usando 15 minutos por defecto")
        schedule.every(15).minutes.do(ejecutar_proceso_principal(access_token))
        #schedule.every(15).minutes.do(ejecutar_proceso_proceso_packing)
"""