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
from tasks.flujo_packing import ejecutar_proceso_principal,ejecutar_proceso_costos
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


    
 




def ejecutar_proceso_tipo_cambio():
    """
    Función wrapper para actualización de tipo de cambio
    """
    logger = logging.getLogger(__name__)
   
    try:
        logger.info("🔑 Obteniendo token de acceso para tipo de cambio...")
        
            
        return ejecutar_proceso_update_tipo_cambio()
        
    except Exception as e:
        logger.error(f"❌ Error en el proceso de tipo de cambio: {str(e)}")
        return False


def configurar_scheduler():
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
        # Programar funciones SIN ejecutarlas inmediatamente
        schedule.every().day.at("08:00").do(ejecutar_proceso_tipo_cambio)
        logger.info(f"💱 Programado proceso de tipo de cambio para ejecutarse diariamente a las 08:00 AM")
        
        schedule.every(5).minutes.do(ejecutar_proceso_principal)
        logger.info(f"⏰ Programado proceso principal cada 5 minutos")
        
        schedule.every(17).minutes.do(ejecutar_proceso_costos )
        logger.info(f"⏰ Programado proceso costos cada 17 minutos")
        
        #schedule.every(23).minutes.do(ejecutar_proceso_bm_packing)
        #logger.info(f"⏰ Programado proceso BM packing cada 23 minutos")
        #schedule.every(15).minutes.do(ejecutar_proceso_images_fcl)  
    except Exception as e:
        logger.error(f"Error al configurar scheduler: {str(e)}")
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
    
    # Mostrar configuración
    mostrar_configuracion()
    
    # Configurar el scheduler
    configurar_scheduler()
    
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
        logger.info("🔄 Ejecutando procesos iniciales...")
        try:
            ejecutar_proceso_principal()
            ejecutar_proceso_costos()
            #ejecutar_proceso_bm_packing()
            #ejecutar_proceso_images_fcl()
            logger.info("✅ Procesos iniciales completados")
        except Exception as e:
            logger.error(f"❌ Error en procesos iniciales: {str(e)}")
        
    
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



