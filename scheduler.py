#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Automatización con schedule para procesar datos y subirlos a OneDrive
Usa la configuración del scheduler desde config.yaml
"""

import pandas as pd
import schedule
import time
import logging
import sys
import os
from datetime import datetime
from utils.get_sheets import read_sheet
from utils.get_token import get_config_value, get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida, subir_archivo_con_reintento
from utils.transform_data import recepcion_clean_data, tiempos_transform_packing_data

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

def get_download_url_by_name(json_data, name):
    """
    Busca en el JSON un archivo por su nombre y retorna su downloadUrl
    
    Args:
        json_data (list): Lista de diccionarios con información de archivos
        name (str): Nombre del archivo a buscar
    
    Returns:
        str: URL de descarga del archivo encontrado, o None si no se encuentra
    """
    for item in json_data:
        if item.get('name') == name:
            return item.get('@microsoft.graph.downloadUrl')
    
    return None

def ejecutar_proceso_principal():
    """
    Función principal que ejecuta todo el proceso de datos
    """
    logger = logging.getLogger(__name__)
    
    try:
        inicio = datetime.now()
        logger.info("🚀 Iniciando proceso automatizado...")
        
        # Obtener configuración desde config.yaml
        drive_id = get_config_value('onedrive', 'drive_id')
        usar_timestamp = get_config_value('onedrive', 'usar_timestamp') or False
        
        # Configuración de Google Sheets
        spreadsheet_id = get_config_value('google_sheets', 'spreadsheet_id')
        sheet_name = get_config_value('google_sheets', 'sheet_name')
        
        # Configuración de carpetas
        carpetas = get_config_value('onedrive', 'carpetas')
        carpeta_volcado = carpetas.get('volcado') if carpetas else "01XOBWFSDLRDZDRGI5RBEI4IZMWN5CC2NS"
        carpeta_enfriamiento = carpetas.get('enfriamiento') if carpetas else "01XOBWFSGMVNZHJBTVUVA2T4UDY3TLDBTH"
        carpeta_salida = carpetas.get('salida') if carpetas else "01XOBWFSF6Y2GOVW7725BZO354PWSELRRZ"
        
        # Configuración de archivos
        archivo_volcado = get_config_value('archivos', 'volcado') or "BD VOLCADO DE MATERIA PRIMA.xlsx"
        archivo_enfriamiento = get_config_value('archivos', 'enfriamiento') or "ENFRIAMIENTO 2025.xlsx"
        archivo_salida = get_config_value('archivos', 'salida') or "datos_procesados.xlsx"
        
        # Obtener token de acceso
        logger.info("🔑 Obteniendo token de acceso...")
        access_token = get_access_token()
        if not access_token:
            logger.error("❌ No se pudo obtener el token de acceso")
            return False
        
        # Leer datos de Google Sheets
        logger.info(f"📊 Leyendo datos de Google Sheets: {spreadsheet_id}/{sheet_name}")
        data = read_sheet(spreadsheet_id, sheet_name)
        df = pd.DataFrame(data[1:], columns=data[0])
        recepcion_df = recepcion_clean_data(df)
        logger.info(f"✅ Datos de Google Sheets procesados: {len(recepcion_df)} filas")
        
        # Obtener datos de la carpeta de volcado
        logger.info(f"📁 Obteniendo datos de volcado: {archivo_volcado}")
        json_data_volcado = listar_archivos_en_carpeta_compartida(
            access_token,
            drive_id,
            carpeta_volcado
        )
        url_excel_volcado = get_download_url_by_name(json_data_volcado, archivo_volcado)
        
        if not url_excel_volcado:
            logger.error(f"❌ No se encontró el archivo de volcado: {archivo_volcado}")
            return False
            
        df_volcado = pd.read_excel(url_excel_volcado)
        logger.info(f"✅ Datos de volcado obtenidos: {len(df_volcado)} filas")
        
        # Obtener datos de la carpeta de enfriamiento
        logger.info(f"📁 Obteniendo datos de enfriamiento: {archivo_enfriamiento}")
        json_data_recepcion = listar_archivos_en_carpeta_compartida(
            access_token,
            drive_id,
            carpeta_enfriamiento
        )
        url_excel_enfriamiento = get_download_url_by_name(json_data_recepcion, archivo_enfriamiento)
        
        if not url_excel_enfriamiento:
            logger.error(f"❌ No se encontró el archivo de enfriamiento: {archivo_enfriamiento}")
            return False
            
        enfriamiento_df = pd.read_excel(url_excel_enfriamiento)
        logger.info(f"✅ Datos de enfriamiento obtenidos: {len(enfriamiento_df)} filas")
        
        # Procesar datos
        logger.info("🔄 Procesando datos...")
        dff = tiempos_transform_packing_data(recepcion_df, enfriamiento_df, df_volcado)
        logger.info(f"✅ Datos procesados: {len(dff)} filas")
        
        # Generar nombre de archivo
        if usar_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nombre_archivo = f"{archivo_salida.replace('.xlsx', '')}_{timestamp}.xlsx"
        else:
            nombre_archivo = archivo_salida
        
        # Subir el DataFrame a OneDrive
        logger.info(f"📤 Subiendo archivo '{nombre_archivo}' a OneDrive...")
        resultado_subida = subir_archivo_con_reintento(
            access_token=access_token,
            dataframe=dff,
            nombre_archivo=nombre_archivo,
            drive_id=drive_id,
            folder_id=carpeta_salida
        )
        
        fin = datetime.now()
        tiempo_total = fin - inicio
        
        # Mostrar resultados
        if resultado_subida:
            logger.info(f"✅ Proceso completado exitosamente")
            logger.info(f"📁 Archivo subido: {nombre_archivo}")
            logger.info(f"🎨 Formato aplicado: Encabezados en negrita, fondo azul, tabla Excel")
            logger.info(f"⏱️ Tiempo total de ejecución: {tiempo_total}")
            return True
        else:
            logger.error(f"❌ Error al subir el archivo")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en el proceso principal: {str(e)}")
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
    
    if interval == 'daily':
        schedule.every().day.at(time_config).do(ejecutar_proceso_principal)
        logger.info(f"⏰ Programado para ejecutarse diariamente a las {time_config}")
        
    elif interval == 'hourly':
        schedule.every().hour.do(ejecutar_proceso_principal)
        logger.info(f"⏰ Programado para ejecutarse cada hora")
        
    elif interval == 'minutes':
        schedule.every(minutes).minutes.do(ejecutar_proceso_principal)
        logger.info(f"⏰ Programado para ejecutarse cada {minutes} minutos")
        
    else:
        logger.warning(f"⚠️ Interval '{interval}' no reconocido, usando 15 minutos por defecto")
        schedule.every(15).minutes.do(ejecutar_proceso_principal)

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
        logger.info("🔄 Ejecutando proceso inicial...")
        ejecutar_proceso_principal()
    
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