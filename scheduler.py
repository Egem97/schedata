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
from data.transform.packing import dataTranformTransporteControl
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

def ejecutar_proceso_proceso_packing():
    """
    Función COSTOS PACKING
    """
    logger = logging.getLogger(__name__)
    try:
        inicio = datetime.now()
        logger.info("🚀 Iniciando proceso automatizado...")
        logger.info("🔑 Obteniendo token de acceso...")
        access_token = get_access_token()
        if not access_token:
            logger.error("❌ No se pudo obtener el token de acceso")
            return False
        

        logger.info(f"📁 Obteniendo datos de Transporte Packing: ")
        json_transporte_packing = listar_archivos_en_carpeta_compartida(
            access_token,
            DRIVE_ID_TRANSPORTE_PACKING,
            ITEM_ID_TRANSPORTE_PACKING
        )
        url_excel_tp_packing = get_download_url_by_name(json_transporte_packing, FILE_NAME_TRANSPORTE_PACKING)
        
        if not url_excel_tp_packing:
            logger.error(f"❌ No se encontró el archivo de volcado: {url_excel_tp_packing}")
            return False
            
        tp_packing_df = pd.read_excel(url_excel_tp_packing)
        tp_packing_df = dataTranformTransporteControl(tp_packing_df)
        logger.info(f"✅ Datos de volcado obtenidos: {len(tp_packing_df)} filas")
        logger.info(f"📤 Subiendo archivo '{FILE_NAME_PROCESADO_TPPACKING}' a OneDrive...")
        resultado_subida = subir_archivo_con_reintento(
            access_token=access_token,
            dataframe=tp_packing_df,
            nombre_archivo=FILE_NAME_PROCESADO_TPPACKING,
            drive_id= DRIVE_ID_CARPETA_STORAGE,
            folder_id= FOLDER_ID_CARPETA_STORAGE
        )
        
        if resultado_subida:
            logger.info(f"✅ Proceso completado exitosamente ({FILE_NAME_PROCESADO_TPPACKING})")
            
        
        logger.info(f"📁 Obteniendo datos de Concesionario Packing: ")
        nombres_meses = ["Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        mes_actual = datetime.now().month
        meses_a_leer = nombres_meses[:mes_actual-12]   
        sheet_id = "1VJy5BMZ6ZV14K_g28AfKivqlThZiK-Dp"
        concesonario_packing_df = pd.DataFrame()
        
        logger.info(f"Intentando leer datos para los meses: {', '.join(meses_a_leer)}")
        
        for mes in meses_a_leer:
            sheet_name = f"Alimentacion_{mes}"
            url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
            
            try:
                df = pd.read_csv(url)
                
                if mes == "Junio":
                    df = df[df.columns[:8]]
                df.columnas = ['NRO. DOC.', 'APELLIDOS, NOMBRE', 'TIPO TRABAJADOR', 'AREA','PUESTO/LABOR', 'CANTIDAD', 'TIPO MENU', 'FECHA',]
                
                concesonario_packing_df = pd.concat([concesonario_packing_df,df], ignore_index=True)
            except Exception as e:
                logger.info(f"⚠ La hoja {sheet_name} no existe o no se pudo leer: {e}")
                continue
        
        
        logger.info(f"📤 Subiendo archivo 'Concesionario Packing' a OneDrive...")
        resultado_subida = subir_archivo_con_reintento(
            access_token=access_token,
            dataframe=concesonario_packing_df,
            nombre_archivo=FILE_NAME_PROCESADO_CONCESIONARIO_PACKING,
            drive_id=DRIVE_ID_CARPETA_STORAGE,
            folder_id=FOLDER_ID_CARPETA_STORAGE
          
        )
        fin = datetime.now()
        tiempo_total = fin - inicio
        
        if resultado_subida:
            logger.info(f"✅ Proceso completado exitosamente")
            logger.info(f"📁 Archivo subido: {FILE_NAME_PROCESADO_CONCESIONARIO_PACKING}")
            logger.info(f"🎨 Formato aplicado: Encabezados en negrita, fondo azul, tabla Excel")
            logger.info(f"⏱️ Tiempo total de ejecución: {tiempo_total}")
            return True
        else:
            logger.error(f"❌ Error al subir el archivo")
            return False
    except Exception as e:
        logger.error(f"❌ Error en el proceso principal: {str(e)}")
        return False

















def ejecutar_proceso_principal():
    """
    Función TIEMPOS PACKING
    """
    logger = logging.getLogger(__name__)
    
    try:
        inicio = datetime.now()
        logger.info("🚀 Iniciando proceso automatizado...")
        # Configuración de Google Sheets
        spreadsheet_id = get_config_value('google_sheets', 'spreadsheet_id')
        sheet_name = get_config_value('google_sheets', 'sheet_name')
        
        logger.info("🔑 Obteniendo token de acceso...")
        access_token = get_access_token()
        if not access_token:
            logger.error("❌ No se pudo obtener el token de acceso")
            return False
        
        # Leer datos de Google Sheets
        logger.info(f"📊 Leyendo datos de Recepcion: ")
        data = read_sheet(spreadsheet_id, sheet_name)
        recepcion_df = pd.DataFrame(data[1:], columns=data[0])
        recepcion_df = recepcion_clean_data(recepcion_df)
        logger.info(f"✅ Datos de Recepcion procesados: {len(recepcion_df)} filas")
        # Obtener datos de la carpeta de enfriamiento
        #https://docs.google.com/spreadsheets/d/1odN1K_xwdXms-7kOCk3SHtLULs3OoKyzU2fRNTRhIu8/edit?gid=0#gid=0
        logger.info(f"📁 Obteniendo datos de enfriamiento: ")
        enfriamiento_data = read_sheet("1odN1K_xwdXms-7kOCk3SHtLULs3OoKyzU2fRNTRhIu8", "ENFRIAMIENTO DE MP 2025")
        enfriamiento_df = pd.DataFrame(enfriamiento_data[1:], columns=enfriamiento_data[0])
        logger.info(f"✅ Datos de enfriamiento obtenidos: {len(enfriamiento_df)} filas")
        # Obtener datos de la carpeta de volcado
        logger.info(f"📁 Obteniendo datos de volcado: ")
        volcado_data = read_sheet("1jcVIIkha6fnoqN5PSMjSfoOn5tyk2_OzP0uJtC19e6Q", "BD")
        
        volcado_df = pd.DataFrame(volcado_data[1:], columns=volcado_data[0])
        
        
        logger.info(f"✅ Datos de volcado obtenidos: {len(enfriamiento_df)} filas")
        dff =tiempos_transform_packing_data(recepcion_df,enfriamiento_df,volcado_df)
        logger.info(f"✅ DatosTIEMPOS PACKING procesados: {len(dff)} filas")

        
        # Subir el DataFrame a OneDrive
        logger.info(f"📤 Subiendo archivo '{FILE_NAME_PROCESADO_TIEMPOS_PACKING}' a OneDrive...")
        resultado2_subida = subir_archivo_con_reintento(
            access_token=access_token,
            dataframe=dff,
            nombre_archivo=FILE_NAME_PROCESADO_TIEMPOS_PACKING,
            drive_id=DRIVE_ID_CARPETA_STORAGE,
            folder_id=FOLDER_ID_CARPETA_STORAGE_TIEMPOS
          
        )
        
        fin = datetime.now()
        tiempo_total = fin - inicio
        
        # Mostrar resultados
        if resultado2_subida:
            logger.info(f"✅ Proceso completado exitosamente")
            logger.info(f"📁 Archivo subido: {FILE_NAME_PROCESADO_TIEMPOS_PACKING}")
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
        #schedule.every().day.at(time_config).do(ejecutar_proceso_proceso_packing)
        logger.info(f"⏰ Programado para ejecutarse diariamente a las {time_config}")
        
    elif interval == 'hourly':
        schedule.every().hour.do(ejecutar_proceso_principal)
        #schedule.every().hour.do(ejecutar_proceso_proceso_packing)
        logger.info(f"⏰ Programado para ejecutarse cada hora")
        
    elif interval == 'minutes':
        schedule.every(minutes).minutes.do(ejecutar_proceso_principal)
        #schedule.every(minutes).minutes.do(ejecutar_proceso_proceso_packing)
        logger.info(f"⏰ Programado para ejecutarse cada {minutes} minutos")
        
    else:
        logger.warning(f"⚠️ Interval '{interval}' no reconocido, usando 15 minutos por defecto")
        schedule.every(15).minutes.do(ejecutar_proceso_principal)
        #schedule.every(15).minutes.do(ejecutar_proceso_proceso_packing)

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
        logger.info("🔄 Ejecutando proceso packing...")
        ejecutar_proceso_principal()
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