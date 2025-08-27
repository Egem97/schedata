import pandas as pd
import logging
from datetime import datetime
from data.transform.packing_transform import *
from utils.get_api import subir_archivo_con_reintento

logger = logging.getLogger(__name__)

def tiempos_proceso_packing_load_data(access_token,tiempo_inicio):
    df = tiempos_packing_data_transform()
    logger.info(f"📤 Subiendo archivo '{FILE_NAME_PROCESADO_TIEMPOS_PACKING}' a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo=FILE_NAME_PROCESADO_TIEMPOS_PACKING,
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE_TIEMPOS
      
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: {FILE_NAME_PROCESADO_TIEMPOS_PACKING}")
        logger.info(f"🎨 Formato aplicado: Encabezados en negrita, fondo azul, tabla Excel")
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False

def balance_masa_load_data(access_token,tiempo_inicio):
    df = joins_pt_transform()
    logger.info(f"📤 Subiendo archivo BALANCE DE MASA a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="BALANCE DE MASAS.xlsx",
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: BALANCE DE MASAS.xlsx")
        
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False
    
def reporte_produccion_load_data(access_token,tiempo_inicio):
    df = reporte_produccion_costos_transform()
    logger.info(f"📤 Subiendo archivo REPORTE DE PRODUCCION a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="KG PT.parquet",
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE,
        type_file="parquet"
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: KG PT.parquet")
        
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False

def registro_phl_pt_formatos_load_data(access_token,tiempo_inicio):
    df = registro_phl_pt_formatos_transform(access_token)
    logger.info(f"📤 Subiendo archivo PHL PRODUCTO TERMINADOa OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="PHL PT.parquet",
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE,
        type_file="parquet"
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: PHL PT.parquet")
        
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False
    

def save_images_fcl_drive_load_data(access_token,tiempo_inicio):
    df = images_fcl_drive_extract_transform(access_token)
    logger.info(f"📤 Subiendo archivo Images Evaluacion Calidad...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="Calidad_Images_FCL.parquet",
        drive_id="b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        folder_id="01XOBWFSBLVGULAQNEKNG2WR7CPRACEN7Q",
        type_file="parquet"
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: Calidad_Images_FCL.parquet")
        
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False