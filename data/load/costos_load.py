import pandas as pd
import logging
from datetime import datetime
from data.transform.costos_transform import *
from constant import *
from utils.get_api import subir_archivo_con_reintento


logger = logging.getLogger(__name__)

def tipo_cambio_load_data(access_token):
    df = tipo_cambio_transform(access_token)
    logger.info(f"📤 Subiendo archivo TIPO DE CAMBIO.xlsx a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="TIPO DE CAMBIO.xlsx",
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE
    )

    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: TIPO DE CAMBIO.xlsx")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False

def costos_transporte_packing_load_data(access_token,tiempo_inicio):
    df = costos_transporte_packing_transform(access_token)
    logger.info(f"📤 Subiendo archivo '{FILE_NAME_PROCESADO_TPPACKING}' a OneDrive...")
    resultado= subir_archivo_con_reintento(
            access_token=access_token,
            dataframe=df,
            nombre_archivo=FILE_NAME_PROCESADO_TPPACKING,
            drive_id= DRIVE_ID_CARPETA_STORAGE,
            folder_id= FOLDER_ID_CARPETA_STORAGE
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: {FILE_NAME_PROCESADO_TPPACKING}")
        logger.info(f"🎨 Formato aplicado: Encabezados en negrita, fondo azul, tabla Excel")
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False

def costos_concesionario_packing_load_data(access_token,tiempo_inicio):
    df = costos_concesionario_packing_transform()
    logger.info(f"📤 Subiendo archivo '{FILE_NAME_PROCESADO_CONCESIONARIO_PACKING}' a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo=FILE_NAME_PROCESADO_CONCESIONARIO_PACKING,
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: {FILE_NAME_PROCESADO_CONCESIONARIO_PACKING}")
        logger.info(f"🎨 Formato aplicado: Encabezados en negrita, fondo azul, tabla Excel")
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False

def bd_costos_packing_load_data(access_token,tiempo_inicio):
    df = procesamiento_costos_packing_transform(access_token)
    logger.info(f"📤 Subiendo archivo 'AGRUPADOR COSTOS PACKING' a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="AGRUPADOR COSTOS PACKING.xlsx",
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: AGRUPADOR COSTOS PACKING.xlsx")
        logger.info(f"🎨 Formato aplicado: Encabezados en negrita, fondo azul, tabla Excel")
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False
    
def mayor_analitico_opex_load_data(access_token,tiempo_inicio):
    agrupador= centro_costos_packing_extract(access_token)[0]
    df = mayor_analitico_opex_transform(access_token,agrupador)
    logger.info(f"📤 Subiendo archivo 'MAYOR ANALITICO' a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="MAYOR ANALITICO.parquet",
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE,
        type_file="parquet"
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: MAYOR ANALITICO.parquet")
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False


def mayor_analitico_packing_load_data(access_token,tiempo_inicio):
    df = mayor_analitico_packing_transform(access_token)
    logger.info(f"📤 Subiendo archivo 'MAYOR ANALITICO PACKING' a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="MAYOR ANALITICO PACKING.parquet",
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE,
        type_file="parquet"
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: MAYOR ANALITICO PACKING.parquet")
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False

def presupuesto_packing_load_data(access_token,tiempo_inicio):
    df = presupuesto_packing_transform(access_token)
    logger.info(f"📤 Subiendo archivo PRESUPUESTO a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="PRESUPUESTO.parquet",
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE,
        type_file="parquet"
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: PRESUPUESTO.parquet") 
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False
    
def kg_presupuesto_packing_load_data(access_token,tiempo_inicio):
    df = kg_presupuesto_packing_transform(access_token)
    logger.info(f"📤 Subiendo archivo KG PPTO a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="KG PPTO.parquet",
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE,
        type_file="parquet"
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: KG PPTO.parquet")
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False
    

def ocupacion_transporte_packing_load_data(access_token,tiempo_inicio):
    df = ocupacion_transporte_packing_transform(access_token)
    logger.info(f"📤 Subiendo archivo Ocupacion Transporte a OneDrive...")
    resultado = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=df,
        nombre_archivo="OCUPACION TRANSPORTE.parquet",
        drive_id=DRIVE_ID_CARPETA_STORAGE,
        folder_id=FOLDER_ID_CARPETA_STORAGE,
        type_file="parquet"
    )
    fin = datetime.now()
    if resultado:
        logger.info(f"✅ Proceso completado exitosamente")
        logger.info(f"📁 Archivo subido: OCUPACION TRANSPORTE.parquet")
        logger.info(f"⏱️ Tiempo total de ejecución: {fin-tiempo_inicio}")
        return True
    else:
        logger.error(f"❌ Error al subir el archivo")
        return False