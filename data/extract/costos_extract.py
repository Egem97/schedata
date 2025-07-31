import pandas as pd
import logging
from datetime import datetime
from constant import *
from utils.get_sheets import read_sheet
from utils.get_api import listar_archivos_en_carpeta_compartida
from utils.helpers import get_download_url_by_name,get_month_name



logger = logging.getLogger(__name__)

def tipo_cambio_extract(access_token):
    logger.info(f"📁 Obteniendo datos de tipo de cambio: ")
    data = listar_archivos_en_carpeta_compartida(
            access_token,
            DRIVE_ID_CARPETA_STORAGE,
            FOLDER_ID_CARPETA_STORAGE
    )
    url_excel = get_download_url_by_name(data, "TIPO DE CAMBIO.xlsx")
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de Tipo de Cambio: TIPO DE CAMBIO.xlsx")
        return False
    return pd.read_excel(url_excel)


def mayor_analitico_packing_extract(access_token):
    data = listar_archivos_en_carpeta_compartida(
            access_token,
            DRIVE_ID_COSTOS_PACKING,
            ITEM_ID_COSTOS_PACKING
        )
    url_excel = get_download_url_by_name(data, "Mayor Analitico.xlsx")
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de Transporte: {url_excel}")
        return False
    return pd.read_excel(url_excel)


def costos_transporte_packing_extract(access_token):
    data = listar_archivos_en_carpeta_compartida(
            access_token,
            DRIVE_ID_TRANSPORTE_PACKING,
            ITEM_ID_TRANSPORTE_PACKING
        )
    url_excel = get_download_url_by_name(data, FILE_NAME_TRANSPORTE_PACKING)
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de Transporte: {url_excel}")
        return False
    return pd.read_excel(url_excel)
        

def costos_concesionario_packing_extract():
    logger.info(f"📁 Obteniendo datos de concesionario: ")
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
            df.columnas = ['NRO. DOC.', 'APELLIDOS, NOMBRE', 'TIPO TRABAJADOR', 'AREA','PUESTO/LABOR', 'CANTIDAD', 'TIPO MENU', 'FECHA']
                
            concesonario_packing_df = pd.concat([concesonario_packing_df,df], ignore_index=True)
        except Exception as e:
                logger.info(f"⚠ La hoja {sheet_name} no existe o no se pudo leer: {e}")
                continue
    return concesonario_packing_df

def planilla_adm_packing_extract(access_token):
    logger.info(f"📁 Obteniendo datos de planilla adm: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSEY3CTJXXAR3BGI6XE6UAGUG4UA"
    )
    
    now = datetime.now()
    year_now = now.year
    month_number_now = now.month
    
    # Lista para almacenar todos los DataFrames
    dataframes_list = []
    
    # Primero, cargar los datos de enero a mayo desde el archivo parquet
    url_2025_parquet = get_download_url_by_name(data, "Planilla_ENE_MAY.parquet")
    if url_2025_parquet:
        try:
            df_ene_may = pd.read_parquet(url_2025_parquet)
            dataframes_list.append(df_ene_may)
            logger.info(f"✅ Cargados datos de Enero-Mayo desde archivo parquet")
        except Exception as e:
            logger.warning(f"⚠️ No se pudo cargar el archivo parquet ENE-MAY: {e}")
    
    # Iterar desde junio (mes 6) hasta el mes actual
    for month_num in range(6, month_number_now + 1):
        month_text = get_month_name(month_num)
        file_name = f"{month_num:02d}. PLANILLA - FIN DE MES - {month_text} {year_now}.xlsx"
        
        url_excel = get_download_url_by_name(data, file_name)
        
        if url_excel:
            try:
                # Leer el archivo Excel
                df_month = pd.read_excel(url_excel, sheet_name="PLANILLA", skiprows=3)
                dataframes_list.append(df_month)
                logger.info(f"✅ Cargados datos de {month_text} ({month_num:02d})")
            except Exception as e:
                logger.warning(f"⚠️ No se pudo leer el archivo de {month_text}: {e}")
                continue
        else:
            logger.warning(f"⚠️ No se encontró el archivo para {month_text}: {file_name}")
            continue
    
    # Concatenar todos los DataFrames encontrados
    if dataframes_list:
        df_final = pd.concat(dataframes_list, ignore_index=True)
        logger.info(f"✅ Concatenación exitosa: {len(dataframes_list)} archivos procesados, {len(df_final)} filas totales")
        return df_final
    else:
        logger.error(f"❌ No se encontraron archivos de planilla para procesar")
        return False
    
def planilla_obreros_packing_extract(access_token):
    logger.info(f"📁 Obteniendo datos de planilla adm: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSEY3CTJXXAR3BGI6XE6UAGUG4UA"
        
    )
    #url_excel = get_download_url_by_name(data, "Planilla_ENE_MAY.parquet")
    url = get_download_url_by_name(data, "PLANILLA OBREROS.xlsm")
    
    if not url or not url:
        logger.error(f"❌ No se encontró el archivo de Planilla Adm:")
        return False
    return pd.read_excel(url,sheet_name="PLANILLA DIARIA",skiprows=3)



def centro_costos_packing_extract(access_token):
    logger.info(f"📁 Obteniendo datos de Centro de Costos: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!DKrRhqg3EES4zcUVZUdhr281sFZAlBZDuFVNPqXRguBl81P5QY7KRpUL2n3RaODo",
        "01PNBE7BDDPRCTEUCL5ZFLQCKHUA4RJAF2"
    )
    url_excel = get_download_url_by_name(data, "AGRUPADOR_COSTOS.xlsx")
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de Centro de Costos:")
        return False
    df = pd.read_excel(url_excel,sheet_name="Agrupador_Costos")
    df["ITEM"] = df["ITEM"].str.upper()
    df["AGRUPADOR"] = df["AGRUPADOR"].str.upper()
    df["SUB AGRUPADOR"] = df["SUB AGRUPADOR"].str.upper()
    return df,pd.read_excel(url_excel,sheet_name="Centro_Costos_Packing")


def presupuesto_packing_extract(access_token):
    logger.info(f"📁 Obteniendo datos de Presupuesto: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        DRIVE_ID_COSTOS_PACKING,
        ITEM_ID_COSTOS_PACKING
    )
    url_excel = get_download_url_by_name(data, "PPTO PACKING.xlsx")
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de Presupuesto:")
        return False
    
    return pd.read_excel(url_excel,sheet_name="PRESUPUESTADO")

def kg_presupuesto_packing_extract(access_token):
    logger.info(f"📁 Obteniendo datos de Presupuesto: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        DRIVE_ID_COSTOS_PACKING,
        ITEM_ID_COSTOS_PACKING
    )
    url_excel = get_download_url_by_name(data, "KG PPTO.xlsx")
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de KG Presupuesto:")
        return False
    
    return pd.read_excel(url_excel,skiprows=1)
