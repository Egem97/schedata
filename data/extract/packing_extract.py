import pandas as pd
import logging
from utils.get_sheets import read_sheet
from constant import *
from utils.helpers import *
from utils.get_api import listar_archivos_en_carpeta_compartida
from utils.get_token import get_access_token_packing

logger = logging.getLogger(__name__)




def recepcion_extract():
    logger.info(f"📁 Obteniendo datos de recepcion: ")
    data = read_sheet("1PWz0McxGvGGD5LzVFXsJTaNIAEYjfWohqtimNVCvTGQ", "KF")
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

def enfriamiento_extract():
    logger.info(f"📁 Obteniendo datos de enfriamiento: ")
    #data = read_sheet("1odN1K_xwdXms-7kOCk3SHtLULs3OoKyzU2fRNTRhIu8", "ENFRIAMIENTO DE MP 2025")
    #df = pd.DataFrame(data[1:], columns=data[0])
    data = listar_archivos_en_carpeta_compartida(
        get_access_token_packing(),
        "b!8PNDXtlxLkaPga8GOB87BU1-H5NesxJIgOJah3G83Hm5hbrxL1sgT7Xj5bnL_Hi9",
        "01S3ZMVBOK65VGLNURUBDYT2KG3GHWTRMD"
    )
    url_excel = get_download_url_by_name(data, "ENFRIAMIENTO 2025.xlsx")
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de enfriamiento:")
        return False
    
    return pd.read_excel(url_excel,sheet_name="ENFRIAMIENTO")
    

def volcado_extract():
    logger.info(f"📁 Obteniendo datos de volcado: ")
    data = read_sheet("1jcVIIkha6fnoqN5PSMjSfoOn5tyk2_OzP0uJtC19e6Q", "BD")
    df = pd.DataFrame(data[1:], columns=data[0])
   
    return df

def descarte_extract():
    logger.info(f"📁 Obteniendo datos de descarte: ")
    data = read_sheet("1jcVIIkha6fnoqN5PSMjSfoOn5tyk2_OzP0uJtC19e6Q", "DESCARTE")
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

def producto_terminado_extract():
    logger.info(f"📁 Obteniendo datos de Producto Terminado: ")
    data = read_sheet("1d5yaDOW69JW_PClEkUH63RFXCsMITPASp19hFG2pqWk", "BD")
    df = pd.DataFrame(data[1:], columns=data[0])
    df.to_excel("producto_terminado2.xlsx", index=False)
    return df
##################################################################################
def reporte_produccion_extract(access_token):
    logger.info(f"📁 Obteniendo datos de reporte de produccion: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!8PNDXtlxLkaPga8GOB87BU1-H5NesxJIgOJah3G83Hm5hbrxL1sgT7Xj5bnL_Hi9",
        "01S3ZMVBN4SH4IF5FDI5GIGOXDH2MPZCJN"
    )
    url_excel = get_download_url_by_name(data, "REPORTE DE PRODUCCION APP.xlsx")
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de images fcl drive:")
        return False
    
    return pd.read_excel(url_excel,sheet_name="RP")#

def evaluacion_calidad_pt_extract(access_token):
    logger.info(f"📁 Obteniendo datos de evaluacion de calidad: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!k0xKW2h1VkGnxasDN0z40PeA8yi0BwBKgEf_EOEPStmAWVEVjX8MQIydW1yMzk1b",
        "01SPKVU4I6RWNBBAVFIJF3GHBOYOFMUKZS"
    )
    url_excel = get_download_url_by_name(data, "BD EVALUACION DE CALIDAD DE PRODUCTO TERMINADO.xlsx")
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de evaluacion calidad pt:")
        return False
    
    return pd.read_excel(url_excel,sheet_name="CALIDAD PRODUCTO TERMINADO")

########################################################################################
"""
    logger.info(f"📁 Obteniendo datos de Reporte de Produccion: ")
    data = read_sheet("1OCBDYRmboSgcQIH0zJQqbAnwTB8f9zSIOaUWBWUXaUM", "RP")
    df = pd.DataFrame(data[1:], columns=data[0])
    df = df[[
    "Semana","Fecha de cosecha","Fecha de proceso","Turno Proceso","Empresa","Tipo","Fundo","Variedad","Kg Procesados","Kg Descarte",
    "% Descarte","Kg Sobre Peso","% Sobre Peso","Kg Merma","% Merma","% Rendimiento MP","Kg Exportables","%. Kg Exportables","TOTAL CAJAS EXPORTADAS"
    ]]
"""



def registro_phl_pt_extract(access_token):
    
    logger.info(f"📁 Obteniendo datos de PHL PT: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!Mx2p-6knhUeohEjU-L-a3w-JZv1JawxAkEY9khgxn7hWjhq65fg_To08YnAwHSc0",
        "01L5M4SATOWDS7G66DCNCYJLOJVNY7SPTV"
    )
    url_excel = get_download_url_by_name(data, "REGISTRO DE PHL - PRODUCTO TERMINADO -154.xlsm")
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de PHL PT:")
        return False
    
    return pd.read_excel(url_excel,sheet_name="TD-DATOS PT")

def agrupador_rp_extract():
    logger.info(f"📁 Obteniendo datos de AGRUPADOR RP: ")
    
    return pd.read_excel("./src/storage/AGRUPADOR RP.xlsx",sheet_name="AGRUPADOR RP"),pd.read_excel("./src/storage/AGRUPADOR RP.xlsx",sheet_name="AGRUPADOR DE CAJAS")

def images_fcl_drive_extract(access_token):
    
    logger.info(f"📁 Obteniendo datos de images fcl drive: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSBLVGULAQNEKNG2WR7CPRACEN7Q"
    )
    url_parquet = get_download_url_by_name(data, "Calidad_Images_FCL.parquet")
    if not url_parquet:
        logger.error(f"❌ No se encontró el archivo de images fcl drive:")
        return False
    
    return pd.read_parquet(url_parquet)

def test_images_fcl_drive_extract(access_token):
    
    logger.info(f"📁 Obteniendo datos de images fcl drive: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR",
        "01XOBWFSBLVGULAQNEKNG2WR7CPRACEN7Q"
    )
    url_parquet = get_download_url_by_name(data, "Calidad_Images_FCL.parquet")
    if not url_parquet:
        logger.error(f"❌ No se encontró el archivo de images fcl drive:")
        return False
    
    return pd.read_parquet(url_parquet)

def phl_pt_all_tabla_extract(access_token):
    
    logger.info(f"📁 Obteniendo datos de PHL PT: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        "b!Mx2p-6knhUeohEjU-L-a3w-JZv1JawxAkEY9khgxn7hWjhq65fg_To08YnAwHSc0",
        "01L5M4SATOWDS7G66DCNCYJLOJVNY7SPTV"
    )
    url_excel = get_download_url_by_name(data, "REGISTRO DE PHL - PRODUCTO TERMINADO -154.xlsm")
    
     
    return (
        pd.read_excel(url_excel,sheet_name="GMH-ABG-CYN"),
        pd.read_excel(url_excel,sheet_name="GAP BERRIES"),
        pd.read_excel(url_excel,sheet_name="SAN LUCAR"),
        pd.read_excel(url_excel,sheet_name="SAN EFISIO"),
    )
    
 