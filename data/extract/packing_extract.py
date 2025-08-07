import pandas as pd
import logging
from utils.get_sheets import read_sheet
from constant import *
from utils.helpers import *
from utils.get_api import listar_archivos_en_carpeta_compartida

logger = logging.getLogger(__name__)




def recepcion_extract():
    logger.info(f"📁 Obteniendo datos de recepcion: ")
    data = read_sheet("1PWz0McxGvGGD5LzVFXsJTaNIAEYjfWohqtimNVCvTGQ", "KF")
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

def enfriamiento_extract():
    logger.info(f"📁 Obteniendo datos de enfriamiento: ")
    data = read_sheet("1odN1K_xwdXms-7kOCk3SHtLULs3OoKyzU2fRNTRhIu8", "ENFRIAMIENTO DE MP 2025")
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

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

def reporte_produccion_extract():
    
    logger.info(f"📁 Obteniendo datos de Reporte de Produccion: ")
    data = read_sheet("1OCBDYRmboSgcQIH0zJQqbAnwTB8f9zSIOaUWBWUXaUM", "RP")
    df = pd.DataFrame(data[1:], columns=data[0])
    df = df[[
    "Semana","Fecha de cosecha","Fecha de proceso","Turno Proceso","Empresa","Tipo","Fundo","Variedad","Kg Procesados","Kg Descarte",
    "% Descarte","Kg Sobre Peso","% Sobre Peso","Kg Merma","% Merma","% Rendimiento MP","Kg Exportables","%. Kg Exportables","TOTAL CAJAS EXPORTADAS"
    ]]
    
    return df


def registro_phl_pt_extract(access_token):
    
    logger.info(f"📁 Obteniendo datos de PHL PT: ")
    data = listar_archivos_en_carpeta_compartida(
        access_token,
        DRIVE_ID_CARPETA_STORAGE,
        FOLDER_ID_CARPETA_STORAGE
    )
    url_excel = get_download_url_by_name(data, "REGISTRO DE PHL - PRODUCTO TERMINADO.xlsm")
    if not url_excel:
        logger.error(f"❌ No se encontró el archivo de PHL PT:")
        return False
    
    return pd.read_excel(url_excel,sheet_name="TD-DATOS PT")

def agrupador_rp_extract():
    logger.info(f"📁 Obteniendo datos de AGRUPADOR RP: ")
    
    return pd.read_excel("./src/storage/AGRUPADOR RP.xlsx",sheet_name="AGRUPADOR RP"),pd.read_excel("./src/storage/AGRUPADOR RP.xlsx",sheet_name="AGRUPADOR DE CAJAS")

