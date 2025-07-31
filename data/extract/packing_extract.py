import pandas as pd
import logging
from utils.get_sheets import read_sheet
from constant import *
from utils.helpers import limpiar_kg_exportables

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