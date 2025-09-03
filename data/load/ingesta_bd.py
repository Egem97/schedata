import pandas as pd
import logging
from datetime import datetime
from data.transform.packing_transform import *
from utils.get_api import subir_archivo_con_reintento
from utils.handler_bd import *

logger = logging.getLogger(__name__)

def ingesta_imagenes_eva_calidad_bd():
    df = images_fcl_drive_extract_transform()
    import streamlit as st
    st.dataframe(df)
    try:
#    logger.info(f"✅ Datos procesados: {len(df)} filas")
        cleaned_df = df[df["image_base64"].notna()]
        insert_dataframe_to_postgresql(cleaned_df)
        
    except:
        logger.error(f"❌ No se encontraron datos")
        return False
   