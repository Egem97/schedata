import streamlit as st
import pandas as pd
from datetime import datetime
from utils.get_token import get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida
from constant import *
from utils.helpers import get_download_url_by_name
from data.transform.packing_transform import *
from utils.styles import styles
from data.extract.packing_extract import descarte_extract,producto_terminado_extract
from data.extract.costos_extract import costos_transporte_packing_extract
from data.transform.costos_transform import *
from data.load.costos_load import costos_transporte_packing_load_data
from data.extract.costos_extract import *
from data.transform.costos_transform import costos_concesionario_packing_transform
from utils.helpers import create_format_excel_in_memory
from data.transform.costos_transform import tipo_cambio_transform
from data.load.costos_load import bd_costos_packing_load_data

    
styles(1)
st.title("Pruebas Streamlit")
access_token = get_access_token()
#df = test_images_fcl_drive_extract(access_token)
#st.dataframe(df)
"""

df =
df["folder_name"] = df["folder_name"].str.strip()
df = df.groupby(["folder_name"]).agg({
        "cantidad_images": "sum",
        "base64_complete": lambda x: x.tolist(),
}).reset_index()
"""
#from data.load.packing_load import save_images_fcl_drive_load_data
#save_images_fcl_drive_load_data(access_token)


#df = images_fcl_drive_extract_transform(access_token)
#st.dataframe(df)



#


"""

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = 'nifty-might-269005-cd303aaaa33f.json'
FOLDER_ID = '1OqY3VnNgsbnKRuqVZqFi6QSXqKDC4uox'


"""