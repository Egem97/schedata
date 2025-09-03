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
import psycopg2
from psycopg2.extras import RealDictCursor
import sqlalchemy
from sqlalchemy import create_engine, text
import io
from data.load.ingesta_bd import ingesta_imagenes_eva_calidad_bd
#ingesta_imagenes_eva_calidad_bd()



#access_token = get_access_token_packing()
#access_token = get_access_token()

#df = registro_phl_pt_formatos_transform(access_token)
#st.write(df)








