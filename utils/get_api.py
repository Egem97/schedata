import requests
import time
import pandas as pd
import io
from pathlib import Path
from utils.get_token import get_access_token, load_config
from utils.helpers import create_format_excel_in_memory

config = load_config()

MAX_RETRIES = config['processing']['max_retries']
RETRY_DELAY = config['processing']['retry_delay']

def listar_archivos_en_carpeta_compartida(access_token: str  ,drive_id: str, item_id: str):
    """
    Lista los archivos dentro de una carpeta compartida en OneDrive / SharePoint usando Microsoft Graph.

    :param access_token: Token de acceso válido con permisos Files.Read.All
    :param drive_id: El ID del drive compartido
    :param item_id: El ID de la carpeta compartida
    :return: Lista de archivos o carpetas dentro de esa carpeta
    """
    
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/children"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json().get("value", [])
    else:
        print("❌ Error al obtener archivos:", response.status_code)
        print(response.json())
        return []

def subir_archivo(access_token: str, dataframe: pd.DataFrame, nombre_archivo: str, drive_id: str, folder_id: str) -> bool:
    """
    Sube un DataFrame como archivo Excel formateado a OneDrive/SharePoint
    
    Args:
        access_token: Token de acceso válido
        dataframe: DataFrame de pandas a subir
        nombre_archivo: Nombre del archivo (debe incluir .xlsx)
        drive_id: ID del drive de OneDrive/SharePoint
        folder_id: ID de la carpeta destino
    
    Returns:
        bool: True si se subió exitosamente, False si hubo error
    """
    try:
        print(f"📊 Aplicando formato especial al archivo Excel...")
        
        # Usar la función de formato especial para crear el Excel en memoria
        excel_data = create_format_excel_in_memory(dataframe)
        
        # Construir URL para subir archivo
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}:/{nombre_archivo}:/content"
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
        
        # Realizar la petición
        response = requests.put(url, headers=headers, data=excel_data)
        
        if response.status_code in [200, 201]:
            print(f"✅ Archivo '{nombre_archivo}' subido exitosamente con formato especial")
            return True
        else:
            print(f"❌ Error al subir archivo: {response.status_code}")
            print(response.json())
            return False
            
    except Exception as e:
        print(f"❌ Error al procesar archivo: {str(e)}")
        return False

def subir_archivo_con_reintento(access_token: str, dataframe: pd.DataFrame, nombre_archivo: str, drive_id: str, folder_id: str) -> bool:
    """
    Sube un DataFrame con formato especial y reintentos automáticos
    
    Args:
        access_token: Token de acceso válido
        dataframe: DataFrame de pandas a subir
        nombre_archivo: Nombre del archivo (debe incluir .xlsx)
        drive_id: ID del drive de OneDrive/SharePoint
        folder_id: ID de la carpeta destino
    
    Returns:
        bool: True si se subió exitosamente, False si falló después de todos los reintentos
    """
    for intento in range(MAX_RETRIES):
        try:
            resultado = subir_archivo(access_token, dataframe, nombre_archivo, drive_id, folder_id)
            if resultado:
                return True
            
            if intento < MAX_RETRIES - 1:
                print(f"🔄 Reintentando en {RETRY_DELAY} segundos... (Intento {intento + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            
        except Exception as e:
            print(f"❌ Error en intento {intento + 1}: {str(e)}")
            if intento < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    
    print(f"❌ No se pudo subir el archivo después de {MAX_RETRIES} intentos")
    return False
        
        
        
        
