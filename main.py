#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
from utils.get_sheets import read_sheet
from utils.get_token import get_config_value, print_config, get_access_token
from utils.get_api import listar_archivos_en_carpeta_compartida, subir_archivo_con_reintento
from utils.transform_data import recepcion_clean_data,tiempos_transform_packing_data

def get_download_url_by_name(json_data, name):
    """
    Busca en el JSON un archivo por su nombre y retorna su downloadUrl
    
    Args:
        json_data (list): Lista de diccionarios con información de archivos
        name (str): Nombre del archivo a buscar
    
    Returns:
        str: URL de descarga del archivo encontrado, o None si no se encuentra
    """
    for item in json_data:
        if item.get('name') == name:
            return item.get('@microsoft.graph.downloadUrl')
    
    return None

# Ejemplo de uso
if __name__ == "__main__":
    inicio = datetime.now()
    drive_id = "b!M5ucw3aa_UqBAcqv3a6affR7vTZM2a5ApFygaKCcATxyLdOhkHDiRKl9EvzaYbuR"
    
    # Obtener token de acceso
    access_token = get_access_token()
    
    # Leer datos de Google Sheets
    data = read_sheet("1PWz0McxGvGGD5LzVFXsJTaNIAEYjfWohqtimNVCvTGQ","KF")
    df = pd.DataFrame(data[1:], columns=data[0],)
    recepcion_df = recepcion_clean_data(df)
    
    # Obtener datos de la carpeta de volcado
    json_data_volcado = listar_archivos_en_carpeta_compartida(
        access_token,
        drive_id,
        "01XOBWFSDLRDZDRGI5RBEI4IZMWN5CC2NS"
    )
    url_excel_volcado = get_download_url_by_name(json_data_volcado, "BD VOLCADO DE MATERIA PRIMA.xlsx")
    df_volcado = pd.read_excel(url_excel_volcado,sheet_name="BD")
    
    # Obtener datos de la carpeta de recepción
    json_data_recepcion = listar_archivos_en_carpeta_compartida(
        access_token,
        drive_id,
        "01XOBWFSGMVNZHJBTVUVA2T4UDY3TLDBTH"
    )
    url_excel_enfriamiento = get_download_url_by_name(json_data_recepcion, "ENFRIAMIENTO 2025.xlsx")
    enfriamiento_df = pd.read_excel(url_excel_enfriamiento,sheet_name="ENFRIAMIENTO")
    
    # Procesar datos
    dff = tiempos_transform_packing_data(recepcion_df, enfriamiento_df, df_volcado)
    
    print("Datos procesados:")
    print(dff.head())
    
    # Generar nombre de archivo con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"datos_procesados_test.xlsx"
    
    # Subir el DataFrame a OneDrive con formato especial
    print(f"\n🔄 Subiendo archivo '{nombre_archivo}' a OneDrive con formato especial...")
    resultado_subida = subir_archivo_con_reintento(
        access_token=access_token,
        dataframe=dff,
        nombre_archivo=nombre_archivo,
        drive_id=drive_id,
        folder_id="01XOBWFSF6Y2GOVW7725BZO354PWSELRRZ"
    )
    
    fin = datetime.now()
    
    # Mostrar resultados
    if resultado_subida:
        print(f"✅ Proceso completado exitosamente")
        print(f"📁 Archivo subido: {nombre_archivo}")
        print(f"🎨 Formato aplicado: Encabezados en negrita, fondo azul, tabla Excel, columnas autoajustadas")
    else:
        print(f"❌ Error al subir el archivo")
    
    print(f"⏱️ Tiempo total de ejecución: {fin - inicio}")
    

    
    
    













