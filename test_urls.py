#!/usr/bin/env python3
"""
Script de prueba para verificar las diferentes URLs de Google Drive
"""

import pandas as pd
from utils.get_sheets import extract_all_data_for_streamlit, authenticate_google_drive

def test_urls():
    """Probar todas las URLs generadas"""
    print("🧪 Iniciando pruebas de URLs...")
    print("=" * 50)
    
    # Extraer datos con todas las opciones
    df = extract_all_data_for_streamlit(
        make_public=True,  # Hacer archivos públicos
        include_base64=True  # Incluir base64
    )
    
    if df is None or df.empty:
        print("❌ No se pudieron extraer datos")
        return
    
    print(f"✅ Datos extraídos: {len(df)} filas")
    
    # Filtrar solo imágenes con datos
    df_images = df[df['image_id'].notna()]
    print(f"📸 Imágenes encontradas: {len(df_images)}")
    
    if len(df_images) == 0:
        print("❌ No se encontraron imágenes")
        return
    
    # Tomar la primera imagen para pruebas
    test_image = df_images.iloc[0]
    print(f"\n🔍 Probando imagen: {test_image['image_name']}")
    print(f"📁 Carpeta: {test_image['folder_name']}")
    print(f"🆔 ID: {test_image['image_id']}")
    
    # Probar cada tipo de URL
    url_types = [
        ('URL Normal', 'image_thumbnail_url'),
        ('URL Pública', 'image_public_thumbnail_url'),
        ('URL Autenticada', 'image_authenticated_url'),
        ('URL Compatible con Streamlit', 'image_streamlit_compatible_url'),
        ('URL de Descarga', 'image_download_url'),
        ('URL de Descarga Pública', 'image_public_download_url'),
        ('URL de Descarga Directa', 'image_direct_download_url'),
        ('URL de Vista Web', 'image_web_content_url'),
    ]
    
    print("\n📋 URLs generadas:")
    print("-" * 50)
    
    for url_name, column_name in url_types:
        url = test_image.get(column_name)
        if url:
            print(f"✅ {url_name}: {url}")
        else:
            print(f"❌ {url_name}: No disponible")
    
    # Verificar base64
    if test_image.get('image_base64'):
        base64_length = len(test_image['image_base64'])
        print(f"✅ Base64: Disponible ({base64_length} caracteres)")
    else:
        print("❌ Base64: No disponible")
    
    print("\n" + "=" * 50)
    print("🎯 Pruebas completadas")
    
    # Guardar resultados
    df.to_csv("test_urls_results.csv", index=False)
    print("💾 Resultados guardados en 'test_urls_results.csv'")

if __name__ == "__main__":
    test_urls()

