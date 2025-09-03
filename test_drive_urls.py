#!/usr/bin/env python3
"""
Script de prueba para las funciones de URLs de descarga de Google Drive
"""

import pandas as pd
from utils.get_sheets import extract_all_data_with_urls, get_download_url, get_thumbnail_url, authenticate_google_drive

def test_url_functions():
    """Probar las funciones de URLs"""
    print("🧪 Probando funciones de URLs de Google Drive")
    print("=" * 50)
    
    # Autenticar
    service = authenticate_google_drive()
    if not service:
        print("❌ Error de autenticación")
        return
    
    print("✅ Autenticación exitosa")
    
    # Probar extracción completa
    print("\n🚀 Extrayendo datos con URLs...")
    df = extract_all_data_with_urls()
    
    if df is not None and not df.empty:
        print(f"✅ Extracción completada: {len(df)} filas")
        
        # Mostrar estadísticas
        print(f"\n📊 Estadísticas:")
        print(f"   - Total de filas: {len(df)}")
        print(f"   - Imágenes con URLs: {len(df[df['image_download_url'].notna()])}")
        print(f"   - Carpetas procesadas: {df['folder_name'].nunique()}")
        
        # Mostrar primeras filas
        print(f"\n📋 Primeras 5 filas:")
        print(df.head().to_string())
        
        # Mostrar URLs de descarga
        urls_df = df[df['image_download_url'].notna()][['image_name', 'image_download_url', 'image_thumbnail_url']]
        if not urls_df.empty:
            print(f"\n🔗 URLs de descarga (primeras 3):")
            print(urls_df.head(3).to_string())
        
        # Guardar CSV
        csv_filename = "drive_images_urls.csv"
        df.to_csv(csv_filename, index=False)
        print(f"\n💾 Datos guardados en: {csv_filename}")
        
    else:
        print("❌ No se pudieron extraer datos")

def test_individual_urls():
    """Probar funciones individuales de URLs"""
    print("\n🔧 Probando funciones individuales...")
    
    service = authenticate_google_drive()
    if not service:
        return
    
    # Ejemplo con un ID de archivo (necesitarías un ID real)
    test_file_id = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"  # Ejemplo de Google
    
    print(f"📝 Probando con ID: {test_file_id}")
    
    # Probar URL de descarga
    download_url = get_download_url(service, test_file_id)
    print(f"   📥 URL de descarga: {download_url}")
    
    # Probar URL de miniatura
    thumbnail_url = get_thumbnail_url(service, test_file_id)
    print(f"   🖼️  URL de miniatura: {thumbnail_url}")

if __name__ == "__main__":
    print("🎯 Iniciando pruebas de URLs de Google Drive")
    print("=" * 60)
    
    # Probar extracción completa
    test_url_functions()
    
    # Probar funciones individuales
    test_individual_urls()
    
    print("\n✅ Pruebas completadas")
