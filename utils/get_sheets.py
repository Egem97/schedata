import re
import io
import base64
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from PIL import Image
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account

# Paso 1: Autenticación
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("nifty-might-269005-cd303aaaa33f.json", scope)
client = gspread.authorize(creds)

def read_sheet(key_sheet, sheet_name):
    try:
        spreadsheet = client.open_by_key(key_sheet)
        sheet = spreadsheet.worksheet(sheet_name)
        data = sheet.get_all_values()

        return data
    except Exception as e:
        return key_sheet, f"Error: {str(e)}"

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = 'nifty-might-269005-cd303aaaa33f.json'
FOLDER_ID = '1OqY3VnNgsbnKRuqVZqFi6QSXqKDC4uox'

def authenticate_google_drive():
       
    try:
        credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=credentials)
        return service
    except Exception as e:
            print(f"Error de autenticación: {e}")
            return None
def list_folders(service, folder_id):
        
    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(
        q=query,
        pageSize=1000,
        fields="nextPageToken, files(id, name, mimeType, size, webViewLink,modifiedTime)"
    ).execute()
            
    files = results.get('files', [])
    dff = pd.DataFrame(files)
    
    
   
    dff = dff[dff["modifiedTime"] > f"{str(fecha_actual)}"]#{str(fecha_actual)}
    #dff = dff.head(5)
    #dff = dff.head(4)
    files_ = dff.to_dict(orient="records")
    del dff
    return files_

def list_images_in_folder(service, folder_id):
    """Listar todas las imágenes en la carpeta especificada"""
    
    query = f"'{folder_id}' in parents and (mimeType contains 'image/')"
    results = service.files().list(
            q=query,
            pageSize=1000,
            fields="nextPageToken, files(id, name, mimeType, size, webViewLink)"
    ).execute()
        
    files = results.get('files', [])
    #dff = pd.DataFrame(files)
    #st.dataframe(dff)
    #dff = dff[dff["modifiedTime"] > "2025-08-25"]
    #files_ = dff.to_dict(orient="records")
    
    return files
    
def download_image(service, file_id):

    request = service.files().get_media(fileId=file_id)
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        
    file.seek(0)
    return file

def optimize_image(image_data, max_size=(1200, 1200), quality=90):
    """Optimizar imagen manteniendo alta calidad visual"""
    try:
        # Abrir imagen
        img = Image.open(image_data)
        original_size = img.size
        
        # Convertir a RGB si es necesario
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        
        # Estrategia de redimensionamiento más conservadora
        if original_size[0] > 2000 or original_size[1] > 2000:
            # Imágenes muy grandes: redimensionar moderadamente
            target_size = (1200, 1200)
        elif original_size[0] > 1500 or original_size[1] > 1500:
            # Imágenes grandes: redimensionar ligeramente
            target_size = (1400, 1400)
        elif original_size[0] > 1000 or original_size[1] > 1000:
            # Imágenes medianas: redimensionar muy ligeramente
            target_size = (1000, 1000)
        else:
            # Imágenes pequeñas: mantener tamaño original
            target_size = original_size
        
        # Redimensionar si es necesario
        if img.size[0] > target_size[0] or img.size[1] > target_size[1]:
            img.thumbnail(target_size, Image.Resampling.LANCZOS)
        
        # Estrategia de compresión más conservadora
        # Probar calidades más altas para mantener mejor calidad
        best_output = None
        best_size = float('inf')
        target_quality = quality
        
        # Probar calidades desde 80 hasta 95 para mantener alta calidad
        for test_quality in range(80, 96, 5):
            output = io.BytesIO()
            img.save(output, format='JPEG', quality=test_quality, optimize=True)
            output.seek(0)
            current_size = len(output.getvalue())
            
            # Si el tamaño es menor y la calidad es alta, actualizar
            if current_size < best_size and test_quality >= 85:
                best_size = current_size
                best_output = output
                target_quality = test_quality
        
        # Si no se encontró una buena opción, usar la calidad original
        if best_output is None:
            best_output = io.BytesIO()
            img.save(best_output, format='JPEG', quality=quality, optimize=True)
            best_output.seek(0)
            target_quality = quality
        
        best_output.seek(0)
        
        # Mostrar información de optimización
        original_bytes = len(image_data.getvalue()) if hasattr(image_data, 'getvalue') else 0
        optimized_bytes = len(best_output.getvalue())
        compression_ratio = (1 - optimized_bytes / original_bytes) * 100 if original_bytes > 0 else 0
        
        print(f"         📊 Optimización: {original_size} → {img.size}, {target_quality}% calidad, {compression_ratio:.1f}% reducción")
        
        return best_output
    except Exception as e:
        print(f"Error al optimizar imagen: {e}")
        return None

def apply_advanced_optimization(image_data):
    """Aplicar optimización avanzada manteniendo alta calidad"""
    try:
        # Si la imagen es muy grande, aplicar compresión adicional
        current_size = len(image_data.getvalue())
        
        if current_size > 150000:  # Más de 150KB (aumentado el umbral)
            # Reabrir la imagen para optimización adicional
            image_data.seek(0)
            img = Image.open(image_data)
            
            # Reducir solo si es muy grande
            if img.size[0] > 800 or img.size[1] > 800:
                new_size = (min(800, img.size[0]), min(800, img.size[1]))
                img.thumbnail(new_size, Image.Resampling.LANCZOS)
            
            # Aplicar compresión con calidad más alta
            output = io.BytesIO()
            img.save(output, format='JPEG', quality=85, optimize=True, progressive=True)
            output.seek(0)
            
            print(f"         🔧 Optimización avanzada aplicada: {current_size/1024:.1f}KB → {len(output.getvalue())/1024:.1f}KB")
            return output
        
        return image_data
    except Exception as e:
        print(f"Error en optimización avanzada: {e}")
        return image_data
    
def image_to_base64(image_data):
    """Convertir imagen a base64 con optimización máxima"""
    try:
        # Optimizar imagen
        optimized_image = optimize_image(image_data)
        if optimized_image:
            # Aplicar optimización adicional si la imagen es muy grande
            final_image = apply_advanced_optimization(optimized_image)
            
            # Convertir a base64
            base64_string = base64.b64encode(final_image.getvalue()).decode('utf-8')
            return f"data:image/jpeg;base64,{base64_string}"
        return None
    except Exception as e:
        print(f"Error al convertir imagen a base64: {e}")
        return None
    

def extract_all_data():
    """Extraer todos los datos de Google Drive"""
    print("🚀 Iniciando extracción de datos de Google Drive")
    print("=" * 60)
    
    # Autenticar con Google Drive
    service = authenticate_google_drive()
    if not service:
        print("❌ No se pudo conectar con Google Drive")
        return None
    
    print("✅ Conexión exitosa con Google Drive")
    
    # Listar carpetas
    print("📁 Obteniendo lista de carpetas...")
    folders = list_folders(service, FOLDER_ID)
    
    if not folders:
        print("❌ No se encontraron carpetas")
        return None
    
    print(f"✅ Se encontraron {len(folders)} carpetas")
    
    # Lista para almacenar todos los datos
    all_data = []
    
    # Procesar cada carpeta
    for i, folder in enumerate(folders, 1):
        print(f"\n📂 Procesando carpeta {i}/{len(folders)}: {folder['name']}")
        
        # Obtener imágenes en la carpeta
        images = list_images_in_folder(service, folder['id'])
        
        if not images:
            print(f"   ⚠️  No se encontraron imágenes en '{folder['name']}'")
            # Agregar carpeta sin imágenes como una fila
            all_data.append({
                'folder_id': folder['id'],
                'folder_name': folder['name'],
                'folder_webViewLink': folder['webViewLink'],
                'image_id': None,
                'image_name': None,
                'image_webViewLink': None,
                'image_base64': None,
                'image_size_mb': 0
            })
            continue
        
        print(f"   ✅ Se encontraron {len(images)} imágenes")
        
        # Procesar cada imagen como una fila separada
        for j, image in enumerate(images, 1):
            original_size_mb = int(image.get('size', 0)) / (1024 * 1024)
            print(f"      🖼️  Procesando imagen {j}/{len(images)}: {image['name']} ({original_size_mb:.2f}MB)")
            
            # Descargar imagen
            image_data = download_image(service, image['id'])
            if not image_data:
                print(f"         ❌ Error al descargar {image['name']}")
                # Agregar fila con error
                all_data.append({
                    'folder_id': folder['id'],
                    'folder_name': folder['name'],
                    'folder_webViewLink': folder['webViewLink'],
                    'image_id': image['id'],
                    'image_name': image['name'],
                    'image_webViewLink': image.get('webViewLink'),
                    'image_base64': None,
                    'image_size_mb': original_size_mb
                })
                continue
            
            # Convertir a base64
            base64_image = image_to_base64(image_data)
            if base64_image:
                # Calcular tamaño optimizado
                optimized_size_mb = len(base64_image) * 0.75 / (1024 * 1024)
                reduction_percent = (1 - optimized_size_mb / original_size_mb) * 100 if original_size_mb > 0 else 0
                
                # Agregar fila con imagen procesada
                all_data.append({
                    'folder_id': folder['id'],
                    'folder_name': folder['name'],
                    'folder_webViewLink': folder['webViewLink'],
                    'image_id': image['id'],
                    'image_name': image['name'],
                    'image_webViewLink': image.get('webViewLink'),
                    'image_base64': base64_image,
                    'image_size_mb': optimized_size_mb
                })
                print(f"         ✅ Imagen optimizada: {image['name']} ({reduction_percent:.1f}% reducción)")
            else:
                print(f"         ❌ Error al procesar {image['name']}")
                # Agregar fila con error
                all_data.append({
                    'folder_id': folder['id'],
                    'folder_name': folder['name'],
                    'folder_webViewLink': folder['webViewLink'],
                    'image_id': image['id'],
                    'image_name': image['name'],
                    'image_webViewLink': image.get('webViewLink'),
                    'image_base64': None,
                    'image_size_mb': original_size_mb
                })
        
        print(f"   ✅ Carpeta '{folder['name']}' completada: {len(images)} imágenes procesadas")
    
    print(f"\n✅ Extracción completada: {len(all_data)} filas procesadas")
    return pd.DataFrame(all_data)