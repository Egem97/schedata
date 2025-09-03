# Google Drive URLs - Documentación

Este documento explica las diferentes formas de generar URLs para archivos de Google Drive y cómo hacer que sean accesibles públicamente.

## Tipos de URLs Disponibles

### 1. URLs Normales (Requieren Autenticación)

#### `get_download_url(service, file_id)`
Genera URL de descarga directa que requiere autenticación:
```
https://drive.google.com/uc?id={file_id}&export=download
```

#### `get_thumbnail_url(service, file_id)`
Genera URL de miniatura que requiere autenticación:
```
https://drive.google.com/thumbnail?id={file_id}&sz=w800
```

### 2. URLs Públicas (No Requieren Autenticación)

#### `get_public_download_url(service, file_id)`
Genera URL de descarga pública (archivo debe ser público):
```
https://drive.google.com/uc?export=download&id={file_id}
```

#### `get_public_thumbnail_url(service, file_id)`
Genera URL de miniatura pública:
```
https://drive.google.com/thumbnail?id={file_id}&sz=w800&authuser=0
```

#### `get_web_content_url(service, file_id)`
Genera URL de vista web (alternativa para archivos públicos):
```
https://drive.google.com/file/d/{file_id}/view?usp=sharing
```

## Funciones de Extracción

### `extract_all_data_with_urls()`
Extrae todos los datos con URLs normales (requieren autenticación).

### `extract_all_data_with_public_urls(make_public=False)`
Extrae todos los datos con URLs públicas. Si `make_public=True`, hace públicos los archivos automáticamente.

## Hacer Archivos Públicos

### `make_file_public(service, file_id)`
Hace un archivo público en Google Drive, permitiendo acceso sin autenticación.

**⚠️ Importante:** Esta función cambia los permisos del archivo en Google Drive.

## Columnas del DataFrame

Cuando usas `extract_all_data_with_public_urls()`, el DataFrame incluye:

- `folder_id`: ID de la carpeta
- `folder_name`: Nombre de la carpeta
- `image_id`: ID de la imagen
- `image_name`: Nombre de la imagen
- `image_download_url`: URL de descarga directa (requiere auth)
- `image_thumbnail_url`: URL de miniatura (requiere auth)
- `image_public_download_url`: URL de descarga pública
- `image_public_thumbnail_url`: URL de miniatura pública
- `image_web_content_url`: URL de vista web
- `image_size_mb`: Tamaño en MB

## Uso en Streamlit

El archivo `pruebas_drive.py` incluye una interfaz completa con:

1. **Extracción con URLs normales**: Para uso interno
2. **Extracción con URLs públicas**: Para acceso público
3. **Opción para hacer archivos públicos**: Automáticamente durante la extracción
4. **Herramienta individual**: Para hacer público un archivo específico

## Consideraciones de Seguridad

⚠️ **Advertencias importantes:**

1. **Archivos públicos**: Una vez que un archivo es público, cualquiera con la URL puede acceder a él
2. **Permisos**: Los cambios de permisos son permanentes hasta que se revoquen manualmente
3. **Cuota**: Los archivos públicos pueden consumir más ancho de banda
4. **Privacidad**: Revisa cuidadosamente qué archivos hacer públicos

## Ejemplo de Uso

```python
from utils.get_sheets import extract_all_data_with_public_urls

# Extraer datos sin hacer archivos públicos
df = extract_all_data_with_public_urls(make_public=False)

# Extraer datos y hacer archivos públicos automáticamente
df = extract_all_data_with_public_urls(make_public=True)
```

## Solución al Problema de Autenticación

El problema original era que las URLs requerían autenticación. Las soluciones implementadas son:

1. **URLs públicas**: Generan URLs que no requieren autenticación (archivo debe ser público)
2. **Función make_public**: Hace automáticamente públicos los archivos
3. **Múltiples formatos**: Proporciona diferentes tipos de URLs para diferentes casos de uso

## Recomendaciones

1. **Para uso interno**: Usa URLs normales
2. **Para acceso público**: Usa URLs públicas con `make_public=True`
3. **Para archivos específicos**: Usa la herramienta individual de hacer público
4. **Siempre revisa**: Los permisos antes de hacer archivos públicos
