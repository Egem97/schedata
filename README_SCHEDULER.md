# 🚀 Sistema Automatizado con Schedule

Este sistema automatiza el procesamiento de datos usando la librería `schedule` de Python y configuración desde `config.yaml`.

## 📋 Características

- ✅ **Automatización programable**: Ejecuta el proceso según horarios configurables
- ✅ **Configuración centralizada**: Toda la configuración en `config.yaml`
- ✅ **Logging completo**: Registro detallado de todas las operaciones
- ✅ **Manejo de errores**: Reintentos automáticos y manejo robusto de errores
- ✅ **Formato Excel profesional**: Archivos con formato visual automático

## 🚀 Uso

### Ejecutar el sistema:
```bash
python scheduler.py
```

### El sistema te preguntará:
```
¿Deseas ejecutar el proceso una vez al inicio? (s/n):
```

### Una vez iniciado, verás:
```
📅 SISTEMA AUTOMATIZADO ACTIVO
============================================================
⏰ Próxima ejecución programada según config.yaml
🛑 Presiona Ctrl+C para detener el sistema
============================================================
```

## ⚙️ Configuración en `config.yaml`

### Scheduler - Opciones de automatización:

```yaml
scheduler:
  interval: "minutes"  # Opciones: daily, hourly, minutes
  time: "09:00"        # Solo para daily (formato HH:MM)
  minutes: 15          # Solo para minutes
```

#### Opciones de `interval`:

1. **`daily`** - Ejecuta una vez al día a la hora especificada
   ```yaml
   scheduler:
     interval: "daily"
     time: "09:00"  # Se ejecuta a las 9:00 AM
   ```

2. **`hourly`** - Ejecuta cada hora
   ```yaml
   scheduler:
     interval: "hourly"
   ```

3. **`minutes`** - Ejecuta cada X minutos
   ```yaml
   scheduler:
     interval: "minutes"
     minutes: 30  # Cada 30 minutos
   ```

### Configuración de archivos:

```yaml
archivos:
  volcado: "BD VOLCADO DE MATERIA PRIMA.xlsx"
  enfriamiento: "ENFRIAMIENTO 2025.xlsx"
  salida: "datos_procesados.xlsx"
```

### Configuración de carpetas OneDrive:

```yaml
onedrive:
  drive_id: "tu_drive_id"
  carpetas:
    volcado: "01XOBWFSDLRDZDRGI5RBEI4IZMWN5CC2NS"
    enfriamiento: "01XOBWFSGMVNZHJBTVUVA2T4UDY3TLDBTH"
    salida: "01XOBWFSF6Y2GOVW7725BZO354PWSELRRZ"
```

### Configuración de Google Sheets:

```yaml
google_sheets:
  spreadsheet_id: "1PWz0McxGvGGD5LzVFXsJTaNIAEYjfWohqtimNVCvTGQ"
  sheet_name: "KF"
```

### Configuración de timestamp:

```yaml
onedrive:
  usar_timestamp: true  # Agrega timestamp a nombres de archivo
```

Si `usar_timestamp: true`, los archivos se guardarán como:
- `datos_procesados_20250120_143022.xlsx`

Si `usar_timestamp: false`, se sobrescribirá:
- `datos_procesados.xlsx`

## 📊 Logging

El sistema genera logs en:
- **Archivo**: `scheduler.log`
- **Consola**: Salida en tiempo real

### Configuración de logging:

```yaml
logging:
  level: "INFO"        # DEBUG, INFO, WARNING, ERROR, CRITICAL
  rotation: "10 MB"    # Tamaño antes de rotar
  retention: "30 days" # Tiempo de retención
```

## 🔄 Flujo del proceso

1. 🔑 **Autenticación**: Obtiene token de Microsoft Graph
2. 📊 **Google Sheets**: Lee datos de la hoja especificada
3. 📁 **OneDrive - Volcado**: Descarga archivo de volcado
4. 📁 **OneDrive - Enfriamiento**: Descarga archivo de enfriamiento
5. 🔄 **Procesamiento**: Ejecuta transformaciones de datos
6. 📤 **Subida**: Sube archivo procesado con formato Excel
7. ⏱️ **Espera**: Espera hasta la próxima ejecución programada

## 🛠️ Personalización

### Cambiar archivos de origen:
Modifica en `config.yaml`:
```yaml
archivos:
  volcado: "MI_ARCHIVO_VOLCADO.xlsx"
  enfriamiento: "MI_ARCHIVO_ENFRIAMIENTO.xlsx"
  salida: "MI_RESULTADO.xlsx"
```

### Cambiar horarios:
```yaml
scheduler:
  interval: "daily"
  time: "14:30"  # 2:30 PM
```

### Cambiar frecuencia:
```yaml
scheduler:
  interval: "minutes"
  minutes: 5  # Cada 5 minutos
```

## 🚨 Manejo de errores

El sistema incluye:
- **Reintentos automáticos** para subida de archivos
- **Validación de archivos** antes de procesarlos
- **Logs detallados** para debugging
- **Continuidad** - Si falla una ejecución, continúa con la siguiente

## 📝 Ejemplos de uso

### 1. Ejecutar cada 30 minutos:
```yaml
scheduler:
  interval: "minutes"
  minutes: 30
```

### 2. Ejecutar diariamente a las 8:00 AM:
```yaml
scheduler:
  interval: "daily"
  time: "08:00"
```

### 3. Ejecutar cada hora:
```yaml
scheduler:
  interval: "hourly"
```

## 🔧 Solución de problemas

### Si el sistema no encuentra archivos:
1. Verifica que los nombres en `config.yaml` coincidan exactamente
2. Revisa que las carpetas OneDrive sean correctas
3. Verifica permisos del token de Microsoft Graph

### Si falla la autenticación:
1. Revisa las credenciales en `config.yaml`
2. Verifica que el token no haya expirado
3. Confirma permisos de la aplicación Azure

### Para detener el sistema:
- Presiona `Ctrl+C` para parar de forma segura
- El sistema registrará la parada en el log 