# 🚀 Configuración Inicial del Proyecto

## 📋 Pasos para configurar el proyecto

### 1. **Copiar archivo de configuración**
```bash
# Copia el archivo de ejemplo
cp config.yaml.example config.yaml
```

### 2. **Editar credenciales**
Abre `config.yaml` y completa con tus credenciales reales:

```yaml
microsoft_graph:
  tenant_id: "tu_tenant_id_real"
  client_id: "tu_client_id_real"
  client_secret: "tu_client_secret_real"

onedrive:
  drive_id: "tu_drive_id_real"
  
google_sheets:
  spreadsheet_id: "tu_spreadsheet_id_real"
```

### 3. **Instalar dependencias**
```bash
pip install -r requirements.txt
```

### 4. **Ejecutar el scheduler**
```bash
python scheduler.py
```

## 🔒 Seguridad

- ✅ **config.yaml** está en `.gitignore` - no se subirá al repositorio
- ✅ ***.json** están ignorados - no se subirán datos temporales
- ✅ **logs** están ignorados - no se subirán archivos de log
- ✅ **archivos temporales** están ignorados

## 📁 Estructura de archivos

```
proyecto/
├── config.yaml.example    # Plantilla de configuración
├── config.yaml           # Tu configuración (ignorado por git)
├── scheduler.py          # Script principal
├── .gitignore           # Archivos a ignorar
├── utils/               # Funciones auxiliares
└── logs/               # Archivos de log (ignorado por git)
```

## ⚠️ Importante

- **NUNCA** subas `config.yaml` al repositorio
- **SIEMPRE** usa `config.yaml.example` como plantilla
- **SIEMPRE** revisa que las credenciales no estén en el código 