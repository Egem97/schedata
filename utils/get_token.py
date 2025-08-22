import requests
from typing import Optional
from utils.config import load_config



# Cargar configuración al inicializar el módulo
config = load_config()

def get_access_token() -> Optional[str]:
    """
    Obtiene el token de acceso para Microsoft Graph API
    """
    if not config:
        print("Error: No se pudo cargar la configuración")
        return None
    
    AUTHORITY = f"https://login.microsoftonline.com/{config['microsoft_graph']['tenant_id']}/oauth2/v2.0/token"
    try:
        response = requests.post(AUTHORITY, data={
            "grant_type": "client_credentials",
            "client_id": config['microsoft_graph']['client_id'],
            "client_secret": config['microsoft_graph']['client_secret'],
            "scope": "https://graph.microsoft.com/.default"
        })
        
        if response.status_code == 200:
            token_response = response.json()
            access_token = token_response.get("access_token")
            
            if access_token:
                print("Token de acceso obtenido exitosamente")
                return access_token
            else:
                print("Error: No se pudo obtener el token de acceso")
                return None
        else:
            print(f"Error HTTP {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"Error al obtener el token: {e}")
        return None

def get_config_value(section: str, key: str = None):
    """
    Obtiene un valor específico de la configuración
    
    Args:
        section: Sección del config (ej: 'microsoft_graph', 'onedrive', etc.)
        key: Clave específica dentro de la sección (opcional)
    
    Returns:
        El valor solicitado o None si no existe
    """
    if not config:
        print("Error: No se pudo cargar la configuración")
        return None
    
    if section not in config:
        print(f"Error: La sección '{section}' no existe en la configuración")
        return None
    
    if key is None:
        return config[section]
    
    if key not in config[section]:
        print(f"Error: La clave '{key}' no existe en la sección '{section}'")
        return None
    
    return config[section][key]

def print_config():
    """
    Imprime toda la configuración de manera organizada
    """
    if not config:
        print("Error: No se pudo cargar la configuración")
        return
    
    print("=== CONFIGURACIÓN CARGADA ===")
    for section, values in config.items():
        print(f"\n[{section.upper()}]")
        if isinstance(values, dict):
            for key, value in values.items():
                # Ocultar valores sensibles
                if 'secret' in key.lower() or 'token' in key.lower():
                    print(f"  {key}: ****")
                else:
                    print(f"  {key}: {value}")
        else:
            print(f"  {values}")

def get_access_token_packing() -> Optional[str]:
    """
    Obtiene el token de acceso para Microsoft Graph API
    """
    if not config:
        print("Error: No se pudo cargar la configuración")
        return None
    
    AUTHORITY = f"https://login.microsoftonline.com/{config['microsoft_graph_packing']['tenant_id']}/oauth2/v2.0/token"
    try:
        response = requests.post(AUTHORITY, data={
            "grant_type": "client_credentials",
            "client_id": config['microsoft_graph_packing']['client_id'],
            "client_secret": config['microsoft_graph_packing']['client_secret'],
            "scope": "https://graph.microsoft.com/.default"
        })
        
        if response.status_code == 200:
            token_response = response.json()
            access_token = token_response.get("access_token")
            
            if access_token:
                print("Token de acceso obtenido exitosamente")
                return access_token
            else:
                print("Error: No se pudo obtener el token de acceso")
                return None
        else:
            print(f"Error HTTP {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"Error al obtener el token: {e}")
        return None