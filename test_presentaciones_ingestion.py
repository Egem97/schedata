"""
Script de prueba para verificar la ingesta de presentaciones
"""

import logging
import pandas as pd
from data.load.ingesta_bd import ingesta_presentaciones_bd
from data.transform.packing_transform import presentaciones_transform
from utils.get_token import get_access_token_packing

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_presentaciones_ingestion():
    """Probar la ingesta de presentaciones"""
    
    logger.info("🚀 Iniciando prueba de ingesta de presentaciones...")
    
    # Obtener token de acceso
    logger.info("🔑 Obteniendo token de acceso...")
    access_token = get_access_token_packing()
    
    if not access_token:
        logger.error("❌ No se pudo obtener el token de acceso")
        return False
    
    logger.info("✅ Token de acceso obtenido correctamente")
    
    # Probar la extracción de datos primero
    logger.info("📊 Probando extracción de datos...")
    try:
        df = presentaciones_transform(access_token)
        logger.info(f"✅ Datos extraídos: {len(df)} filas, {len(df.columns)} columnas")
        logger.info(f"📋 Columnas: {list(df.columns)}")
        
        if not df.empty:
            logger.info("📝 Primeras 3 filas:")
            logger.info(df.head(3).to_string())
        else:
            logger.warning("⚠️ DataFrame vacío - esto podría ser normal si no hay datos")
            
    except Exception as e:
        logger.error(f"❌ Error en extracción de datos: {str(e)}")
        return False
    
    # Probar la ingesta completa
    logger.info("🔄 Probando ingesta completa...")
    try:
        success = ingesta_presentaciones_bd(access_token)
        
        if success:
            logger.info("✅ Ingesta completada exitosamente")
            return True
        else:
            logger.error("❌ Error en la ingesta")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en ingesta: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_presentaciones_ingestion()
    
    if success:
        print("\n🎉 ¡Prueba completada exitosamente!")
    else:
        print("\n💥 Prueba falló")
