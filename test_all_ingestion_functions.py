"""
Script de prueba para verificar todas las funciones de ingesta
"""

import logging
from data.load.ingesta_bd import (
    ingesta_reporte_produccion_bd,
    ingesta_evaluacion_calidad_pt_bd,
    ingesta_phl_pt_all_tabla_bd,
    ingesta_presentaciones_bd
)
from utils.get_token import get_access_token_packing

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_all_ingestion_functions():
    """Probar todas las funciones de ingesta"""
    
    logger.info("🚀 Iniciando prueba de todas las funciones de ingesta...")
    
    # Obtener token de acceso
    logger.info("🔑 Obteniendo token de acceso...")
    access_token = get_access_token_packing()
    
    if not access_token:
        logger.error("❌ No se pudo obtener el token de acceso")
        return False
    
    logger.info("✅ Token de acceso obtenido correctamente")
    
    # Lista de funciones a probar
    functions_to_test = [
        ("Reporte de Producción", ingesta_reporte_produccion_bd),
        ("Evaluación de Calidad PT", ingesta_evaluacion_calidad_pt_bd),
        ("PHL PT All Tabla", ingesta_phl_pt_all_tabla_bd),
        ("Presentaciones", ingesta_presentaciones_bd)
    ]
    
    results = {}
    
    for name, function in functions_to_test:
        logger.info(f"\n{'='*50}")
        logger.info(f"🧪 Probando: {name}")
        logger.info(f"{'='*50}")
        
        try:
            success = function(access_token)
            results[name] = success
            
            if success:
                logger.info(f"✅ {name}: ÉXITO")
            else:
                logger.error(f"❌ {name}: FALLÓ")
                
        except Exception as e:
            logger.error(f"💥 {name}: ERROR - {str(e)}")
            results[name] = False
    
    # Resumen de resultados
    logger.info(f"\n{'='*50}")
    logger.info("📊 RESUMEN DE RESULTADOS")
    logger.info(f"{'='*50}")
    
    success_count = 0
    total_count = len(results)
    
    for name, success in results.items():
        status = "✅ ÉXITO" if success else "❌ FALLÓ"
        logger.info(f"{name}: {status}")
        if success:
            success_count += 1
    
    logger.info(f"\n📈 Resultado final: {success_count}/{total_count} funciones exitosas")
    
    if success_count == total_count:
        logger.info("🎉 ¡Todas las funciones funcionan correctamente!")
        return True
    else:
        logger.warning(f"⚠️ {total_count - success_count} funciones fallaron")
        return False

def test_individual_function(function_name, function):
    """Probar una función individual"""
    
    logger.info(f"🧪 Probando función individual: {function_name}")
    
    access_token = get_access_token_packing()
    if not access_token:
        logger.error("❌ No se pudo obtener el token de acceso")
        return False
    
    try:
        success = function(access_token)
        if success:
            logger.info(f"✅ {function_name}: ÉXITO")
        else:
            logger.error(f"❌ {function_name}: FALLÓ")
        return success
    except Exception as e:
        logger.error(f"💥 {function_name}: ERROR - {str(e)}")
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Probar función específica
        function_name = sys.argv[1].lower()
        
        if function_name == "reporte":
            test_individual_function("Reporte de Producción", ingesta_reporte_produccion_bd)
        elif function_name == "evaluacion":
            test_individual_function("Evaluación de Calidad PT", ingesta_evaluacion_calidad_pt_bd)
        elif function_name == "phl":
            test_individual_function("PHL PT All Tabla", ingesta_phl_pt_all_tabla_bd)
        elif function_name == "presentaciones":
            test_individual_function("Presentaciones", ingesta_presentaciones_bd)
        else:
            print("Funciones disponibles: reporte, evaluacion, phl, presentaciones")
    else:
        # Probar todas las funciones
        success = test_all_ingestion_functions()
        
        if success:
            print("\n🎉 ¡Todas las pruebas completadas exitosamente!")
        else:
            print("\n💥 Algunas pruebas fallaron")


