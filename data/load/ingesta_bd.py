import pandas as pd
import logging
from datetime import datetime
from data.transform.packing_transform import *
from utils.get_api import subir_archivo_con_reintento
from utils.handler_bd import *
#reporte_produccion_transform
logger = logging.getLogger(__name__)

def ingesta_reporte_produccion_bd(access_token):
    df = reporte_produccion_transform(access_token)

    try:
        from utils.handler_bd import create_reporte_produccion_table, insert_reporte_produccion_to_postgresql
        from utils.reporte_produccion_utils import clear_and_reload_reporte_produccion
        
        # Crear tabla si no existe
        create_reporte_produccion_table()
        
        # Verificar si la tabla está vacía
        from utils.handler_bd import create_database_connection
        connection = create_database_connection()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM reporte_produccion")
            count = cursor.fetchone()[0]
            cursor.close()
            connection.close()
            
            if count == 0:
                # Tabla vacía, usar inserción simple
                logger.info("📝 Tabla vacía detectada, usando inserción simple...")
                success = insert_reporte_produccion_to_postgresql(df)
            else:
                # Tabla con datos, usar recarga segura
                logger.info("🔄 Tabla con datos detectada, usando recarga segura...")
                success = clear_and_reload_reporte_produccion(df)
        else:
            # Si no se puede conectar, usar inserción simple por defecto
            logger.warning("⚠️ No se pudo verificar el estado de la tabla, usando inserción simple...")
            success = insert_reporte_produccion_to_postgresql(df)
        
        if success:
            logger.info(f"✅ Datos de reporte de producción procesados exitosamente: {len(df)} filas")
            return True
        else:
            logger.error(f"❌ Error al procesar datos de reporte de producción")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en ingesta de reporte de producción: {str(e)}")
        return False

def ingesta_evaluacion_calidad_pt_bd(access_token):
    """Ingesta de datos de evaluación de calidad de producto terminado"""
    from data.transform.packing_transform import evaluacion_calidad_pt_transform
    from utils.handler_bd import create_evaluacion_calidad_pt_table, insert_evaluacion_calidad_pt_to_postgresql
    from utils.evaluacion_calidad_utils import clear_and_reload_evaluacion_calidad_pt
    
    df = evaluacion_calidad_pt_transform(access_token)
    
    try:
        # Crear tabla si no existe
        create_evaluacion_calidad_pt_table()
        
        # Verificar si la tabla está vacía
        from utils.handler_bd import create_database_connection
        connection = create_database_connection()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM evaluacion_calidad_pt")
            count = cursor.fetchone()[0]
            cursor.close()
            connection.close()
            
            if count == 0:
                # Tabla vacía, usar inserción simple
                logger.info("📝 Tabla vacía detectada, usando inserción simple...")
                success = insert_evaluacion_calidad_pt_to_postgresql(df)
            else:
                # Tabla con datos, usar recarga segura
                logger.info("🔄 Tabla con datos detectada, usando recarga segura...")
                success = clear_and_reload_evaluacion_calidad_pt(df)
        else:
            # Si no se puede conectar, usar inserción simple por defecto
            logger.warning("⚠️ No se pudo verificar el estado de la tabla, usando inserción simple...")
            success = insert_evaluacion_calidad_pt_to_postgresql(df)
        
        if success:
            logger.info(f"✅ Datos de evaluación de calidad procesados exitosamente: {len(df)} filas")
            return True
        else:
            logger.error(f"❌ Error al procesar datos de evaluación de calidad")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en ingesta de evaluación de calidad: {str(e)}")
        return False
   
def ingesta_phl_pt_all_tabla_bd(access_token):
    """Ingesta de datos de PHL PT All Tabla"""
    from data.transform.packing_transform import phl_pt_all_tabla_transform
    from utils.handler_bd import create_phl_pt_all_tabla_table, insert_phl_pt_all_tabla_to_postgresql
    from utils.phl_pt_all_tabla_utils import clear_and_reload_phl_pt_all_tabla
    
    df = phl_pt_all_tabla_transform(access_token)
    
    try:
        # Crear tabla si no existe
        create_phl_pt_all_tabla_table()
        
        # Verificar si la tabla está vacía
        from utils.handler_bd import create_database_connection
        connection = create_database_connection()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM phl_pt_all_tabla")
            count = cursor.fetchone()[0]
            cursor.close()
            connection.close()
            
            if count == 0:
                # Tabla vacía, usar inserción simple
                logger.info("📝 Tabla vacía detectada, usando inserción simple...")
                success = insert_phl_pt_all_tabla_to_postgresql(df)
            else:
                # Tabla con datos, usar recarga segura
                logger.info("🔄 Tabla con datos detectada, usando recarga segura...")
                success = clear_and_reload_phl_pt_all_tabla(df)
        else:
            # Si no se puede conectar, usar inserción simple por defecto
            logger.warning("⚠️ No se pudo verificar el estado de la tabla, usando inserción simple...")
            success = insert_phl_pt_all_tabla_to_postgresql(df)
        
        if success:
            logger.info(f"✅ Datos de PHL PT All Tabla procesados exitosamente: {len(df)} filas")
            return True
        else:
            logger.error(f"❌ Error al procesar datos de PHL PT All Tabla")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en ingesta de PHL PT All Tabla: {str(e)}")
        return False

def ingesta_presentaciones_bd(access_token):
    """Ingesta de datos de presentaciones"""
    from data.transform.packing_transform import presentaciones_transform
    from utils.handler_bd import create_presentaciones_table, insert_presentaciones_to_postgresql
    from utils.presentaciones_utils import clear_and_reload_presentaciones
    
    df = presentaciones_transform(access_token)
    
    try:
        # Crear tabla si no existe
        create_presentaciones_table()
        
        # Verificar si la tabla está vacía
        from utils.handler_bd import create_database_connection
        connection = create_database_connection()
        if connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM presentaciones")
            count = cursor.fetchone()[0]
            cursor.close()
            connection.close()
            
            if count == 0:
                # Tabla vacía, usar inserción simple
                logger.info("📝 Tabla vacía detectada, usando inserción simple...")
                success = insert_presentaciones_to_postgresql(df)
            else:
                # Tabla con datos, usar recarga segura
                logger.info("🔄 Tabla con datos detectada, usando recarga segura...")
                success = clear_and_reload_presentaciones(df)
        else:
            # Si no se puede conectar, usar inserción simple por defecto
            logger.warning("⚠️ No se pudo verificar el estado de la tabla, usando inserción simple...")
            success = insert_presentaciones_to_postgresql(df)
        
        if success:
            logger.info(f"✅ Datos de presentaciones procesados exitosamente: {len(df)} filas")
            return True
        else:
            logger.error(f"❌ Error al procesar datos de presentaciones")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en ingesta de presentaciones: {str(e)}")
        return False
   




















































def ingesta_imagenes_eva_calidad_bd():
    df = images_fcl_drive_extract_transform()
    logger.info(f"📊 DataFrame extraído: {len(df)} filas, {len(df.columns)} columnas")
    try:
        logger.info(f"✅ Datos procesados: {len(df)} filas")
        cleaned_df = df[df["image_base64"].notna()]
        cleaned_df = cleaned_df.drop_duplicates()
        insert_dataframe_to_postgresql(cleaned_df)
        
    except:
        logger.error(f"❌ No se encontraron datos")
        return False
   