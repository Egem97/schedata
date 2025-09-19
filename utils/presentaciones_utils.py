import logging
import pandas as pd
from utils.handler_bd import create_database_connection

logger = logging.getLogger(__name__)

def clear_and_reload_presentaciones(df):
    """
    Vaciar y recargar la tabla presentaciones de manera segura para evitar bloqueos.
    Usa una estrategia de tabla temporal para minimizar el tiempo de bloqueo.
    """
    if df.empty:
        logger.warning("⚠️ No hay datos para insertar")
        return False
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        connection.autocommit = False
        cursor = connection.cursor()
        
        logger.info("🔄 Iniciando proceso de recarga segura de presentaciones...")
        
        # Paso 1: Crear tabla temporal con los nuevos datos
        logger.info("📝 Paso 1: Creando tabla temporal...")
        temp_table_sql = """
        CREATE TEMP TABLE presentaciones_temp (LIKE presentaciones INCLUDING ALL);
        """
        cursor.execute(temp_table_sql)
        
        # Paso 2: Insertar datos en la tabla temporal
        logger.info("📊 Paso 2: Insertando datos en tabla temporal...")
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'DESCRIPCION DE PRODUCTO': 'descripcion_producto',
            'PESO caja': 'peso_caja',
            'SOBRE PESO': 'sobre_peso',
            'ESQUINEROS ADIONALES': 'esquinero_adicionales'
        }
        
        # Crear la consulta INSERT para la tabla temporal
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO presentaciones_temp ({', '.join(columns_list)})
        VALUES ({placeholders})
        """
        
        # Contar registros a insertar
        total_records = len(df)
        processed_records = 0
        error_records = 0
        
        # Progreso
        logger.info(f"Iniciando procesamiento de {total_records} registros...")
        
        # Lista para almacenar errores detallados
        detailed_errors = []
        
        # Procesar en lotes para mejor rendimiento
        batch_size = 50
        
        for i in range(0, total_records, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_values = []
            batch_errors = []
            
            # Preparar valores del lote
            for index, row in batch_df.iterrows():
                try:
                    values = []
                    
                    # Mapear cada columna del DataFrame a la columna de la base de datos
                    for df_col, db_col in column_mapping.items():
                        if df_col in row:
                            value = row[df_col]
                            
                            # Manejar valores nulos
                            if pd.isna(value) or value == '' or value == '-':
                                values.append(None)
                            else:
                                # Convertir tipos según la columna
                                if 'descripcion_producto' in db_col:
                                    # Manejar texto
                                    values.append(str(value)[:255] if len(str(value)) > 255 else str(value))
                                elif 'peso_caja' in db_col or 'sobre_peso' in db_col:
                                    # Manejar números decimales
                                    try:
                                        values.append(float(value) if value != 0 else 0.0)
                                    except:
                                        values.append(0.0)
                                elif 'esquinero_adicionales' in db_col:
                                    # Manejar enteros
                                    try:
                                        values.append(int(float(value)) if value != 0 else 0)
                                    except:
                                        values.append(0)
                                else:
                                    # Manejar texto por defecto
                                    values.append(str(value)[:255] if len(str(value)) > 255 else str(value))
                        else:
                            values.append(None)
                    
                    batch_values.append(tuple(values))
                    
                except Exception as e:
                    error_msg = f"Error preparando datos para fila {index}: {str(e)}"
                    batch_errors.append((index, error_msg))
                    detailed_errors.append(error_msg)
            
            # Insertar lote si hay valores válidos
            if batch_values:
                try:
                    cursor.executemany(insert_sql, batch_values)
                    processed_records += len(batch_values)
                    
                except Exception as e:
                    error_msg = f"Error insertando lote {i//batch_size + 1}: {str(e)}"
                    detailed_errors.append(error_msg)
                    
                    # Intentar insertar registros uno por uno para identificar el problema
                    for j, values in enumerate(batch_values):
                        try:
                            cursor.execute(insert_sql, values)
                            processed_records += 1
                        except Exception as individual_error:
                            error_records += 1
                            error_msg = f"Error en registro individual {i+j}: {str(individual_error)[:100]}"
                            detailed_errors.append(error_msg)
            
            # Contar errores del lote
            error_records += len(batch_errors)
            
            # Mostrar errores del lote
            if batch_errors:
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        if processed_records == 0:
            logger.warning("⚠️ No se pudieron insertar registros en la tabla temporal")
            # Si no hay registros para insertar, no es necesariamente un error
            # Podría ser que el DataFrame esté vacío
            if len(df) == 0:
                logger.info("ℹ️ El DataFrame está vacío, no hay datos para insertar")
                connection.rollback()
                return True  # Retornar True porque no es un error
            else:
                logger.error("❌ Error: DataFrame tiene datos pero no se pudieron insertar")
                connection.rollback()
                return False
        
        # Paso 3: Vaciar tabla principal
        logger.info("🗑️ Paso 3: Vaciando tabla principal...")
        cursor.execute("DELETE FROM presentaciones")
        
        # Paso 4: Copiar datos de temporal a principal
        logger.info("📋 Paso 4: Copiando datos de temporal a principal...")
        copy_sql = """
        INSERT INTO presentaciones 
        SELECT * FROM presentaciones_temp
        """
        cursor.execute(copy_sql)
        
        # Paso 5: Confirmar transacción
        connection.commit()
        
        # Limpiar tabla temporal
        cursor.execute("DROP TABLE presentaciones_temp")
        
        logger.info(f"✅ Recarga completada exitosamente: {processed_records} registros procesados")
        
        # Mostrar errores si los hay
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return True
        
    except Exception as e:
        # Rollback en caso de error
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico en recarga: {str(e)}")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def clear_presentaciones_table():
    """
    Limpiar todos los datos de la tabla presentaciones
    """
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Contar registros antes de eliminar
        cursor.execute("SELECT COUNT(*) FROM presentaciones")
        count_before = cursor.fetchone()[0]
        
        if count_before == 0:
            logger.info("ℹ️ La tabla presentaciones ya está vacía")
            cursor.close()
            connection.close()
            return True
        
        # Mostrar confirmación
        logger.warning(f"⚠️ Se van a eliminar {count_before} registros de la tabla presentaciones")
        
        # Eliminar todos los registros
        cursor.execute("DELETE FROM presentaciones")
        deleted_rows = cursor.rowcount
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"✅ Se eliminaron {deleted_rows} registros de la tabla presentaciones")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error al limpiar la tabla presentaciones: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def get_presentaciones_stats():
    """
    Obtener estadísticas de la tabla presentaciones para monitoreo de desempeño
    """
    connection = create_database_connection()
    if not connection:
        return None
    
    try:
        cursor = connection.cursor()
        
        # Estadísticas generales
        stats_sql = """
        SELECT 
            COUNT(*) as total_presentaciones,
            COUNT(DISTINCT descripcion_producto) as unique_productos,
            AVG(peso_caja) as peso_promedio_caja,
            AVG(sobre_peso) as sobre_peso_promedio,
            SUM(esquinero_adicionales) as total_esquineros,
            MIN(created_at) as fecha_creacion_min,
            MAX(updated_at) as fecha_actualizacion_max
        FROM presentaciones
        """
        
        cursor.execute(stats_sql)
        stats = cursor.fetchone()
        
        # Top productos por peso
        top_peso_sql = """
        SELECT 
            descripcion_producto,
            peso_caja,
            sobre_peso,
            esquinero_adicionales
        FROM presentaciones
        ORDER BY peso_caja DESC
        LIMIT 10
        """
        
        cursor.execute(top_peso_sql)
        top_peso = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return {
            'general': stats,
            'top_peso': top_peso
        }
        
    except Exception as e:
        logger.error(f"❌ Error al obtener estadísticas: {e}")
        if connection:
            connection.close()
        return None
