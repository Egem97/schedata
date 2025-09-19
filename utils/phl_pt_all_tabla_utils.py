import logging
import pandas as pd
from utils.handler_bd import create_database_connection

logger = logging.getLogger(__name__)

def clear_and_reload_phl_pt_all_tabla(df):
    """
    Vaciar y recargar la tabla phl_pt_all_tabla de manera segura para evitar bloqueos.
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
        
        logger.info("🔄 Iniciando proceso de recarga segura de phl_pt_all_tabla...")
        
        # Paso 1: Crear tabla temporal con los nuevos datos
        logger.info("📝 Paso 1: Creando tabla temporal...")
        temp_table_sql = """
        CREATE TEMP TABLE phl_pt_all_tabla_temp (LIKE phl_pt_all_tabla INCLUDING ALL);
        """
        cursor.execute(temp_table_sql)
        
        # Paso 2: Insertar datos en la tabla temporal
        logger.info("📊 Paso 2: Insertando datos en tabla temporal...")
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'ENVIO': 'envio',
            'SEMANA': 'semana',
            'F. PRODUCCION': 'fecha_produccion',
            'F. COSECHA': 'fecha_cosecha',
            'CLIENTE': 'cliente',
            'TIPO DE PALLET': 'tipo_pallet',
            'CONTENEDOR': 'contenedor',
            'DESCRIPCION DEL PRODUCTO': 'descripcion_producto',
            'DESTINO': 'destino',
            'FUNDO': 'fundo',
            'VARIEDAD': 'variedad',
            'Nº CAJAS': 'n_cajas',
            'Nº DE PALLET': 'n_pallet',
            'TURNO': 'turno',
            'LINEA': 'linea',
            'PHL ORIGEN': 'phl_origen',
            'MATERIALES ADICIONALES': 'materiales_adicionales',
            'OBSERVACIONES': 'observaciones',
            'SOBRE PESO': 'sobre_peso',
            'PESO DE CAJA': 'peso_caja',
            'EXPORTABLE': 'exportable',
            'ESTADO': 'estado'
        }
        
        # Crear la consulta INSERT para la tabla temporal
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO phl_pt_all_tabla_temp ({', '.join(columns_list)})
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
        batch_size = 100
        
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
                                if 'fecha' in db_col:
                                    # Manejar fechas
                                    if isinstance(value, str):
                                        try:
                                            from datetime import datetime
                                            if '/' in value:
                                                value = datetime.strptime(value, '%Y-%m-%d')
                                            else:
                                                value = datetime.strptime(value, '%Y-%m-%d')
                                        except:
                                            values.append(None)
                                            continue
                                    values.append(value)
                                elif 'n_cajas' in db_col or 'peso_caja' in db_col or 'exportable' in db_col or 'semana' in db_col or 'turno' in db_col or 'linea' in db_col:
                                    # Manejar números decimales
                                    try:
                                        values.append(float(value) if value != 0 else 0.0)
                                    except:
                                        values.append(0.0)
                                elif 'sobre_peso' in db_col:
                                    # Manejar enteros
                                    try:
                                        values.append(int(float(value)) if value != 0 else 0)
                                    except:
                                        values.append(0)
                                else:
                                    # Manejar texto
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
            logger.error("❌ No se pudieron insertar registros en la tabla temporal")
            connection.rollback()
            return False
        
        # Paso 3: Vaciar tabla principal
        logger.info("🗑️ Paso 3: Vaciando tabla principal...")
        cursor.execute("DELETE FROM phl_pt_all_tabla")
        
        # Paso 4: Copiar datos de temporal a principal
        logger.info("📋 Paso 4: Copiando datos de temporal a principal...")
        copy_sql = """
        INSERT INTO phl_pt_all_tabla 
        SELECT * FROM phl_pt_all_tabla_temp
        """
        cursor.execute(copy_sql)
        
        # Paso 5: Confirmar transacción
        connection.commit()
        
        # Limpiar tabla temporal
        cursor.execute("DROP TABLE phl_pt_all_tabla_temp")
        
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

def clear_phl_pt_all_tabla_table():
    """
    Limpiar todos los datos de la tabla phl_pt_all_tabla
    """
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Contar registros antes de eliminar
        cursor.execute("SELECT COUNT(*) FROM phl_pt_all_tabla")
        count_before = cursor.fetchone()[0]
        
        if count_before == 0:
            logger.info("ℹ️ La tabla phl_pt_all_tabla ya está vacía")
            cursor.close()
            connection.close()
            return True
        
        # Mostrar confirmación
        logger.warning(f"⚠️ Se van a eliminar {count_before} registros de la tabla phl_pt_all_tabla")
        
        # Eliminar todos los registros
        cursor.execute("DELETE FROM phl_pt_all_tabla")
        deleted_rows = cursor.rowcount
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"✅ Se eliminaron {deleted_rows} registros de la tabla phl_pt_all_tabla")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error al limpiar la tabla phl_pt_all_tabla: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False
