import logging
import pandas as pd
from utils.handler_bd import create_database_connection

logger = logging.getLogger(__name__)

def clear_and_reload_evaluacion_calidad_pt(df):
    """
    Vaciar y recargar la tabla evaluacion_calidad_pt de manera segura para evitar bloqueos.
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
        
        logger.info("🔄 Iniciando proceso de recarga segura de evaluacion_calidad_pt...")
        
        # Paso 1: Crear tabla temporal con los nuevos datos
        logger.info("📝 Paso 1: Creando tabla temporal...")
        temp_table_sql = """
        CREATE TEMP TABLE evaluacion_calidad_pt_temp (LIKE evaluacion_calidad_pt INCLUDING ALL);
        """
        cursor.execute(temp_table_sql)
        
        # Paso 2: Insertar datos en la tabla temporal
        logger.info("📊 Paso 2: Insertando datos en tabla temporal...")
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'FECHA DE MP': 'fecha_mp',
            'FECHA DE PROCESO': 'fecha_proceso',
            'SEMANA': 'semana',
            'EVALUADOR': 'evaluador',
            'PRODUCTOR': 'productor',
            'TIPO DE PRODUCTO': 'tipo_producto',
            'FUNDO': 'fundo',
            'HORA': 'hora',
            'LINEA': 'linea',
            'VIAJE': 'viaje',
            'MODULO': 'modulo',
            'TURNO': 'turno',
            'VARIEDAD': 'variedad',
            'PRESENTACION': 'presentacion',
            'DESTINO': 'destino',
            'TIPO DE CAJA': 'tipo_caja',
            'TRAZABILIDAD': 'trazabilidad',
            'PESO DE MUESTRA (g)': 'peso_muestra_g',
            'FRUTOS CON PEDICELO': 'frutos_con_pedicelo',
            'FUMAGINA': 'fumagina',
            'F.BLOOM': 'f_bloom',
            'HERIDA CICATRIZADA': 'herida_cicatrizada',
            'EXCRETA DE ABEJA': 'excreta_abeja',
            'RUSSET': 'russet',
            'POLVO': 'polvo',
            'FRUTOS ROJIZOS': 'frutos_rojizos',
            'RESTOS FLORALES': 'restos_florales',
            'HALO VERDE': 'halo_verde',
            'PICADO': 'picado',
            'BAJO CALIBRE': 'bajo_calibre',
            'CHANCHITO BLANCO': 'chanchito_blanco',
            'F. MOJADA': 'f_mojada',
            'DAÑO DE TRIPS': 'dano_trips',
            'OTROS': 'otros',
            'TOTAL DE DEFECTOS DE CALIDAD': 'total_defectos_calidad',
            'HERIDA ABIERTA': 'herida_abierta',
            'QUERESA': 'queresa',
            'DESHIDRATACIÓN  LEVE': 'deshidratacion_leve',
            'DESHIDRATACION MODERADO': 'deshidratacion_moderado',
            'DESHIDRATADO SEVERO': 'deshidratado_severo',
            'MACHUCON': 'machucon',
            'DESGARRO': 'desgarro',
            'SOBREMADURO': 'sobremaduro',
            'BLANDA SEVERA': 'blanda_severa',
            'BLANDA MODERADO': 'blanda_moderado',
            'EXCRETA DE AVE': 'excreta_ave',
            'HONGOS': 'hongos',
            'PUDRICION': 'pudricion',
            'BAYA REVENTADA': 'baya_reventada',
            'BAYA COLAPSADA': 'baya_colapsada',
            'PRESENCIA DE LARVA': 'presencia_larva',
            'EXUDACION': 'exudacion',
            'OTROS2': 'otros2',
            'TOTAL DE CONDICION': 'total_condicion',
            'TOTAL DE EXPORTABLE': 'total_exportable',
            'TOTAL DE NO EXPORTABLE': 'total_no_exportable',
            'N° FCL': 'n_fcl',
            'CALIBRE': 'calibre',
            'BRIX': 'brix',
            'ACIDEZ': 'acidez',
            'OBSERVACIONES': 'observaciones',
            'EMPRESA': 'empresa'
        }
        
        # Insertar en tabla temporal
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_temp_sql = f"""
        INSERT INTO evaluacion_calidad_pt_temp ({', '.join(columns_list)})
        VALUES ({placeholders})
        """
        
        # Procesar datos en lotes para la tabla temporal
        batch_size = 100
        total_records = len(df)
        processed_records = 0
        
        logger.info(f"Iniciando procesamiento de {total_records} registros...")
        
        for i in range(0, total_records, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_values = []
            
            for index, row in batch_df.iterrows():
                try:
                    values = []
                    for df_col, db_col in column_mapping.items():
                        if df_col in row:
                            value = row[df_col]
                            if pd.isna(value) or value == '' or value == '-':
                                values.append(None)
                            else:
                                # Convertir tipos según la columna
                                if 'fecha' in db_col:
                                    # Manejar fechas
                                    if isinstance(value, str):
                                        try:
                                            from datetime import datetime
                                            value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                                        except:
                                            try:
                                                value = datetime.strptime(value, '%Y-%m-%d')
                                            except:
                                                values.append(None)
                                                continue
                                    values.append(value)
                                elif 'semana' in db_col or 'viaje' in db_col or 'modulo' in db_col:
                                    # Manejar enteros
                                    try:
                                        values.append(int(float(value)) if value != 0 else 0)
                                    except:
                                        values.append(0)
                                elif 'linea' in db_col or 'peso_muestra_g' in db_col or 'brix' in db_col or 'acidez' in db_col:
                                    # Manejar números decimales específicos
                                    try:
                                        values.append(float(value) if value != 0 else 0.0)
                                    except:
                                        values.append(0.0)
                                elif any(x in db_col for x in ['frutos_con_pedicelo', 'fumagina', 'f_bloom', 'herida_cicatrizada', 
                                                              'excreta_abeja', 'russet', 'polvo', 'frutos_rojizos', 'restos_florales',
                                                              'halo_verde', 'picado', 'bajo_calibre', 'chanchito_blanco', 'f_mojada',
                                                              'dano_trips', 'otros', 'total_defectos_calidad', 'herida_abierta', 'queresa',
                                                              'deshidratacion_leve', 'deshidratacion_moderado', 'deshidratado_severo',
                                                              'machucon', 'desgarro', 'sobremaduro', 'blanda_severa', 'blanda_moderado',
                                                              'excreta_ave', 'hongos', 'pudricion', 'baya_reventada', 'baya_colapsada',
                                                              'presencia_larva', 'exudacion', 'otros2', 'total_condicion', 'total_exportable',
                                                              'total_no_exportable']):
                                    # Manejar porcentajes y medidas de calidad
                                    try:
                                        values.append(float(value) if value != 0 else 0.0)
                                    except:
                                        values.append(0.0)
                                else:
                                    # Manejar texto
                                    values.append(str(value)[:255] if len(str(value)) > 255 else str(value))
                        else:
                            values.append(None)
                    
                    batch_values.append(tuple(values))
                except Exception as e:
                    logger.warning(f"⚠️ Error en fila {index}: {str(e)}")
                    continue
            
            if batch_values:
                cursor.executemany(insert_temp_sql, batch_values)
                processed_records += len(batch_values)
            
            # Actualizar progreso
            progress = min((i + batch_size) / total_records, 1.0)
            logger.info(f"Procesando datos temporales: {processed_records}/{total_records}")
        
        logger.info("Procesamiento completado")
        
        # Verificar si se procesaron registros
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
        
        # Paso 3: Transacción atómica para reemplazar la tabla principal
        logger.info("🔄 Paso 3: Reemplazando tabla principal (operación atómica)...")
        
        # Usar transacción para minimizar el tiempo de bloqueo
        cursor.execute("BEGIN;")
        
        # Eliminar datos existentes
        cursor.execute("TRUNCATE TABLE evaluacion_calidad_pt RESTART IDENTITY;")
        
        # Copiar datos de la tabla temporal a la principal
        cursor.execute("""
            INSERT INTO evaluacion_calidad_pt 
            SELECT * FROM evaluacion_calidad_pt_temp;
        """)
        
        # Confirmar transacción
        cursor.execute("COMMIT;")
        
        # Paso 4: Limpiar tabla temporal
        cursor.execute("DROP TABLE evaluacion_calidad_pt_temp;")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"✅ Recarga completada exitosamente: {processed_records} registros procesados")
        return True
        
    except Exception as e:
        # Rollback en caso de error
        if connection:
            try:
                connection.rollback()
                cursor.execute("DROP TABLE IF EXISTS evaluacion_calidad_pt_temp;")
                connection.commit()
            except:
                pass
            connection.close()
        
        logger.error(f"❌ Error durante la recarga: {str(e)}")
        return False
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def clear_evaluacion_calidad_pt_table():
    """
    Vaciar completamente la tabla evaluacion_calidad_pt de manera segura
    """
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Contar registros antes de eliminar
        cursor.execute("SELECT COUNT(*) FROM evaluacion_calidad_pt;")
        count_before = cursor.fetchone()[0]
        
        if count_before == 0:
            st.info("ℹ️ La tabla evaluacion_calidad_pt ya está vacía")
            cursor.close()
            connection.close()
            return True
        
        # Mostrar confirmación
        st.warning(f"⚠️ Se van a eliminar {count_before} registros de evaluacion_calidad_pt")
        
        # Truncar tabla (más rápido que DELETE)
        cursor.execute("TRUNCATE TABLE evaluacion_calidad_pt RESTART IDENTITY;")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"✅ Se eliminaron {count_before} registros de evaluacion_calidad_pt")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error al vaciar la tabla: {e}")
        if connection:
            connection.close()
        return False
