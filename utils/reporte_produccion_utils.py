import logging
import pandas as pd
from utils.handler_bd import create_database_connection

logger = logging.getLogger(__name__)

def clear_and_reload_reporte_produccion(df):
    """
    Vaciar y recargar la tabla reporte_produccion de manera segura para evitar bloqueos.
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
        
        logger.info("🔄 Iniciando proceso de recarga segura de reporte_produccion...")
        
        # Paso 1: Crear tabla temporal con los nuevos datos
        logger.info("📝 Paso 1: Creando tabla temporal...")
        temp_table_sql = """
        CREATE TEMP TABLE reporte_produccion_temp (LIKE reporte_produccion INCLUDING ALL);
        """
        cursor.execute(temp_table_sql)
        
        # Paso 2: Insertar datos en la tabla temporal
        logger.info("📊 Paso 2: Insertando datos en tabla temporal...")
        
        # Usar la misma lógica de inserción pero en la tabla temporal
        column_mapping = {
            'Semana': 'semana',
            'Fecha de cosecha': 'fecha_cosecha',
            'Fecha de proceso': 'fecha_proceso',
            'Turno Proceso': 'turno_proceso',
            'Empresa': 'empresa',
            'Tipo': 'tipo',
            'Fundo': 'fundo',
            'Variedad': 'variedad',
            'Kg Procesados': 'kg_procesados',
            'Kg Descarte': 'kg_descarte',
            '% Descarte': 'pct_descarte',
            'Kg Sobre Peso ': 'kg_sobre_peso',
            '% Sobre Peso': 'pct_sobre_peso',
            'Kg Merma': 'kg_merma',
            '% Merma': 'pct_merma',
            '% Rendimiento MP': 'pct_rendimiento_mp',
            '4.4OZ C/E SAN LUCAR CHINA': 'caja_44oz_san_lucar_china',
            '125 GRS C/E BERRY WORLD CHINA': 'caja_125grs_berry_world_china',
            '4.4OZ C/E BERRY FRESH CHINA': 'caja_44oz_berry_fresh_china',
            'CAJA 12x125 gr 1.5 KG (4.4 OZ)': 'caja_12x125gr_15kg_44oz',
            ' 1.5 KG (4.4 OZ AEREO)': 'caja_15kg_44oz_aereo',
            ' 1.5 KG (4.4 OZ AMORIA)': 'caja_15kg_44oz_amoria',
            '12 x 7 OZ PRIVATE SELECTION-M': 'caja_12x7oz_private_selection',
            '3KG (9.8OZ 5X7 )  CHINA': 'caja_3kg_98oz_5x7_china',
            '9,8OZ GENÉRICO 3.34 KG ': 'caja_98oz_generico_334kg',
            ' 3.80 KG (12X1PT GNCO)': 'caja_380kg_12x1pt_gnco',
            '3.74KG (9.8 PT   SAN LUCAR MARITIMO)': 'caja_374kg_98pt_san_lucar_maritimo',
            ' 3.324KG (9.8OZ)(12X1PT GEN.  AEREO)': 'caja_3324kg_98oz_12x1pt_gen_aereo',
            ' 3.324KG (9.8OZ)(12X1PT SAN LUCAR AEREO)': 'caja_3324kg_98oz_12x1pt_san_lucar_aereo',
            ' 3.80 KG (12X1PT BERRY FRESH)': 'caja_380kg_12x1pt_berry_fresh',
            ' 4.08 KG (8X18OZ SAN LUCAR)': 'caja_408kg_8x18oz_san_lucar',
            ' 4.08 KG (8X18OZ GNCO)2': 'caja_408kg_8x18oz_gnco2',
            ' 4.08 KG (8X18OZ BF)': 'caja_408kg_8x18oz_bf',
            '8x18 oz C/E  BOO BERRIES': 'caja_8x18oz_ce_boo_berries',
            ' 6.12 KG (12X18OZ BF)': 'caja_612kg_12x18oz_bf',
            ' 6.12 KG (12X18OZ GEN) ': 'caja_612kg_12x18oz_gen',
            ' 6.12 KG (12X18OZ PREMIUM)': 'caja_612kg_12x18oz_premium',
            '2.04 Kg (6Oz)GENERICO ORG.': 'caja_204kg_6oz_generico_org',
            '2.04 KG (6OZ) MARITIMO': 'caja_204kg_6oz_maritimo',
            '2.04 KG (6OZ) SAN LUCAR': 'caja_204kg_6oz_san_lucar',
            ' 2.04 KG (6 OZ) B/F': 'caja_204kg_6oz_bf',
            '3.0 KG (GRANEL SAL LUCAR)': 'caja_30kg_granel_sal_lucar',
            '3.0 KG (GRANEL AÉREO)': 'caja_30kg_granel_aereo',
            ' 3.40 KG (CANASTILLA) AEREO': 'caja_340kg_canastilla_aereo',
            ' 3.40 KG (CANASTILLA)': 'caja_340kg_canastilla',
            '3.0 KG (GRANEL )': 'caja_30kg_granel',
            '3.3KG CANASTILLA SL-M': 'caja_33kg_canastilla_sl_m',
            'N° Bandejas Nacional': 'n_bandejas_nacional',
            'Formato': 'formato',
            'Kg Nacionales': 'kg_nacionales',
            'Kg Procesados2': 'kg_procesados2',
            'Kg Exportables': 'kg_exportables',
            'Kg Fruta Jumbo': 'kg_fruta_jumbo',
            '% Fruta Jumbo': 'pct_fruta_jumbo',
            '% Fruta Convencional': 'pct_fruta_convencional',
            '%. Kg Exportables': 'pct_kg_exportables',
            '%. Kg Nacionales': 'pct_kg_nacionales',
            'Descarte por polvo (KG)': 'descarte_por_polvo_kg',
            'Descarte campo (KG)': 'descarte_campo_kg',
            'Muestra 1.5 KG (4.4 OZ)': 'muestra_15kg_44oz',
            'Muestra 12x7 OZ': 'muestra_12x7oz',
            'Muestra 8*18 BERRY FRESH': 'muestra_8x18_berry_fresh',
            'Muestra 4.08 KG (8X18 OZ) GEN.': 'muestra_408kg_8x18oz_gen',
            ' MUESTRA 9.8 OZ MARITIMO': 'muestra_98oz_maritimo',
            '9.8oz AEREO': 'muestra_98oz_aereo',
            'Muestra 6.1 KG (4.4 OZ)': 'muestra_61kg_44oz',
            ' Muestra 2.04 KG (6 OZ)': 'muestra_204kg_6oz',
            'Muestra 3.0 KG (BULK)': 'muestra_30kg_bulk',
            'Muestra 12*1pt': 'muestra_12x1pt',
            'MUESTRA 3.3KG CANASTILLA SL-M': 'muestra_33kg_canastilla_sl_m',
            'KG DE MUESTRA': 'kg_de_muestra',
            'TOTAL CAJAS EXPORTADAS': 'total_cajas_exportadas',
            'TOTAL DE CAJAS EXPORTADAS + MUESTRAS': 'total_cajas_exportadas_muestras',
            '1.- Recepción': 'h_recepcion',
            '2.- Pesado MP': 'h_pesado_mp',
            '3.- Abastecimiento': 'h_abastecimiento',
            '4.- Retiro de jarras/jabas': 'h_retiro_jarras_jabas',
            '5.- Industrial': 'h_industrial',
            '6.- Pesado': 'h_pesado',
            '7.- Encajado': 'h_encajado',
            '8.- Abastecedor de Clamshells': 'h_abastecedor_clamshells',
            '9.- Etiquetado': 'h_etiquetado',
            '10.- Paletizado': 'h_paletizado',
            '11.- Enzunchado': 'h_enzunchado',
            '12.- Montacarguista': 'h_montacarguista',
            '13.- Control empaque': 'h_control_empaque',
            '14.- Pizarra': 'h_pizarra',
            '15.- Almacen': 'h_almacen',
            '16.- Trazabilidad': 'h_trazabilidad',
            '17.- Habilitacion material': 'h_habilitacion_material',
            '18.- Limpieza': 'h_limpieza',
            '19.- Aux. Producc.': 'h_aux_producc',
            'Apoyo otras áreas': 'h_apoyo_otras_areas',
            'H. INICIO': 'h_inicio',
            'H. FINAL': 'h_final',
            'REFRIGERIO': 'refrigerio',
            'PARADAS': 'paradas',
            'TOTAL HORAS LABORADAS': 'total_horas_laboradas',
            'HORAS EFECTIVAS': 'horas_efectivas',
            'N° OPERARIOS': 'n_operarios',
            'hr hombre totales': 'hr_hombre_totales',
            'hr hombre efectivas': 'hr_hombre_efectivas',
            'PRODUCTIVIDAD 1(kg/hr*oper.)': 'productividad_1_kg_hr_oper',
            'Productividad Pesado': 'productividad_pesado',
            'CAJAS/HORA': 'cajas_hora',
            'Objetivo (kg/hr*pesador)': 'objetivo_kg_hr_pesador',
            '4.4OZ': 'kg_44oz',
            '12X7OZ': 'kg_12x7oz',
            '2.04KG 6OZ': 'kg_204kg_6oz',
            '9.8OZ PINTA PLANA': 'kg_98oz_pinta_plana',
            '3.8KG PINTA ': 'kg_38kg_pinta',
            'GRANEL': 'kg_granel',
            '4.08KG 8X18OZ': 'kg_408kg_8x18oz',
            '6.12KG 12X18OZ': 'kg_612kg_12x18oz',
            'OBSERVACION': 'observacion'
        }
        
        # Insertar en tabla temporal
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_temp_sql = f"""
        INSERT INTO reporte_produccion_temp ({', '.join(columns_list)})
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
                                if 'fecha' in db_col:
                                    if isinstance(value, str):
                                        try:
                                            from datetime import datetime
                                            value = datetime.strptime(value, '%Y-%m-%d').date()
                                        except:
                                            values.append(None)
                                            continue
                                    values.append(value)
                                elif 'h_inicio' in db_col or 'h_final' in db_col:
                                    if isinstance(value, str) and ':' in value:
                                        try:
                                            from datetime import time
                                            time_parts = value.split(':')
                                            if len(time_parts) >= 2:
                                                hour = int(time_parts[0])
                                                minute = int(time_parts[1])
                                                second = int(time_parts[2]) if len(time_parts) > 2 else 0
                                                values.append(time(hour, minute, second))
                                            else:
                                                values.append(None)
                                        except:
                                            values.append(None)
                                    else:
                                        values.append(None)
                                elif 'kg_' in db_col or 'pct_' in db_col or 'productividad' in db_col or 'cajas_hora' in db_col or 'objetivo' in db_col:
                                    try:
                                        values.append(float(value) if value != 0 else 0.0)
                                    except:
                                        values.append(0.0)
                                elif 'caja_' in db_col or 'muestra_' in db_col or 'n_' in db_col or 'total_' in db_col or 'formato' in db_col:
                                    try:
                                        values.append(int(float(value)) if value != 0 else 0)
                                    except:
                                        values.append(0)
                                else:
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
        cursor.execute("TRUNCATE TABLE reporte_produccion RESTART IDENTITY;")
        
        # Copiar datos de la tabla temporal a la principal
        cursor.execute("""
            INSERT INTO reporte_produccion 
            SELECT * FROM reporte_produccion_temp;
        """)
        
        # Confirmar transacción
        cursor.execute("COMMIT;")
        
        # Paso 4: Limpiar tabla temporal
        cursor.execute("DROP TABLE reporte_produccion_temp;")
        
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
                cursor.execute("DROP TABLE IF EXISTS reporte_produccion_temp;")
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

def clear_reporte_produccion_table():
    """
    Vaciar completamente la tabla reporte_produccion de manera segura
    """
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Contar registros antes de eliminar
        cursor.execute("SELECT COUNT(*) FROM reporte_produccion;")
        count_before = cursor.fetchone()[0]
        
        if count_before == 0:
            logger.info("ℹ️ La tabla reporte_produccion ya está vacía")
            cursor.close()
            connection.close()
            return True
        
        # Mostrar confirmación
        logger.warning(f"⚠️ Se van a eliminar {count_before} registros de reporte_produccion")
        
        # Truncar tabla (más rápido que DELETE)
        cursor.execute("TRUNCATE TABLE reporte_produccion RESTART IDENTITY;")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"✅ Se eliminaron {count_before} registros de reporte_produccion")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error al vaciar la tabla: {e}")
        if connection:
            connection.close()
        return False
