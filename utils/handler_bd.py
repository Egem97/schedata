import psycopg2
import streamlit as st
from psycopg2.extras import RealDictCursor
import sqlalchemy
from sqlalchemy import create_engine, text
import io
import pandas as pd

from utils.timezone_utils import get_lima_date_string
# Configuración de la base de datos PostgreSQL
DB_CONFIG = {
    'host': '34.136.15.241',
    'port': 5666,
    'database': 'apg_database',
    'user': 'apg_adm_v1',
    'password': 'hfuBZyXf4Dni',
    'options': '-c timezone=America/Lima'  # Configurar zona horaria de Lima
}
def create_database_connection():
    """Crear conexión a la base de datos PostgreSQL"""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")
        return None

def create_table_if_not_exists():
    """Crear la tabla si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con la estructura del dataframe
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS images_fcl_drive (
            id SERIAL PRIMARY KEY,
            folder_id VARCHAR(255),
            folder_name VARCHAR(255),
            folder_webViewLink TEXT,
            folder_modifiedTime TIMESTAMPTZ,
            image_id VARCHAR(255) UNIQUE NOT NULL,
            image_name VARCHAR(255),
            image_webViewLink TEXT,
            image_modifiedTime TIMESTAMPTZ,
            image_base64 TEXT,
            image_size_mb DECIMAL(10, 6),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_images_fcl_folder_id ON images_fcl_drive(folder_id);",
            "CREATE INDEX IF NOT EXISTS idx_images_fcl_folder_name ON images_fcl_drive(folder_name);",
            "CREATE INDEX IF NOT EXISTS idx_images_fcl_created_at ON images_fcl_drive(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                st.warning(f"⚠️ Error creando índice: {index_error}")
        
        # Verificar si existe la restricción única en image_id
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.table_constraints 
            WHERE table_name = 'images_fcl_drive' 
            AND constraint_type = 'UNIQUE' 
            AND constraint_name LIKE '%image_id%'
        """)
        
        unique_constraint_exists = cursor.fetchone()[0] > 0
        
        if not unique_constraint_exists:
            try:
                # Agregar restricción única si no existe (para tablas existentes)
                cursor.execute("ALTER TABLE images_fcl_drive ADD CONSTRAINT unique_image_id UNIQUE (image_id);")
                st.info("✅ Restricción única agregada a image_id")
            except Exception as constraint_error:
                st.warning(f"⚠️ No se pudo agregar restricción única: {constraint_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        st.success("✅ Tabla e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        st.error(f"Error al crear la tabla: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_dataframe_to_postgresql(df):
    """Insertar el dataframe en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        st.warning("⚠️ No hay datos para insertar")
        return False
    
    # Mostrar información del dataframe
    st.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    st.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Usar UPSERT para evitar duplicados
        insert_sql = """
        INSERT INTO images_fcl_drive (
            folder_id, folder_name, folder_webViewLink, folder_modifiedTime,
            image_id, image_name, image_webViewLink, image_modifiedTime,
            image_base64, image_size_mb
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (image_id) 
        DO UPDATE SET 
            folder_name = EXCLUDED.folder_name,
            folder_webViewLink = EXCLUDED.folder_webViewLink,
            folder_modifiedTime = EXCLUDED.folder_modifiedTime,
            image_name = EXCLUDED.image_name,
            image_webViewLink = EXCLUDED.image_webViewLink,
            image_modifiedTime = EXCLUDED.image_modifiedTime,
            image_base64 = EXCLUDED.image_base64,
            image_size_mb = EXCLUDED.image_size_mb
        """
        
        # Contar registros a insertar
        total_records = len(df)
        processed_records = 0
        error_records = 0
        updated_records = 0
        
        # Progreso
        progress_bar = st.progress(0)
        status_text = st.empty()
        error_container = st.container()
        
        # Lista para almacenar errores detallados
        detailed_errors = []
        
        # Procesar en lotes más pequeños para mejor rendimiento y memoria
        batch_size = 5  # Reducido debido a datos base64 grandes
        
        for i in range(0, total_records, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_values = []
            batch_errors = []
            
            # Preparar valores del lote
            for index, row in batch_df.iterrows():
                try:
                    # Validar que image_id no sea nulo (requerido para UPSERT)
                    if pd.isna(row.get('image_id')) or not str(row.get('image_id', '')).strip():
                        error_msg = f"image_id es requerido para fila {index}"
                        batch_errors.append((index, error_msg))
                        continue
                    
                    # Convertir timestamps de manera más robusta y a zona horaria de Lima
                    folder_modified_time = None
                    image_modified_time = None
                    
                    if pd.notna(row.get('folder_modifiedTime')):
                        try:
                            # Usar la función de timezone para parsear y convertir a Lima
                            from utils.timezone_utils import parse_google_drive_timestamp
                            folder_modified_time = parse_google_drive_timestamp(str(row['folder_modifiedTime']))
                        except Exception as e:
                            detailed_errors.append(f"Error procesando folder_modifiedTime en fila {index}: {e}")
                            folder_modified_time = None
                    
                    if pd.notna(row.get('image_modifiedTime')):
                        try:
                            # Usar la función de timezone para parsear y convertir a Lima
                            from utils.timezone_utils import parse_google_drive_timestamp
                            image_modified_time = parse_google_drive_timestamp(str(row['image_modifiedTime']))
                        except Exception as e:
                            detailed_errors.append(f"Error procesando image_modifiedTime en fila {index}: {e}")
                            image_modified_time = None
                    
                    # Manejar base64 con límites más estrictos
                    base64_data = None
                    if pd.notna(row.get('image_base64')):
                        base64_str = str(row.get('image_base64', ''))
                        # Límite más conservador para evitar problemas de memoria
                        max_base64_size = 5000000  # 5MB en caracteres
                        if len(base64_str) > max_base64_size:
                            st.warning(f"⚠️ Base64 muy grande ({len(base64_str):,} chars) para fila {index}, se omitirá")
                            base64_data = None  # No insertar base64 muy grande
                        else:
                            base64_data = base64_str
                    
                    # Validar y preparar valores
                    values = (
                        str(row.get('folder_id', ''))[:255] if pd.notna(row.get('folder_id')) else None,
                        str(row.get('folder_name', ''))[:255] if pd.notna(row.get('folder_name')) else None,
                        str(row.get('folder_webViewLink', ''))[:2000] if pd.notna(row.get('folder_webViewLink')) else None,  # URLs pueden ser largas
                        folder_modified_time,
                        str(row.get('image_id', ''))[:255] if pd.notna(row.get('image_id')) else None,
                        str(row.get('image_name', ''))[:255] if pd.notna(row.get('image_name')) else None,
                        str(row.get('image_webViewLink', ''))[:2000] if pd.notna(row.get('image_webViewLink')) else None,
                        image_modified_time,
                        base64_data,
                        float(row.get('image_size_mb', 0)) if pd.notna(row.get('image_size_mb')) and row.get('image_size_mb') != '' else 0.0
                    )
                    
                    batch_values.append(values)
                    
                except Exception as e:
                    error_msg = f"Error preparando datos para fila {index}: {str(e)}"
                    batch_errors.append((index, error_msg))
                    detailed_errors.append(error_msg)
            
            # Insertar lote si hay valores válidos
            if batch_values:
                try:
                    cursor.executemany(insert_sql, batch_values)
                    processed_records += len(batch_values)
                    
                    # Commit cada lote para evitar transacciones muy largas
                    connection.commit()
                    
                except Exception as e:
                    # Rollback en caso de error
                    connection.rollback()
                    error_msg = f"Error insertando lote {i//batch_size + 1}: {str(e)}"
                    detailed_errors.append(error_msg)
                    
                    # Intentar insertar registros uno por uno para identificar el problema
                    for j, values in enumerate(batch_values):
                        try:
                            cursor.execute(insert_sql, values)
                            connection.commit()
                            processed_records += 1
                        except Exception as individual_error:
                            connection.rollback()
                            error_records += 1
                            error_msg = f"Error en registro individual {i+j}: {str(individual_error)[:100]}"
                            detailed_errors.append(error_msg)
            
            # Contar errores del lote
            error_records += len(batch_errors)
            
            # Mostrar errores del lote
            if batch_errors:
                with error_container:
                    for idx, error in batch_errors:
                        st.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            progress_bar.progress(progress)
            status_text.text(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Limpiar elementos de progreso
        progress_bar.empty()
        status_text.empty()
        
        # Mostrar resultados finales
        if error_records == 0:
            st.success(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            st.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            with st.expander("Ver errores detallados"):
                for error in detailed_errors:
                    st.text(error)
        elif len(detailed_errors) > 10:
            st.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            with st.expander("Ver primeros 10 errores"):
                for error in detailed_errors[:10]:
                    st.text(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        st.error(f"❌ Error crítico al insertar datos: {str(e)}")
        st.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def clear_table_data():
    """Limpiar todos los datos de la tabla"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM images_fcl_drive")
        connection.commit()
        cursor.close()
        connection.close()
        
        st.success("✅ Tabla limpiada exitosamente")
        return True
        
    except Exception as e:
        st.error(f"Error al limpiar la tabla: {e}")
        if connection:
            connection.close()
        return False

def get_table_data():
    """Obtener datos de la tabla para verificación"""
    connection = create_database_connection()
    if not connection:
        return None
    
    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM images_fcl_drive ORDER BY created_at DESC LIMIT 100")
        data = cursor.fetchall()
        cursor.close()
        connection.close()
        
        if data:
            df = pd.DataFrame(data)
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error al obtener datos: {e}")
        if connection:
            connection.close()
        return None

def validate_dataframe_for_insertion(df):
    """Validar y limpiar el dataframe antes de la inserción"""
    if df.empty:
        return df, []
    
    validation_errors = []
    cleaned_df = df.copy()
    
    # Validar columnas requeridas
    required_columns = [
        'folder_id', 'folder_name', 'folder_webViewLink', 'folder_modifiedTime',
        'image_id', 'image_name', 'image_webViewLink', 'image_modifiedTime',
        'image_base64', 'image_size_mb'
    ]
    
    missing_columns = [col for col in required_columns if col not in cleaned_df.columns]
    if missing_columns:
        validation_errors.append(f"Columnas faltantes: {missing_columns}")
        return df, validation_errors
    
    # Filtrar filas con image_id nulo o vacío (requerido para UPSERT)
    initial_rows = len(cleaned_df)
    cleaned_df = cleaned_df[cleaned_df['image_id'].notna()]
    cleaned_df = cleaned_df[cleaned_df['image_id'].astype(str).str.strip() != '']
    removed_rows = initial_rows - len(cleaned_df)
    
    if removed_rows > 0:
        validation_errors.append(f"Se removieron {removed_rows} filas con image_id nulo o vacío")
    
    # Remover duplicados basados en image_id
    initial_rows = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates(subset=['image_id'], keep='last')
    removed_duplicates = initial_rows - len(cleaned_df)
    
    if removed_duplicates > 0:
        validation_errors.append(f"Se removieron {removed_duplicates} duplicados basados en image_id")
    
    # Validar y limpiar datos de texto
    text_columns = ['folder_name', 'image_name']
    for col in text_columns:
        if col in cleaned_df.columns:
            # Limpiar espacios en blanco
            cleaned_df[col] = cleaned_df[col].astype(str).str.strip()
            # Truncar si es muy largo
            max_length = 255
            long_values = cleaned_df[col].str.len() > max_length
            if long_values.any():
                validation_errors.append(f"Se truncaron {long_values.sum()} valores largos en columna {col}")
                cleaned_df[col] = cleaned_df[col].str[:max_length]
    
    # Validar URLs
    url_columns = ['folder_webViewLink', 'image_webViewLink']
    for col in url_columns:
        if col in cleaned_df.columns:
            # Truncar URLs muy largas
            max_length = 2000
            long_urls = cleaned_df[col].astype(str).str.len() > max_length
            if long_urls.any():
                validation_errors.append(f"Se truncaron {long_urls.sum()} URLs largas en columna {col}")
                cleaned_df[col] = cleaned_df[col].astype(str).str[:max_length]
    
    # Validar tamaños de imagen
    if 'image_size_mb' in cleaned_df.columns:
        # Convertir a numérico, reemplazar valores inválidos con 0
        cleaned_df['image_size_mb'] = pd.to_numeric(cleaned_df['image_size_mb'], errors='coerce').fillna(0)
        # Limitar valores extremos
        extreme_values = (cleaned_df['image_size_mb'] > 1000) | (cleaned_df['image_size_mb'] < 0)
        if extreme_values.any():
            validation_errors.append(f"Se corrigieron {extreme_values.sum()} valores extremos en image_size_mb")
            cleaned_df.loc[extreme_values, 'image_size_mb'] = 0
    
    # Validar base64 (opcional pero útil)
    if 'image_base64' in cleaned_df.columns:
        # Contar imágenes sin base64
        no_base64 = cleaned_df['image_base64'].isna().sum()
        if no_base64 > 0:
            validation_errors.append(f"Advertencia: {no_base64} imágenes sin datos base64")
        
        # Verificar tamaños de base64 muy grandes
        base64_sizes = cleaned_df['image_base64'].astype(str).str.len()
        large_base64 = base64_sizes > 5000000  # 5MB en caracteres
        if large_base64.any():
            validation_errors.append(f"Advertencia: {large_base64.sum()} imágenes con base64 muy grande (>5MB)")
    
    return cleaned_df, validation_errors



def clear_day_gcl_img(target_date=None):
    """
    Limpiar todos los registros de una fecha específica de la tabla images_fcl_drive
    
    Args:
        target_date (str, optional): Fecha en formato 'YYYY-MM-DD'. 
                                   Si no se proporciona, usa la fecha actual en Lima.
    
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    from utils.timezone_utils import get_lima_date_string
    
    # Si no se proporciona fecha, usar la fecha actual en Lima
    if target_date is None:
        target_date = get_lima_date_string()
    
    # Validar formato de fecha
    try:
        from datetime import datetime
        datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError:
        st.error(f"❌ Formato de fecha inválido: {target_date}. Use formato YYYY-MM-DD")
        return False
    
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Primero contar cuántos registros se van a eliminar
        count_sql = """
        SELECT COUNT(*) FROM images_fcl_drive 
        WHERE DATE(folder_modifiedTime) = %s 
           OR DATE(image_modifiedTime) = %s
        """
        cursor.execute(count_sql, (target_date, target_date))
        records_to_delete = cursor.fetchone()[0]
        
        if records_to_delete == 0:
            st.info(f"ℹ️ No se encontraron registros para la fecha {target_date}")
            cursor.close()
            connection.close()
            return True
        
        # Mostrar confirmación
        st.warning(f"⚠️ Se van a eliminar {records_to_delete} registros de la fecha {target_date}")
        
        # Eliminar registros de la fecha específica
        delete_sql = """
        DELETE FROM images_fcl_drive 
        WHERE DATE(folder_modifiedTime) = %s 
           OR DATE(image_modifiedTime) = %s
        """
        
        cursor.execute(delete_sql, (target_date, target_date))
        deleted_rows = cursor.rowcount
        
        connection.commit()
        cursor.close()
        connection.close()
        
        st.success(f"✅ Se eliminaron {deleted_rows} registros de la fecha {target_date}")
        return True
        
    except Exception as e:
        st.error(f"❌ Error al limpiar registros de {target_date}: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def clear_date_range_gcl_img(start_date, end_date=None):
    """
    Limpiar registros de un rango de fechas específico de la tabla images_fcl_drive
    
    Args:
        start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'
        end_date (str, optional): Fecha de fin en formato 'YYYY-MM-DD'. 
                                Si no se proporciona, usa la misma fecha de inicio.
    
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    from datetime import datetime
    
    # Si no se proporciona fecha de fin, usar la fecha de inicio
    if end_date is None:
        end_date = start_date
    
    # Validar formatos de fecha
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError as e:
        st.error(f"❌ Formato de fecha inválido: {e}. Use formato YYYY-MM-DD")
        return False
    
    # Verificar que start_date <= end_date
    if start_date > end_date:
        st.error(f"❌ La fecha de inicio ({start_date}) debe ser menor o igual a la fecha de fin ({end_date})")
        return False
    
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Primero contar cuántos registros se van a eliminar
        count_sql = """
        SELECT COUNT(*) FROM images_fcl_drive 
        WHERE (DATE(folder_modifiedTime) BETWEEN %s AND %s)
           OR (DATE(image_modifiedTime) BETWEEN %s AND %s)
        """
        cursor.execute(count_sql, (start_date, end_date, start_date, end_date))
        records_to_delete = cursor.fetchone()[0]
        
        if records_to_delete == 0:
            date_range = start_date if start_date == end_date else f"{start_date} a {end_date}"
            st.info(f"ℹ️ No se encontraron registros para el rango de fechas {date_range}")
            cursor.close()
            connection.close()
            return True
        
        # Mostrar confirmación
        date_range = start_date if start_date == end_date else f"{start_date} a {end_date}"
        st.warning(f"⚠️ Se van a eliminar {records_to_delete} registros del rango {date_range}")
        
        # Eliminar registros del rango de fechas
        delete_sql = """
        DELETE FROM images_fcl_drive 
        WHERE (DATE(folder_modifiedTime) BETWEEN %s AND %s)
           OR (DATE(image_modifiedTime) BETWEEN %s AND %s)
        """
        
        cursor.execute(delete_sql, (start_date, end_date, start_date, end_date))
        deleted_rows = cursor.rowcount
        
        connection.commit()
        cursor.close()
        connection.close()
        
        st.success(f"✅ Se eliminaron {deleted_rows} registros del rango {date_range}")
        return True
        
    except Exception as e:
        date_range = start_date if start_date == end_date else f"{start_date} a {end_date}"
        st.error(f"❌ Error al limpiar registros del rango {date_range}: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False