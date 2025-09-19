import psycopg2
import logging
from psycopg2.extras import RealDictCursor
import sqlalchemy
from sqlalchemy import create_engine, text
import io
import pandas as pd
from utils.config import load_config
from utils.timezone_utils import get_lima_date_string

logger = logging.getLogger(__name__)
# Configuración de la base de datos PostgreSQL
config = load_config()
    
DB_CONFIG = {
    'host': config['db']['host'],
    'port': config['db']['port'],
    'database': config['db']['database'],
    'user': config['db']['user'],
    'password': config['db']['password'],
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
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
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
                logger.info("✅ Restricción única agregada a image_id")
            except Exception as constraint_error:
                logger.warning(f"⚠️ No se pudo agregar restricción única: {constraint_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla: {e}")
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
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
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
        logger.info(f"Iniciando procesamiento de {total_records} registros...")
        
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
                            logger.warning(f"⚠️ Base64 muy grande ({len(base64_str):,} chars) para fila {index}, se omitirá")
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_presentaciones_table():
    """Crear la tabla presentaciones si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe presentaciones
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS presentaciones (
            id SERIAL PRIMARY KEY,
            descripcion_producto VARCHAR(255) UNIQUE NOT NULL,
            peso_caja DECIMAL(10, 3),
            sobre_peso DECIMAL(10, 3),
            esquinero_adicionales INTEGER,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_descripcion ON presentaciones(descripcion_producto);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_peso_caja ON presentaciones(peso_caja);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_created_at ON presentaciones(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla presentaciones e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla presentaciones: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_presentaciones_to_postgresql(df):
    """Insertar el dataframe de presentaciones en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'DESCRIPCION DE PRODUCTO': 'descripcion_producto',
            'PESO caja': 'peso_caja',
            'SOBRE PESO': 'sobre_peso',
            'ESQUINEROS ADIONALES': 'esquinero_adicionales'
        }
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO presentaciones ({', '.join(columns_list)})
        VALUES ({placeholders})
        ON CONFLICT (descripcion_producto) 
        DO UPDATE SET 
            peso_caja = EXCLUDED.peso_caja,
            sobre_peso = EXCLUDED.sobre_peso,
            esquinero_adicionales = EXCLUDED.esquinero_adicionales,
            updated_at = CURRENT_TIMESTAMP
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_phl_pt_all_tabla_table():
    """Crear la tabla phl_pt_all_tabla si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe phl_pt_all_tabla
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS phl_pt_all_tabla (
            id SERIAL PRIMARY KEY,
            envio VARCHAR(255),
            semana DECIMAL(10, 2),
            fecha_produccion TIMESTAMPTZ,
            fecha_cosecha TIMESTAMPTZ,
            cliente VARCHAR(255),
            tipo_pallet VARCHAR(255),
            contenedor VARCHAR(255),
            descripcion_producto VARCHAR(255),
            destino VARCHAR(255),
            fundo VARCHAR(255),
            variedad VARCHAR(255),
            n_cajas DECIMAL(15, 3),
            n_pallet VARCHAR(255),
            turno DECIMAL(10, 2),
            linea DECIMAL(10, 2),
            phl_origen VARCHAR(255),
            materiales_adicionales VARCHAR(255),
            observaciones TEXT,
            sobre_peso INTEGER,
            peso_caja DECIMAL(15, 3),
            exportable DECIMAL(15, 3),
            estado VARCHAR(255),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_semana ON phl_pt_all_tabla(semana);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fecha_produccion ON phl_pt_all_tabla(fecha_produccion);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fecha_cosecha ON phl_pt_all_tabla(fecha_cosecha);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_cliente ON phl_pt_all_tabla(cliente);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fundo ON phl_pt_all_tabla(fundo);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_variedad ON phl_pt_all_tabla(variedad);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_estado ON phl_pt_all_tabla(estado);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_created_at ON phl_pt_all_tabla(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla phl_pt_all_tabla e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla phl_pt_all_tabla: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_phl_pt_all_tabla_to_postgresql(df):
    """Insertar el dataframe de phl_pt_all_tabla en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe

    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
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
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO phl_pt_all_tabla ({', '.join(columns_list)})
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_presentaciones_table():
    """Crear la tabla presentaciones si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe presentaciones
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS presentaciones (
            id SERIAL PRIMARY KEY,
            descripcion_producto VARCHAR(255) UNIQUE NOT NULL,
            peso_caja DECIMAL(10, 3),
            sobre_peso DECIMAL(10, 3),
            esquinero_adicionales INTEGER,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_descripcion ON presentaciones(descripcion_producto);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_peso_caja ON presentaciones(peso_caja);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_created_at ON presentaciones(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla presentaciones e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla presentaciones: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_presentaciones_to_postgresql(df):
    """Insertar el dataframe de presentaciones en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'DESCRIPCION DE PRODUCTO': 'descripcion_producto',
            'PESO caja': 'peso_caja',
            'SOBRE PESO': 'sobre_peso',
            'ESQUINEROS ADIONALES': 'esquinero_adicionales'
        }
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO presentaciones ({', '.join(columns_list)})
        VALUES ({placeholders})
        ON CONFLICT (descripcion_producto) 
        DO UPDATE SET 
            peso_caja = EXCLUDED.peso_caja,
            sobre_peso = EXCLUDED.sobre_peso,
            esquinero_adicionales = EXCLUDED.esquinero_adicionales,
            updated_at = CURRENT_TIMESTAMP
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
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
        
        logger.info("✅ Tabla limpiada exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al limpiar la tabla: {e}")
        if connection:
            connection.close()

def create_presentaciones_table():
    """Crear la tabla presentaciones si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe presentaciones
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS presentaciones (
            id SERIAL PRIMARY KEY,
            descripcion_producto VARCHAR(255) UNIQUE NOT NULL,
            peso_caja DECIMAL(10, 3),
            sobre_peso DECIMAL(10, 3),
            esquinero_adicionales INTEGER,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_descripcion ON presentaciones(descripcion_producto);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_peso_caja ON presentaciones(peso_caja);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_created_at ON presentaciones(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla presentaciones e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla presentaciones: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_presentaciones_to_postgresql(df):
    """Insertar el dataframe de presentaciones en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'DESCRIPCION DE PRODUCTO': 'descripcion_producto',
            'PESO caja': 'peso_caja',
            'SOBRE PESO': 'sobre_peso',
            'ESQUINEROS ADIONALES': 'esquinero_adicionales'
        }
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO presentaciones ({', '.join(columns_list)})
        VALUES ({placeholders})
        ON CONFLICT (descripcion_producto) 
        DO UPDATE SET 
            peso_caja = EXCLUDED.peso_caja,
            sobre_peso = EXCLUDED.sobre_peso,
            esquinero_adicionales = EXCLUDED.esquinero_adicionales,
            updated_at = CURRENT_TIMESTAMP
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_phl_pt_all_tabla_table():
    """Crear la tabla phl_pt_all_tabla si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe phl_pt_all_tabla
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS phl_pt_all_tabla (
            id SERIAL PRIMARY KEY,
            envio VARCHAR(255),
            semana DECIMAL(10, 2),
            fecha_produccion TIMESTAMPTZ,
            fecha_cosecha TIMESTAMPTZ,
            cliente VARCHAR(255),
            tipo_pallet VARCHAR(255),
            contenedor VARCHAR(255),
            descripcion_producto VARCHAR(255),
            destino VARCHAR(255),
            fundo VARCHAR(255),
            variedad VARCHAR(255),
            n_cajas DECIMAL(15, 3),
            n_pallet VARCHAR(255),
            turno DECIMAL(10, 2),
            linea DECIMAL(10, 2),
            phl_origen VARCHAR(255),
            materiales_adicionales VARCHAR(255),
            observaciones TEXT,
            sobre_peso INTEGER,
            peso_caja DECIMAL(15, 3),
            exportable DECIMAL(15, 3),
            estado VARCHAR(255),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_semana ON phl_pt_all_tabla(semana);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fecha_produccion ON phl_pt_all_tabla(fecha_produccion);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fecha_cosecha ON phl_pt_all_tabla(fecha_cosecha);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_cliente ON phl_pt_all_tabla(cliente);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fundo ON phl_pt_all_tabla(fundo);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_variedad ON phl_pt_all_tabla(variedad);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_estado ON phl_pt_all_tabla(estado);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_created_at ON phl_pt_all_tabla(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla phl_pt_all_tabla e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla phl_pt_all_tabla: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_phl_pt_all_tabla_to_postgresql(df):
    """Insertar el dataframe de phl_pt_all_tabla en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
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
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO phl_pt_all_tabla ({', '.join(columns_list)})
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_presentaciones_table():
    """Crear la tabla presentaciones si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe presentaciones
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS presentaciones (
            id SERIAL PRIMARY KEY,
            descripcion_producto VARCHAR(255) UNIQUE NOT NULL,
            peso_caja DECIMAL(10, 3),
            sobre_peso DECIMAL(10, 3),
            esquinero_adicionales INTEGER,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_descripcion ON presentaciones(descripcion_producto);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_peso_caja ON presentaciones(peso_caja);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_created_at ON presentaciones(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla presentaciones e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla presentaciones: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_presentaciones_to_postgresql(df):
    """Insertar el dataframe de presentaciones en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'DESCRIPCION DE PRODUCTO': 'descripcion_producto',
            'PESO caja': 'peso_caja',
            'SOBRE PESO': 'sobre_peso',
            'ESQUINEROS ADIONALES': 'esquinero_adicionales'
        }
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO presentaciones ({', '.join(columns_list)})
        VALUES ({placeholders})
        ON CONFLICT (descripcion_producto) 
        DO UPDATE SET 
            peso_caja = EXCLUDED.peso_caja,
            sobre_peso = EXCLUDED.sobre_peso,
            esquinero_adicionales = EXCLUDED.esquinero_adicionales,
            updated_at = CURRENT_TIMESTAMP
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        return False

def create_evaluacion_calidad_pt_table():
    """Crear la tabla evaluacion_calidad_pt si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas de evaluación de calidad
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS evaluacion_calidad_pt (
            id SERIAL PRIMARY KEY,
            fecha_mp TIMESTAMPTZ,
            fecha_proceso TIMESTAMPTZ,
            semana INTEGER,
            evaluador VARCHAR(255),
            productor VARCHAR(255),
            tipo_producto VARCHAR(255),
            fundo VARCHAR(255),
            hora VARCHAR(50),
            linea DECIMAL(10, 2),
            viaje INTEGER,
            modulo INTEGER,
            turno VARCHAR(50),
            variedad VARCHAR(255),
            presentacion VARCHAR(255),
            destino VARCHAR(255),
            tipo_caja VARCHAR(255),
            trazabilidad VARCHAR(255),
            peso_muestra_g DECIMAL(10, 3),
            frutos_con_pedicelo DECIMAL(8, 3),
            fumagina DECIMAL(8, 3),
            f_bloom DECIMAL(8, 3),
            herida_cicatrizada DECIMAL(8, 3),
            excreta_abeja DECIMAL(8, 3),
            russet DECIMAL(8, 3),
            polvo DECIMAL(8, 3),
            frutos_rojizos DECIMAL(8, 3),
            restos_florales DECIMAL(8, 3),
            halo_verde DECIMAL(8, 3),
            picado DECIMAL(8, 3),
            bajo_calibre DECIMAL(8, 3),
            chanchito_blanco DECIMAL(8, 3),
            f_mojada DECIMAL(8, 3),
            dano_trips DECIMAL(8, 3),
            otros DECIMAL(8, 3),
            total_defectos_calidad DECIMAL(8, 3),
            herida_abierta DECIMAL(8, 3),
            queresa DECIMAL(8, 3),
            deshidratacion_leve DECIMAL(8, 3),
            deshidratacion_moderado DECIMAL(8, 3),
            deshidratado_severo DECIMAL(8, 3),
            machucon DECIMAL(8, 3),
            desgarro DECIMAL(8, 3),
            sobremaduro DECIMAL(8, 3),
            blanda_severa DECIMAL(8, 3),
            blanda_moderado DECIMAL(8, 3),
            excreta_ave DECIMAL(8, 3),
            hongos DECIMAL(8, 3),
            pudricion DECIMAL(8, 3),
            baya_reventada DECIMAL(8, 3),
            baya_colapsada DECIMAL(8, 3),
            presencia_larva DECIMAL(8, 3),
            exudacion DECIMAL(8, 3),
            otros2 DECIMAL(8, 3),
            total_condicion DECIMAL(8, 3),
            total_exportable DECIMAL(8, 3),
            total_no_exportable DECIMAL(8, 3),
            n_fcl VARCHAR(255),
            calibre VARCHAR(50),
            brix DECIMAL(8, 3),
            acidez DECIMAL(8, 3),
            observaciones TEXT,
            empresa VARCHAR(255),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_evaluacion_calidad_fecha_proceso ON evaluacion_calidad_pt(fecha_proceso);",
            "CREATE INDEX IF NOT EXISTS idx_evaluacion_calidad_semana ON evaluacion_calidad_pt(semana);",
            "CREATE INDEX IF NOT EXISTS idx_evaluacion_calidad_empresa ON evaluacion_calidad_pt(empresa);",
            "CREATE INDEX IF NOT EXISTS idx_evaluacion_calidad_fundo ON evaluacion_calidad_pt(fundo);",
            "CREATE INDEX IF NOT EXISTS idx_evaluacion_calidad_variedad ON evaluacion_calidad_pt(variedad);",
            "CREATE INDEX IF NOT EXISTS idx_evaluacion_calidad_n_fcl ON evaluacion_calidad_pt(n_fcl);",
            "CREATE INDEX IF NOT EXISTS idx_evaluacion_calidad_created_at ON evaluacion_calidad_pt(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla evaluacion_calidad_pt e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla evaluacion_calidad_pt: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
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
        logger.error(f"Error al obtener datos: {e}")
        if connection:
            connection.close()

def create_presentaciones_table():
    """Crear la tabla presentaciones si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe presentaciones
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS presentaciones (
            id SERIAL PRIMARY KEY,
            descripcion_producto VARCHAR(255) UNIQUE NOT NULL,
            peso_caja DECIMAL(10, 3),
            sobre_peso DECIMAL(10, 3),
            esquinero_adicionales INTEGER,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_descripcion ON presentaciones(descripcion_producto);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_peso_caja ON presentaciones(peso_caja);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_created_at ON presentaciones(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla presentaciones e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla presentaciones: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_presentaciones_to_postgresql(df):
    """Insertar el dataframe de presentaciones en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'DESCRIPCION DE PRODUCTO': 'descripcion_producto',
            'PESO caja': 'peso_caja',
            'SOBRE PESO': 'sobre_peso',
            'ESQUINEROS ADIONALES': 'esquinero_adicionales'
        }
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO presentaciones ({', '.join(columns_list)})
        VALUES ({placeholders})
        ON CONFLICT (descripcion_producto) 
        DO UPDATE SET 
            peso_caja = EXCLUDED.peso_caja,
            sobre_peso = EXCLUDED.sobre_peso,
            esquinero_adicionales = EXCLUDED.esquinero_adicionales,
            updated_at = CURRENT_TIMESTAMP
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_phl_pt_all_tabla_table():
    """Crear la tabla phl_pt_all_tabla si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe phl_pt_all_tabla
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS phl_pt_all_tabla (
            id SERIAL PRIMARY KEY,
            envio VARCHAR(255),
            semana DECIMAL(10, 2),
            fecha_produccion TIMESTAMPTZ,
            fecha_cosecha TIMESTAMPTZ,
            cliente VARCHAR(255),
            tipo_pallet VARCHAR(255),
            contenedor VARCHAR(255),
            descripcion_producto VARCHAR(255),
            destino VARCHAR(255),
            fundo VARCHAR(255),
            variedad VARCHAR(255),
            n_cajas DECIMAL(15, 3),
            n_pallet VARCHAR(255),
            turno DECIMAL(10, 2),
            linea DECIMAL(10, 2),
            phl_origen VARCHAR(255),
            materiales_adicionales VARCHAR(255),
            observaciones TEXT,
            sobre_peso INTEGER,
            peso_caja DECIMAL(15, 3),
            exportable DECIMAL(15, 3),
            estado VARCHAR(255),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_semana ON phl_pt_all_tabla(semana);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fecha_produccion ON phl_pt_all_tabla(fecha_produccion);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fecha_cosecha ON phl_pt_all_tabla(fecha_cosecha);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_cliente ON phl_pt_all_tabla(cliente);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fundo ON phl_pt_all_tabla(fundo);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_variedad ON phl_pt_all_tabla(variedad);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_estado ON phl_pt_all_tabla(estado);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_created_at ON phl_pt_all_tabla(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla phl_pt_all_tabla e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla phl_pt_all_tabla: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_phl_pt_all_tabla_to_postgresql(df):
    """Insertar el dataframe de phl_pt_all_tabla en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
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
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO phl_pt_all_tabla ({', '.join(columns_list)})
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_presentaciones_table():
    """Crear la tabla presentaciones si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe presentaciones
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS presentaciones (
            id SERIAL PRIMARY KEY,
            descripcion_producto VARCHAR(255) UNIQUE NOT NULL,
            peso_caja DECIMAL(10, 3),
            sobre_peso DECIMAL(10, 3),
            esquinero_adicionales INTEGER,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_descripcion ON presentaciones(descripcion_producto);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_peso_caja ON presentaciones(peso_caja);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_created_at ON presentaciones(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla presentaciones e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla presentaciones: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_presentaciones_to_postgresql(df):
    """Insertar el dataframe de presentaciones en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'DESCRIPCION DE PRODUCTO': 'descripcion_producto',
            'PESO caja': 'peso_caja',
            'SOBRE PESO': 'sobre_peso',
            'ESQUINEROS ADIONALES': 'esquinero_adicionales'
        }
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO presentaciones ({', '.join(columns_list)})
        VALUES ({placeholders})
        ON CONFLICT (descripcion_producto) 
        DO UPDATE SET 
            peso_caja = EXCLUDED.peso_caja,
            sobre_peso = EXCLUDED.sobre_peso,
            esquinero_adicionales = EXCLUDED.esquinero_adicionales,
            updated_at = CURRENT_TIMESTAMP
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
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
        logger.error(f"❌ Formato de fecha inválido: {target_date}. Use formato YYYY-MM-DD")
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
            logger.info(f"ℹ️ No se encontraron registros para la fecha {target_date}")
            cursor.close()
            connection.close()
            return True
        
        # Mostrar confirmación
        logger.warning(f"⚠️ Se van a eliminar {records_to_delete} registros de la fecha {target_date}")
        
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
        
        logger.info(f"✅ Se eliminaron {deleted_rows} registros de la fecha {target_date}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error al limpiar registros de {target_date}: {e}")
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
        logger.error(f"❌ Formato de fecha inválido: {e}. Use formato YYYY-MM-DD")
        return False
    
    # Verificar que start_date <= end_date
    if start_date > end_date:
        logger.error(f"❌ La fecha de inicio ({start_date}) debe ser menor o igual a la fecha de fin ({end_date})")
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
            logger.info(f"ℹ️ No se encontraron registros para el rango de fechas {date_range}")
            cursor.close()
            connection.close()
            return True
        
        # Mostrar confirmación
        date_range = start_date if start_date == end_date else f"{start_date} a {end_date}"
        logger.warning(f"⚠️ Se van a eliminar {records_to_delete} registros del rango {date_range}")
        
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
        
        logger.info(f"✅ Se eliminaron {deleted_rows} registros del rango {date_range}")
        return True
        
    except Exception as e:
        date_range = start_date if start_date == end_date else f"{start_date} a {end_date}"
        logger.error(f"❌ Error al limpiar registros del rango {date_range}: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def create_reporte_produccion_table():
    """Crear la tabla reporte_produccion si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del reporte de producción
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS reporte_produccion (
            id SERIAL PRIMARY KEY,
            semana VARCHAR(50),
            fecha_cosecha DATE,
            fecha_proceso DATE,
            turno_proceso VARCHAR(100),
            empresa VARCHAR(255),
            tipo VARCHAR(100),
            fundo VARCHAR(255),
            variedad VARCHAR(255),
            kg_procesados DECIMAL(15, 3),
            kg_descarte DECIMAL(15, 3),
            pct_descarte DECIMAL(8, 3),
            kg_sobre_peso DECIMAL(15, 3),
            pct_sobre_peso DECIMAL(8, 3),
            kg_merma DECIMAL(15, 3),
            pct_merma DECIMAL(8, 3),
            pct_rendimiento_mp DECIMAL(8, 3),
            caja_44oz_san_lucar_china INTEGER,
            caja_125grs_berry_world_china INTEGER,
            caja_44oz_berry_fresh_china INTEGER,
            caja_12x125gr_15kg_44oz INTEGER,
            caja_15kg_44oz_aereo INTEGER,
            caja_15kg_44oz_amoria INTEGER,
            caja_12x7oz_private_selection INTEGER,
            caja_3kg_98oz_5x7_china INTEGER,
            caja_98oz_generico_334kg INTEGER,
            caja_380kg_12x1pt_gnco INTEGER,
            caja_374kg_98pt_san_lucar_maritimo INTEGER,
            caja_3324kg_98oz_12x1pt_gen_aereo INTEGER,
            caja_3324kg_98oz_12x1pt_san_lucar_aereo INTEGER,
            caja_380kg_12x1pt_berry_fresh INTEGER,
            caja_408kg_8x18oz_san_lucar INTEGER,
            caja_408kg_8x18oz_gnco2 INTEGER,
            caja_408kg_8x18oz_bf INTEGER,
            caja_8x18oz_ce_boo_berries INTEGER,
            caja_612kg_12x18oz_bf INTEGER,
            caja_612kg_12x18oz_gen INTEGER,
            caja_612kg_12x18oz_premium INTEGER,
            caja_204kg_6oz_generico_org INTEGER,
            caja_204kg_6oz_maritimo INTEGER,
            caja_204kg_6oz_san_lucar INTEGER,
            caja_204kg_6oz_bf INTEGER,
            caja_30kg_granel_sal_lucar INTEGER,
            caja_30kg_granel_aereo INTEGER,
            caja_340kg_canastilla_aereo INTEGER,
            caja_340kg_canastilla INTEGER,
            caja_30kg_granel INTEGER,
            caja_33kg_canastilla_sl_m INTEGER,
            n_bandejas_nacional INTEGER,
            formato INTEGER,
            kg_nacionales DECIMAL(15, 3),
            kg_procesados2 DECIMAL(15, 3),
            kg_exportables DECIMAL(15, 3),
            kg_fruta_jumbo DECIMAL(15, 3),
            pct_fruta_jumbo DECIMAL(8, 3),
            pct_fruta_convencional DECIMAL(8, 3),
            pct_kg_exportables DECIMAL(8, 3),
            pct_kg_nacionales DECIMAL(8, 3),
            descarte_por_polvo_kg DECIMAL(15, 3),
            descarte_campo_kg DECIMAL(15, 3),
            muestra_15kg_44oz INTEGER,
            muestra_12x7oz INTEGER,
            muestra_8x18_berry_fresh INTEGER,
            muestra_408kg_8x18oz_gen INTEGER,
            muestra_98oz_maritimo INTEGER,
            muestra_98oz_aereo INTEGER,
            muestra_61kg_44oz INTEGER,
            muestra_204kg_6oz INTEGER,
            muestra_30kg_bulk INTEGER,
            muestra_12x1pt INTEGER,
            muestra_33kg_canastilla_sl_m INTEGER,
            kg_de_muestra DECIMAL(15, 3),
            total_cajas_exportadas INTEGER,
            total_cajas_exportadas_muestras INTEGER,
            h_recepcion DECIMAL(8, 2),
            h_pesado_mp DECIMAL(8, 2),
            h_abastecimiento DECIMAL(8, 2),
            h_retiro_jarras_jabas DECIMAL(8, 2),
            h_industrial DECIMAL(8, 2),
            h_pesado DECIMAL(8, 2),
            h_encajado DECIMAL(8, 2),
            h_abastecedor_clamshells DECIMAL(8, 2),
            h_etiquetado DECIMAL(8, 2),
            h_paletizado DECIMAL(8, 2),
            h_enzunchado DECIMAL(8, 2),
            h_montacarguista DECIMAL(8, 2),
            h_control_empaque DECIMAL(8, 2),
            h_pizarra DECIMAL(8, 2),
            h_almacen DECIMAL(8, 2),
            h_trazabilidad DECIMAL(8, 2),
            h_habilitacion_material DECIMAL(8, 2),
            h_limpieza DECIMAL(8, 2),
            h_aux_producc DECIMAL(8, 2),
            h_apoyo_otras_areas DECIMAL(8, 2),
            h_inicio TIME,
            h_final TIME,
            refrigerio DECIMAL(8, 2),
            paradas DECIMAL(8, 2),
            total_horas_laboradas DECIMAL(8, 2),
            horas_efectivas DECIMAL(8, 2),
            n_operarios INTEGER,
            hr_hombre_totales DECIMAL(8, 2),
            hr_hombre_efectivas DECIMAL(8, 2),
            productividad_1_kg_hr_oper DECIMAL(8, 3),
            productividad_pesado DECIMAL(8, 3),
            cajas_hora DECIMAL(8, 3),
            objetivo_kg_hr_pesador DECIMAL(8, 3),
            kg_44oz DECIMAL(15, 3),
            kg_12x7oz DECIMAL(15, 3),
            kg_204kg_6oz DECIMAL(15, 3),
            kg_98oz_pinta_plana DECIMAL(15, 3),
            kg_38kg_pinta DECIMAL(15, 3),
            kg_granel DECIMAL(15, 3),
            kg_408kg_8x18oz DECIMAL(15, 3),
            kg_612kg_12x18oz DECIMAL(15, 3),
            observacion TEXT,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_reporte_produccion_semana ON reporte_produccion(semana);",
            "CREATE INDEX IF NOT EXISTS idx_reporte_produccion_fecha_proceso ON reporte_produccion(fecha_proceso);",
            "CREATE INDEX IF NOT EXISTS idx_reporte_produccion_empresa ON reporte_produccion(empresa);",
            "CREATE INDEX IF NOT EXISTS idx_reporte_produccion_fundo ON reporte_produccion(fundo);",
            "CREATE INDEX IF NOT EXISTS idx_reporte_produccion_variedad ON reporte_produccion(variedad);",
            "CREATE INDEX IF NOT EXISTS idx_reporte_produccion_created_at ON reporte_produccion(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla reporte_produccion e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla reporte_produccion: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_reporte_produccion_to_postgresql(df):
    """Insertar el dataframe de reporte_produccion en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
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
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO reporte_produccion ({', '.join(columns_list)})
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
                                if 'fecha' in db_col:
                                    # Manejar fechas
                                    if isinstance(value, str):
                                        try:
                                            from datetime import datetime
                                            if '/' in value:
                                                value = datetime.strptime(value, '%Y-%m-%d').date()
                                            else:
                                                value = datetime.strptime(value, '%Y-%m-%d').date()
                                        except:
                                            values.append(None)
                                            continue
                                    values.append(value)
                                elif 'h_inicio' in db_col or 'h_final' in db_col:
                                    # Manejar horas
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
                                    # Manejar números decimales
                                    try:
                                        values.append(float(value) if value != 0 else 0.0)
                                    except:
                                        values.append(0.0)
                                elif 'caja_' in db_col or 'muestra_' in db_col or 'n_' in db_col or 'total_' in db_col or 'formato' in db_col:
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_presentaciones_table():
    """Crear la tabla presentaciones si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe presentaciones
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS presentaciones (
            id SERIAL PRIMARY KEY,
            descripcion_producto VARCHAR(255) UNIQUE NOT NULL,
            peso_caja DECIMAL(10, 3),
            sobre_peso DECIMAL(10, 3),
            esquinero_adicionales INTEGER,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_descripcion ON presentaciones(descripcion_producto);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_peso_caja ON presentaciones(peso_caja);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_created_at ON presentaciones(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla presentaciones e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla presentaciones: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_presentaciones_to_postgresql(df):
    """Insertar el dataframe de presentaciones en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'DESCRIPCION DE PRODUCTO': 'descripcion_producto',
            'PESO caja': 'peso_caja',
            'SOBRE PESO': 'sobre_peso',
            'ESQUINEROS ADIONALES': 'esquinero_adicionales'
        }
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO presentaciones ({', '.join(columns_list)})
        VALUES ({placeholders})
        ON CONFLICT (descripcion_producto) 
        DO UPDATE SET 
            peso_caja = EXCLUDED.peso_caja,
            sobre_peso = EXCLUDED.sobre_peso,
            esquinero_adicionales = EXCLUDED.esquinero_adicionales,
            updated_at = CURRENT_TIMESTAMP
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_phl_pt_all_tabla_table():
    """Crear la tabla phl_pt_all_tabla si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe phl_pt_all_tabla
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS phl_pt_all_tabla (
            id SERIAL PRIMARY KEY,
            envio VARCHAR(255),
            semana DECIMAL(10, 2),
            fecha_produccion TIMESTAMPTZ,
            fecha_cosecha TIMESTAMPTZ,
            cliente VARCHAR(255),
            tipo_pallet VARCHAR(255),
            contenedor VARCHAR(255),
            descripcion_producto VARCHAR(255),
            destino VARCHAR(255),
            fundo VARCHAR(255),
            variedad VARCHAR(255),
            n_cajas DECIMAL(15, 3),
            n_pallet VARCHAR(255),
            turno DECIMAL(10, 2),
            linea DECIMAL(10, 2),
            phl_origen VARCHAR(255),
            materiales_adicionales VARCHAR(255),
            observaciones TEXT,
            sobre_peso INTEGER,
            peso_caja DECIMAL(15, 3),
            exportable DECIMAL(15, 3),
            estado VARCHAR(255),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_semana ON phl_pt_all_tabla(semana);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fecha_produccion ON phl_pt_all_tabla(fecha_produccion);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fecha_cosecha ON phl_pt_all_tabla(fecha_cosecha);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_cliente ON phl_pt_all_tabla(cliente);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_fundo ON phl_pt_all_tabla(fundo);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_variedad ON phl_pt_all_tabla(variedad);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_estado ON phl_pt_all_tabla(estado);",
            "CREATE INDEX IF NOT EXISTS idx_phl_pt_created_at ON phl_pt_all_tabla(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla phl_pt_all_tabla e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla phl_pt_all_tabla: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_phl_pt_all_tabla_to_postgresql(df):
    """Insertar el dataframe de phl_pt_all_tabla en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
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
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO phl_pt_all_tabla ({', '.join(columns_list)})
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_presentaciones_table():
    """Crear la tabla presentaciones si no existe con la estructura del dataframe"""
    connection = create_database_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # SQL para crear la tabla con todas las columnas del dataframe presentaciones
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS presentaciones (
            id SERIAL PRIMARY KEY,
            descripcion_producto VARCHAR(255) UNIQUE NOT NULL,
            peso_caja DECIMAL(10, 3),
            sobre_peso DECIMAL(10, 3),
            esquinero_adicionales INTEGER,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Crear índices para mejorar rendimiento
        create_indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_descripcion ON presentaciones(descripcion_producto);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_peso_caja ON presentaciones(peso_caja);",
            "CREATE INDEX IF NOT EXISTS idx_presentaciones_created_at ON presentaciones(created_at);"
        ]
        
        cursor.execute(create_table_sql)
        
        # Crear índices
        for index_sql in create_indexes_sql:
            try:
                cursor.execute(index_sql)
            except Exception as index_error:
                logger.warning(f"⚠️ Error creando índice: {index_error}")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("✅ Tabla presentaciones e índices creados/verificados exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al crear la tabla presentaciones: {e}")
        if connection:
            try:
                connection.rollback()
            except:
                pass
            connection.close()
        return False

def insert_presentaciones_to_postgresql(df):
    """Insertar el dataframe de presentaciones en PostgreSQL con manejo mejorado de errores y transacciones"""
    if df.empty:
        logger.info("ℹ️ DataFrame vacío, no hay datos para insertar")
        return True  # No es un error, simplemente no hay datos
    
    # Mostrar información del dataframe
    logger.info(f"📊 DataFrame a insertar: {len(df)} filas, {len(df.columns)} columnas")
    logger.info(f"📋 Columnas: {list(df.columns)}")
    
    connection = create_database_connection()
    if not connection:
        return False
    
    cursor = None
    try:
        # Configurar autocommit en False para manejo manual de transacciones
        connection.autocommit = False
        cursor = connection.cursor()
        
        # Mapeo de columnas del DataFrame a columnas de la base de datos
        column_mapping = {
            'DESCRIPCION DE PRODUCTO': 'descripcion_producto',
            'PESO caja': 'peso_caja',
            'SOBRE PESO': 'sobre_peso',
            'ESQUINEROS ADIONALES': 'esquinero_adicionales'
        }
        
        # Crear la consulta INSERT con todas las columnas
        columns_list = list(column_mapping.values())
        placeholders = ', '.join(['%s'] * len(columns_list))
        insert_sql = f"""
        INSERT INTO presentaciones ({', '.join(columns_list)})
        VALUES ({placeholders})
        ON CONFLICT (descripcion_producto) 
        DO UPDATE SET 
            peso_caja = EXCLUDED.peso_caja,
            sobre_peso = EXCLUDED.sobre_peso,
            esquinero_adicionales = EXCLUDED.esquinero_adicionales,
            updated_at = CURRENT_TIMESTAMP
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
                for idx, error in batch_errors:
                    logger.error(f"❌ Fila {idx}: {error[:150]}...")
            
            # Actualizar progreso
            total_processed = processed_records + error_records
            progress = min(total_processed / total_records, 1.0)
            logger.info(f"Procesando lote {i//batch_size + 1}: {total_processed}/{total_records} (✅ {processed_records}, ❌ {error_records})")
        
        # Procesamiento completado
        logger.info("Procesamiento completado")
        
        # Mostrar resultados finales
        if error_records == 0:
            logger.info(f"✅ Se procesaron {processed_records} de {total_records} registros exitosamente")
        else:
            logger.warning(f"⚠️ Se procesaron {processed_records} de {total_records} registros. {error_records} errores encontrados.")
        
        # Mostrar errores detallados si hay pocos
        if detailed_errors and len(detailed_errors) <= 10:
            logger.info("Errores detallados:")
            for error in detailed_errors:
                logger.error(error)
        elif len(detailed_errors) > 10:
            logger.info(f"Se encontraron {len(detailed_errors)} errores. Los primeros 10:")
            for error in detailed_errors[:10]:
                logger.error(error)
        
        return processed_records > 0
        
    except Exception as e:
        # Rollback en caso de error general
        if connection:
            connection.rollback()
        logger.error(f"❌ Error crítico al insertar datos: {str(e)}")
        logger.error("La transacción fue revertida.")
        return False
        
    finally:
        # Cerrar recursos
        if cursor:
            cursor.close()
        if connection:
            connection.close()