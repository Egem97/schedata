"""
Ejemplo de uso de las funciones para crear tabla e ingestar datos de presentaciones
"""

import streamlit as st
from data.transform.packing_transform import presentaciones_transform
from data.load.ingesta_bd import ingesta_presentaciones_bd
from utils.handler_bd import create_presentaciones_table, insert_presentaciones_to_postgresql
from utils.presentaciones_utils import clear_and_reload_presentaciones, clear_presentaciones_table, get_presentaciones_stats
from utils.get_token import get_access_token_packing

def main():
    st.title("Ingestión de Datos Presentaciones")
    
    # Obtener token de acceso
    access_token = get_access_token_packing()
    
    if not access_token:
        st.error("❌ No se pudo obtener el token de acceso")
        return
    
    st.success("✅ Token de acceso obtenido correctamente")
    
    # Crear tabla
    st.subheader("1. Crear Tabla")
    if st.button("Crear Tabla presentaciones"):
        if create_presentaciones_table():
            st.success("✅ Tabla creada exitosamente")
        else:
            st.error("❌ Error al crear la tabla")
    
    # Extraer y transformar datos
    st.subheader("2. Extraer y Transformar Datos")
    if st.button("Procesar Datos"):
        try:
            with st.spinner("Procesando datos..."):
                df = presentaciones_transform(access_token)
                
            st.success(f"✅ Datos procesados: {len(df)} filas, {len(df.columns)} columnas")
            
            # Mostrar información del dataframe
            st.subheader("Información del DataFrame")
            st.write(f"**Filas:** {len(df)}")
            st.write(f"**Columnas:** {len(df.columns)}")
            st.write("**Columnas:**", list(df.columns))
            
            # Mostrar primeras filas
            st.subheader("Primeras 10 filas")
            st.dataframe(df.head(10))
            
            # Mostrar tipos de datos
            st.subheader("Tipos de Datos")
            st.dataframe(df.dtypes.to_frame('Tipo'))
            
            # Mostrar estadísticas básicas
            st.subheader("Estadísticas Básicas")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Presentaciones", len(df))
            
            with col2:
                peso_promedio = df['PESO caja'].mean() if 'PESO caja' in df.columns else 0
                st.metric("Peso Promedio Caja", f"{peso_promedio:.2f} kg")
            
            with col3:
                sobre_peso_promedio = df['SOBRE PESO'].mean() if 'SOBRE PESO' in df.columns else 0
                st.metric("Sobre Peso Promedio", f"{sobre_peso_promedio:.2f}")
            
            # Guardar en session state para usar en la inserción
            st.session_state['presentaciones_df'] = df
            
        except Exception as e:
            st.error(f"❌ Error al procesar datos: {str(e)}")
    
    # Opciones de inserción
    st.subheader("3. Opciones de Inserción")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Inserción Simple") and 'presentaciones_df' in st.session_state:
            df = st.session_state['presentaciones_df']
            
            if insert_presentaciones_to_postgresql(df):
                st.success("✅ Datos insertados exitosamente")
            else:
                st.error("❌ Error al insertar los datos")
    
    with col2:
        if st.button("Recarga Segura") and 'presentaciones_df' in st.session_state:
            df = st.session_state['presentaciones_df']
            
            if clear_and_reload_presentaciones(df):
                st.success("✅ Datos recargados exitosamente")
            else:
                st.error("❌ Error al recargar los datos")
    
    with col3:
        if st.button("Ingesta Completa"):
            if ingesta_presentaciones_bd(access_token):
                st.success("✅ Ingesta completa exitosa")
            else:
                st.error("❌ Error en ingesta completa")
    
    # Opciones de limpieza
    st.subheader("4. Opciones de Limpieza")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Limpiar Tabla Completa"):
            if clear_presentaciones_table():
                st.success("✅ Tabla limpiada exitosamente")
            else:
                st.error("❌ Error al limpiar la tabla")
    
    with col2:
        if st.button("Ver Estadísticas"):
            stats = get_presentaciones_stats()
            if stats:
                st.subheader("Estadísticas de la Tabla")
                
                general = stats['general']
                if general:
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Presentaciones", general[0])
                    
                    with col2:
                        st.metric("Productos Únicos", general[1])
                    
                    with col3:
                        st.metric("Peso Promedio", f"{general[2]:.2f} kg" if general[2] else "N/A")
                    
                    with col4:
                        st.metric("Sobre Peso Promedio", f"{general[3]:.2f}" if general[3] else "N/A")
                
                # Mostrar top productos por peso
                if stats['top_peso']:
                    st.subheader("Top 10 Productos por Peso")
                    top_df = pd.DataFrame(stats['top_peso'], columns=[
                        'Descripción Producto', 'Peso Caja', 'Sobre Peso', 'Esquineros Adicionales'
                    ])
                    st.dataframe(top_df)
            else:
                st.error("❌ Error al obtener estadísticas")
    
    # Mostrar datos si están disponibles
    if 'presentaciones_df' in st.session_state:
        st.subheader("Datos Disponibles")
        df = st.session_state['presentaciones_df']
        
        # Filtros
        col1, col2 = st.columns(2)
        
        with col1:
            # Filtro por peso de caja
            peso_min = st.number_input("Peso mínimo de caja", min_value=0.0, value=0.0, step=0.1)
            peso_max = st.number_input("Peso máximo de caja", min_value=0.0, value=10.0, step=0.1)
        
        with col2:
            # Filtro por sobre peso
            sobre_peso_min = st.number_input("Sobre peso mínimo", min_value=0.0, value=0.0, step=0.01)
            sobre_peso_max = st.number_input("Sobre peso máximo", min_value=0.0, value=2.0, step=0.01)
        
        # Aplicar filtros
        df_filtrado = df.copy()
        
        if 'PESO caja' in df.columns:
            df_filtrado = df_filtrado[
                (df_filtrado['PESO caja'] >= peso_min) & 
                (df_filtrado['PESO caja'] <= peso_max)
            ]
        
        if 'SOBRE PESO' in df.columns:
            df_filtrado = df_filtrado[
                (df_filtrado['SOBRE PESO'] >= sobre_peso_min) & 
                (df_filtrado['SOBRE PESO'] <= sobre_peso_max)
            ]
        
        st.write(f"**Registros filtrados:** {len(df_filtrado)}")
        st.dataframe(df_filtrado)

if __name__ == "__main__":
    main()
