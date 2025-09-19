"""
Ejemplo de uso de las funciones para crear tabla e ingestar datos de phl_pt_all_tabla
"""

import streamlit as st
from data.transform.packing_transform import phl_pt_all_tabla_transform
from data.load.ingesta_bd import ingesta_phl_pt_all_tabla_bd
from utils.handler_bd import create_phl_pt_all_tabla_table, insert_phl_pt_all_tabla_to_postgresql
from utils.phl_pt_all_tabla_utils import clear_and_reload_phl_pt_all_tabla, clear_phl_pt_all_tabla_table
from utils.get_token import get_access_token_packing

def main():
    st.title("Ingestión de Datos PHL PT All Tabla")
    
    # Obtener token de acceso
    access_token = get_access_token_packing()
    
    if not access_token:
        st.error("❌ No se pudo obtener el token de acceso")
        return
    
    st.success("✅ Token de acceso obtenido correctamente")
    
    # Crear tabla
    st.subheader("1. Crear Tabla")
    if st.button("Crear Tabla phl_pt_all_tabla"):
        if create_phl_pt_all_tabla_table():
            st.success("✅ Tabla creada exitosamente")
        else:
            st.error("❌ Error al crear la tabla")
    
    # Extraer y transformar datos
    st.subheader("2. Extraer y Transformar Datos")
    if st.button("Procesar Datos"):
        try:
            with st.spinner("Procesando datos..."):
                df = phl_pt_all_tabla_transform(access_token)
                
            st.success(f"✅ Datos procesados: {len(df)} filas, {len(df.columns)} columnas")
            
            # Mostrar información del dataframe
            st.subheader("Información del DataFrame")
            st.write(f"**Filas:** {len(df)}")
            st.write(f"**Columnas:** {len(df.columns)}")
            st.write("**Columnas:**", list(df.columns))
            
            # Mostrar primeras filas
            st.subheader("Primeras 5 filas")
            st.dataframe(df.head())
            
            # Mostrar tipos de datos
            st.subheader("Tipos de Datos")
            st.dataframe(df.dtypes.to_frame('Tipo'))
            
            # Guardar en session state para usar en la inserción
            st.session_state['phl_pt_df'] = df
            
        except Exception as e:
            st.error(f"❌ Error al procesar datos: {str(e)}")
    
    # Opciones de inserción
    st.subheader("3. Opciones de Inserción")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Inserción Simple") and 'phl_pt_df' in st.session_state:
            df = st.session_state['phl_pt_df']
            
            if insert_phl_pt_all_tabla_to_postgresql(df):
                st.success("✅ Datos insertados exitosamente")
            else:
                st.error("❌ Error al insertar los datos")
    
    with col2:
        if st.button("Recarga Segura") and 'phl_pt_df' in st.session_state:
            df = st.session_state['phl_pt_df']
            
            if clear_and_reload_phl_pt_all_tabla(df):
                st.success("✅ Datos recargados exitosamente")
            else:
                st.error("❌ Error al recargar los datos")
    
    with col3:
        if st.button("Ingesta Completa"):
            if ingesta_phl_pt_all_tabla_bd(access_token):
                st.success("✅ Ingesta completa exitosa")
            else:
                st.error("❌ Error en ingesta completa")
    
    # Opciones de limpieza
    st.subheader("4. Opciones de Limpieza")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Limpiar Tabla Completa"):
            if clear_phl_pt_all_tabla_table():
                st.success("✅ Tabla limpiada exitosamente")
            else:
                st.error("❌ Error al limpiar la tabla")
    
    with col2:
        if st.button("Crear/Verificar Tabla"):
            if create_phl_pt_all_tabla_table():
                st.success("✅ Tabla creada/verificada exitosamente")
            else:
                st.error("❌ Error al crear/verificar la tabla")
    
    # Mostrar datos si están disponibles
    if 'phl_pt_df' in st.session_state:
        st.subheader("Datos Disponibles")
        df = st.session_state['phl_pt_df']
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        
        with col1:
            clientes = ['Todos'] + list(df['CLIENTE'].unique())
            cliente_seleccionado = st.selectbox("Cliente", clientes)
        
        with col2:
            fundos = ['Todos'] + list(df['FUNDO'].unique())
            fundo_seleccionado = st.selectbox("Fundo", fundos)
        
        with col3:
            variedades = ['Todos'] + list(df['VARIEDAD'].unique())
            variedad_seleccionada = st.selectbox("Variedad", variedades)
        
        # Aplicar filtros
        df_filtrado = df.copy()
        
        if cliente_seleccionado != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['CLIENTE'] == cliente_seleccionado]
        
        if fundo_seleccionado != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['FUNDO'] == fundo_seleccionado]
        
        if variedad_seleccionada != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['VARIEDAD'] == variedad_seleccionada]
        
        st.write(f"**Registros filtrados:** {len(df_filtrado)}")
        st.dataframe(df_filtrado)

if __name__ == "__main__":
    main()
