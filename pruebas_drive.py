import pandas as pd
import streamlit as st
from utils.get_sheets import (
    extract_all_data_with_urls, 
    extract_all_data_with_public_urls,
    list_folders, 
    list_images_in_folder, 
    authenticate_google_drive, 
    make_file_public,
    FOLDER_ID
)

def main():
    st.set_page_config(
        page_title="Pruebas Google Drive - URLs",
        page_icon="🖼️",
        layout="wide"
    )
    
    st.title("🖼️ Pruebas Google Drive - URLs de Descarga")
    st.markdown("---")
    
    # Sidebar para opciones
    st.sidebar.header("Opciones")
    
    # Botón para extraer datos con URLs normales
    if st.sidebar.button("🚀 Extraer Datos con URLs", type="primary"):
        with st.spinner("Extrayendo datos de Google Drive..."):
            try:
                # Extraer datos con URLs
                df = extract_all_data_with_urls()
                
                if df is not None and not df.empty:
                    st.success(f"✅ Extracción completada: {len(df)} filas encontradas")
                    
                    # Mostrar estadísticas
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total de filas", len(df))
                    with col2:
                        st.metric("Imágenes con URLs", len(df[df['image_download_url'].notna()]))
                    with col3:
                        st.metric("Carpetas procesadas", df['folder_name'].nunique())
                    
                    # Mostrar DataFrame
                    st.subheader("📊 Datos Extraídos")
                    st.dataframe(df, use_container_width=True)
                    
                    # Mostrar URLs de descarga
                    st.subheader("🔗 URLs de Descarga")
                    urls_df = df[df['image_download_url'].notna()][['image_name', 'image_download_url', 'image_thumbnail_url']]
                    st.dataframe(urls_df, use_container_width=True)
                    
                    # Botón para descargar CSV
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Descargar CSV",
                        data=csv,
                        file_name="drive_images_urls.csv",
                        mime="text/csv"
                    )
                    
                else:
                    st.error("❌ No se pudieron extraer datos")
                    
            except Exception as e:
                st.error(f"❌ Error durante la extracción: {str(e)}")
    
    # Botón para extraer datos con URLs públicas
    st.sidebar.markdown("---")
    st.sidebar.subheader("URLs Públicas")
    
    make_public = st.sidebar.checkbox("🔓 Hacer archivos públicos automáticamente", value=False)
    
    if st.sidebar.button("🌐 Extraer Datos con URLs Públicas", type="secondary"):
        with st.spinner("Extrayendo datos con URLs públicas..."):
            try:
                # Extraer datos con URLs públicas
                df = extract_all_data_with_public_urls(make_public=make_public)
                
                if df is not None and not df.empty:
                    st.success(f"✅ Extracción completada: {len(df)} filas encontradas")
                    
                    # Mostrar estadísticas
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total de filas", len(df))
                    with col2:
                        st.metric("Imágenes con URLs", len(df[df['image_download_url'].notna()]))
                    with col3:
                        st.metric("URLs públicas", len(df[df['image_public_download_url'].notna()]))
                    with col4:
                        st.metric("Carpetas procesadas", df['folder_name'].nunique())
                    
                    # Mostrar DataFrame
                    st.subheader("📊 Datos Extraídos con URLs Públicas")
                    st.dataframe(df, use_container_width=True)
                    
                    # Mostrar URLs públicas de descarga
                    st.subheader("🌐 URLs Públicas de Descarga")
                    public_urls_df = df[df['image_public_download_url'].notna()][
                        ['image_name', 'image_public_download_url', 'image_public_thumbnail_url', 'image_web_content_url']
                    ]
                    st.dataframe(public_urls_df, use_container_width=True)
                    
                    # Mostrar URLs normales vs públicas
                    st.subheader("🔗 Comparación URLs Normales vs Públicas")
                    comparison_df = df[df['image_download_url'].notna()][
                        ['image_name', 'image_download_url', 'image_public_download_url', 'image_thumbnail_url', 'image_public_thumbnail_url']
                    ]
                    st.dataframe(comparison_df, use_container_width=True)
                    
                    # Botón para descargar CSV
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Descargar CSV con URLs Públicas",
                        data=csv,
                        file_name="drive_images_public_urls.csv",
                        mime="text/csv"
                    )
                    
                else:
                    st.error("❌ No se pudieron extraer datos")
                    
            except Exception as e:
                st.error(f"❌ Error durante la extracción: {str(e)}")
    
    # Botón para hacer público un archivo específico
    st.sidebar.markdown("---")
    st.sidebar.subheader("Hacer Archivo Público")
    
    file_id = st.sidebar.text_input("ID del archivo a hacer público", value="")
    
    if st.sidebar.button("🔓 Hacer Público") and file_id:
        with st.spinner("Haciendo archivo público..."):
            try:
                service = authenticate_google_drive()
                if service:
                    success = make_file_public(service, file_id)
                    if success:
                        st.success(f"✅ Archivo {file_id} hecho público")
                        
                        # Generar URLs públicas
                        from utils.get_sheets import get_public_download_url, get_public_thumbnail_url, get_web_content_url
                        
                        public_download = get_public_download_url(service, file_id)
                        public_thumbnail = get_public_thumbnail_url(service, file_id)
                        web_content = get_web_content_url(service, file_id)
                        
                        st.subheader("🔗 URLs Públicas Generadas")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.write("**Descarga Directa:**")
                            st.code(public_download)
                        with col2:
                            st.write("**Miniatura:**")
                            st.code(public_thumbnail)
                        with col3:
                            st.write("**Vista Web:**")
                            st.code(web_content)
                    else:
                        st.error("❌ No se pudo hacer público el archivo")
                else:
                    st.error("❌ Error de autenticación")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # Botón para listar solo carpetas
    if st.sidebar.button("📁 Listar Carpetas"):
        with st.spinner("Obteniendo carpetas..."):
            try:
                service = authenticate_google_drive()
                if service:
                    folders = list_folders(service, FOLDER_ID)
                    if folders:
                        st.success(f"✅ Se encontraron {len(folders)} carpetas")
                        folders_df = pd.DataFrame(folders)
                        st.dataframe(folders_df, use_container_width=True)
                    else:
                        st.warning("⚠️ No se encontraron carpetas")
                else:
                    st.error("❌ Error de autenticación")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # Botón para listar imágenes de una carpeta específica
    st.sidebar.markdown("---")
    st.sidebar.subheader("Listar Imágenes por Carpeta")
    
    # Input para ID de carpeta
    folder_id = st.sidebar.text_input("ID de Carpeta", value="")
    
    if st.sidebar.button("🖼️ Listar Imágenes") and folder_id:
        with st.spinner("Obteniendo imágenes..."):
            try:
                service = authenticate_google_drive()
                if service:
                    images = list_images_in_folder(service, folder_id)
                    if images:
                        st.success(f"✅ Se encontraron {len(images)} imágenes")
                        images_df = pd.DataFrame(images)
                        st.dataframe(images_df, use_container_width=True)
                    else:
                        st.warning("⚠️ No se encontraron imágenes en esta carpeta")
                else:
                    st.error("❌ Error de autenticación")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
    
    # Información adicional
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    ### 📋 Información
    
    **Funciones disponibles:**
    - `extract_all_data_with_urls()`: Extrae datos con URLs normales
    - `extract_all_data_with_public_urls()`: Extrae datos con URLs públicas
    - `make_file_public()`: Hace un archivo público
    - `get_public_download_url()`: Genera URL de descarga pública
    - `get_public_thumbnail_url()`: Genera URL de miniatura pública
    
    **Columnas del DataFrame:**
    - `folder_id`: ID de la carpeta
    - `folder_name`: Nombre de la carpeta
    - `image_id`: ID de la imagen
    - `image_name`: Nombre de la imagen
    - `image_download_url`: URL de descarga directa (requiere auth)
    - `image_thumbnail_url`: URL de miniatura (requiere auth)
    - `image_public_download_url`: URL de descarga pública
    - `image_public_thumbnail_url`: URL de miniatura pública
    - `image_web_content_url`: URL de vista web
    - `image_size_mb`: Tamaño en MB
    """)

if __name__ == "__main__":
    main()
