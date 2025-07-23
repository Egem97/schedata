import streamlit as st

def styles(pt = 3):
    st.markdown("""
        <style>
               .block-container {
                    padding-top: %srem;
                    padding-bottom: 0rem;
                    padding-left: 2.5rem;
                    padding-right: 2.5rem;
                }
                [data-testid="stVerticalBlockBorderWrapper"]{
                    padding: 1px;
                }
                [data-testid="stHeader"]{
                    height: 2.5rem;
                }
                div[data-baseweb="select"] span {
                    font-size: 11px !important; /* Cambia el tamaño de la fuente */
                }
                
                
        </style>
    """%(pt), unsafe_allow_html=True)