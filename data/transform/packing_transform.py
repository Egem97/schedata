import pandas as pd
import logging
from data.extract.packing_extract import *
from utils.helpers import corregir_hora_tarde, convert_mixed_dates,transform_kg_text_rp_packing
from utils.suppress_warnings import setup_pandas_warnings

setup_pandas_warnings()


logger = logging.getLogger(__name__)

def recepcion_tiempos_packing_transform():
    df = recepcion_extract()
    df["PESO NETO CAMPO"] = df["PESO NETO CAMPO"].str.replace(",", ".", regex=False).astype(float)
    df["KILOS BRUTO"] = df["KILOS BRUTO"].str.replace(",", ".", regex=False).astype(float)
    df["KILOS NETO"] = df["KILOS NETO"].str.replace(",", ".", regex=False).astype(float)
    df["N° JABAS"] = df["N° JABAS"].replace('',0)
    df["N° JABAS"] = df["N° JABAS"].astype(float)
    df["N° JARRAS"] = df["N° JARRAS"].replace('','0')
    df["N° JARRAS"] = df["N° JARRAS"].str.replace(",", ".", regex=False).astype(float)
    df["PESO PROMEDIO JARRA"] = df["PESO PROMEDIO JARRA"].replace('',"0")
    df["PESO PROMEDIO JARRA"] = df["PESO PROMEDIO JARRA"].str.replace(",", ".", regex=False).astype(float)
    df["TEMPERATURA"] = df["TEMPERATURA"].str.replace(",", ".", regex=False).astype(float)
    df["PESO PROMEDIO JABA"] = df["PESO PROMEDIO JABA"].replace('',"0")
    df["PESO PROMEDIO JABA"] = df["PESO PROMEDIO JABA"].str.replace(",", ".", regex=False).astype(float)
    df["DIF"] = df["DIF"].str.replace(",", ".", regex=False).astype(float)
    df["TRASLADO"] = df["TRASLADO"].str.replace(",", ".", regex=False).astype(float)
    df["PESO PALLET"] = df["PESO PALLET"].replace('',"0")
    df["PESO PALLET"] = df["PESO PALLET"].astype(float)
    var_category = ['CODIGO QR','EMPRESA','TIPO PRODUCTO','FUNDO', 'VARIEDAD', 'N° PALLET',  'PLACA','N° TARJETA PALLET','GUIA']
    var_numeric = ["KILOS BRUTO","KILOS NETO","PESO NETO CAMPO","N° JABAS","N° JARRAS"]
    df = df[df['FECHA RECEPCION'].notna()]
    df['FECHA RECEPCION'] = pd.to_datetime(df['FECHA RECEPCION'], dayfirst=True).dt.strftime('%Y-%m-%d')
    df['FECHA SALIDA CAMPO'] = pd.to_datetime(df['FECHA SALIDA CAMPO'], dayfirst=True).dt.strftime('%Y-%m-%d')
    df['N° VIAJE'] = df['N° VIAJE'].astype(str)
    df['T° ESTADO'] = df['T° ESTADO'].fillna("-")
    df[var_category] = df[var_category].fillna("-")
    df[var_numeric] = df[var_numeric].fillna(0)
    df["GUIA CONSOLIDADA"] = df["GUIA CONSOLIDADA"].fillna("-")

    if df["FECHA RECEPCION"].dtype == 'object':
        df["FECHA RECEPCION"] = pd.to_datetime(df["FECHA RECEPCION"], errors='coerce')
    if df["HORA RECEPCION"].dtype == 'object' or 'time' in str(df["HORA RECEPCION"].dtype):
        df["HORA RECEPCION"] = df["HORA RECEPCION"].astype(str).str.extract(r'(\d{2}:\d{2}:\d{2})')[0]
    elif pd.api.types.is_datetime64_any_dtype(df["HORA RECEPCION"]):
        df["HORA RECEPCION"] = df["HORA RECEPCION"].dt.strftime('%H:%M:%S')

    if "HORA RECEPCION" in df.columns:
        df["HORA RECEPCION"] = df["HORA RECEPCION"].apply(corregir_hora_tarde)

    df = df.groupby(["FECHA RECEPCION","HORA RECEPCION","N° PALLET","CODIGO QR"])[["KILOS BRUTO"]].sum().reset_index()
    df = df[df["FECHA RECEPCION"] >= "2025-07-10"]
    df = df.rename(columns={"CODIGO QR":"QR"})
    df = df.drop(columns=["KILOS BRUTO"])
    logger.info(f"✅ Datos de Recepcion procesados: {len(df)} filas")
    return df

def enfriamiento_tiempos_packing_transform():
    df = enfriamiento_extract()
    df['FECHA'] = pd.to_datetime(df['FECHA'], dayfirst=True).dt.strftime('%Y-%m-%d')
    df["HORA INICIAL"] = df["HORA INICIAL"].fillna("-").astype(str)
    df["HORA FINAL"] = df["HORA FINAL"].fillna("-").astype(str)
    df = df.groupby(["FECHA","HORA INICIAL","QR","HORA FINAL"])[["FORMATO"]].count().reset_index()
    df = df[df["FECHA"] >= "2025-07-10"]
    df = df.rename(columns={
            "FECHA": "FECHA ENFRIAMIENTO",
            "HORA FINAL": "HORA FINAL ENFRIAMIENTO",
            "HORA INICIAL": "HORA INICIAL ENFRIAMIENTO"
    })
    df = df.drop(columns=["FORMATO"])
    #df["HORA INICIAL ENFRIAMIENTO"] = pd.to_datetime(df["HORA INICIAL ENFRIAMIENTO"], format='%H:%M', errors='coerce').dt.strftime('%H:%M:%S')
    #df["HORA FINAL ENFRIAMIENTO"] = pd.to_datetime(df["HORA FINAL ENFRIAMIENTO"], format='%H:%M', errors='coerce').dt.strftime('%H:%M:%S')
    logger.info(f"✅ Datos de Recepcion Enfriamiento: {len(df)} filas")
    return df

def volcado_tiempos_packing_transform():
    df = volcado_extract()
    df['FECHA DE COSECHA'] = pd.to_datetime(df['FECHA DE COSECHA'], dayfirst=True).dt.strftime('%Y-%m-%d')
    df['FECHA DE PROCESO'] = pd.to_datetime(df['FECHA DE PROCESO'], dayfirst=True).dt.strftime('%Y-%m-%d')
    df["PESO NETO"] = df["PESO NETO"].str.replace(",", ".", regex=False).astype(float)
    if df["FECHA DE PROCESO"].dtype == 'object':
        df["FECHA DE PROCESO"] = pd.to_datetime(df["FECHA DE PROCESO"], errors='coerce')
    df = df.groupby(["FECHA DE PROCESO","HORA INICIO","HORA FINAL","QR","PROVEEDOR","FORMATO"])[["TIPO DE PRODUCTO"]].count().reset_index()
    df = df[df["FECHA DE PROCESO"] >= "2025-07-10"]
    df["QR"] = df["QR"].str.strip()
    df["HORA INICIO"] = pd.to_datetime(df["HORA INICIO"], format='%H:%M', errors='coerce').dt.strftime('%H:%M:%S')
    df["HORA FINAL"] = pd.to_datetime(df["HORA FINAL"], format='%H:%M', errors='coerce').dt.strftime('%H:%M:%S')
    df = df.rename(columns={"HORA INICIO": "HORA INICIO PROCESO","HORA FINAL": "HORA FINAL PROCESO"})
    df = df.drop(columns=["TIPO DE PRODUCTO"])
    df["QR"] = df["QR"].fillna("N/A")

    #df = df[df["HORA FINAL PROCESO"].notna()]
    
    logger.info(f"✅ Datos de Clean Tiempos: {len(df)} filas")
    return df

def tiempos_packing_data_transform():
    logger.info(f"✅ Procesando Datos")
    
    df = pd.merge(recepcion_tiempos_packing_transform(),enfriamiento_tiempos_packing_transform(),on="QR",how="left")
    df = pd.merge(df,volcado_tiempos_packing_transform(),on="QR",how="left")
    df["FECHA RECEPCION"] = df["FECHA RECEPCION"].dt.date
    df["FECHA DE PROCESO"] = df["FECHA DE PROCESO"].dt.date
    
    logger.info(f"✅ Datos procesados: {len(df)} filas")
    
    return df






def volcado_bm_transform():
    logger.info(f"✅ Procesando Datos")
    df = volcado_extract()
    #df["PESO NETO"] = df["PESO NETO"].str.replace("", "0", regex=False)
    df["PESO NETO"] = df["PESO NETO"].str.replace(",", ".", regex=False).astype(float)
    df['FECHA DE COSECHA'] = pd.to_datetime(df['FECHA DE COSECHA'], dayfirst=True).dt.strftime('%Y-%m-%d')
    df['FECHA DE PROCESO'] = pd.to_datetime(df['FECHA DE PROCESO'], dayfirst=True).dt.strftime('%Y-%m-%d')
    df["VARIEDAD"] = df["VARIEDAD"].str.strip()
    df["TURNO DE PROCESO"] = df["TURNO DE PROCESO"].str.strip()
    df["PROVEEDOR"] = df["PROVEEDOR"].str.strip()
    df["TIPO DE PRODUCTO"] = df["TIPO DE PRODUCTO"].str.strip()
    df["FUNDO"] = df["FUNDO"].str.strip()
    df["PROVEEDOR"] = df["PROVEEDOR"].replace({"EXCELLENCE FRUIT SAC":"EXCELLENCE FRUIT S.A.C"})
    df = df.groupby(['SEMANA', 'FECHA DE COSECHA', 'FECHA DE PROCESO', 'TURNO DE PROCESO','PROVEEDOR',
            'TIPO DE PRODUCTO','FUNDO', 'VARIEDAD', ])[["PESO NETO"]].sum().reset_index() 
    df = df.rename(columns={"PESO NETO":"Kg Procesados","PROVEEDOR":"EMPRESA"})
    return df

def descarte_bm_transform():
    logger.info(f"✅ Procesando Datos")
    df = descarte_extract()
    df = df.rename(columns={"FECHA DE PROCESO ":"FECHA DE PROCESO"})
    df['FECHA DE COSECHA'] = pd.to_datetime(df['FECHA DE COSECHA'], dayfirst=True).dt.strftime('%Y-%m-%d')
    df['FECHA DE PROCESO'] = pd.to_datetime(df['FECHA DE PROCESO'], dayfirst=True).dt.strftime('%Y-%m-%d')
    
    df["VARIEDAD"] = df["VARIEDAD"].str.strip()
    df["EMPRESA"] = df["EMPRESA"].str.strip()
    df["FUNDO"] = df["FUNDO"].str.strip()
    df["KG DESCARTE"] = df["KG DESCARTE"].str.replace(",", ".", regex=False).astype(float)
    
    return df

def volcado_bm_descarte_transform():
    logger.info(f"✅ Procesando Datos")
    df = volcado_bm_transform()
    df_descarte = descarte_bm_transform()
    volcado_dff =pd.merge(
        df,df_descarte,
        on=['SEMANA', 'FECHA DE COSECHA', 'FECHA DE PROCESO','EMPRESA', 'FUNDO', 'VARIEDAD'],
        how="left"
    )
    volcado_dff["KG DESCARTE"] = volcado_dff["KG DESCARTE"].fillna(0)
    volcado_dff["FECHA DE COSECHA"] = pd.to_datetime(volcado_dff["FECHA DE COSECHA"])
    volcado_dff["FECHA DE PROCESO"] = pd.to_datetime(volcado_dff["FECHA DE PROCESO"])
    return volcado_dff

def producto_terminado_transform():
    logger.info(f"✅ Procesando Datos")
    df = producto_terminado_extract()
    df["Nº CAJAS"] = df["Nº CAJAS"].astype(float)
    df["TURNO"] = df["TURNO"].fillna("NO ESPECIFICADO")
    df["CLIENTE"] = df["CLIENTE"].fillna("NO ESPECIFICADO")
    df["DESCRIPCION DEL PRODUCTO"] = df["DESCRIPCION DEL PRODUCTO"].fillna("NO ESPECIFICADO")
    df["FUNDO"] = df["FUNDO"].fillna("NO ESPECIFICADO")
    df["VARIEDAD"] = df["VARIEDAD"].fillna("NO ESPECIFICADO")
    df["TURNO"] = df["TURNO"].replace("", "NO ESPECIFICADO")
    df["CLIENTE"] = df["CLIENTE"].replace("", "NO ESPECIFICADO")
    df["DESCRIPCION DEL PRODUCTO"] = df["DESCRIPCION DEL PRODUCTO"].replace("", "NO ESPECIFICADO")
    df["FUNDO"] = df["FUNDO"].replace("", "NO ESPECIFICADO")
    df["VARIEDAD"] = df["VARIEDAD"].replace("", "NO ESPECIFICADO")

    # Reemplazar guiones por barras
    df["F. COSECHA"] = df["F. COSECHA"].str.replace("-", "/")
    df["F. PRODUCCION"] = df["F. PRODUCCION"].str.replace("-", "/")

    # Convertir las fechas usando la función personalizada
    df["F. COSECHA"] = convert_mixed_dates(df["F. COSECHA"]).dt.strftime('%Y/%m/%d')
    df["F. PRODUCCION"] = convert_mixed_dates(df["F. PRODUCCION"]).dt.strftime('%Y/%m/%d')
    df["VARIEDAD"] = df["VARIEDAD"].str.strip()
    df["FUNDO"] = df["FUNDO"].str.strip()
    df["TURNO"] = df["TURNO"].str.strip()
    df["CLIENTE"] = df["CLIENTE"].str.strip()
    df["DESCRIPCION DEL PRODUCTO"] = df["DESCRIPCION DEL PRODUCTO"].str.strip()
    #list_productos = list(reg_dff["DESCRIPCION DEL PRODUCTO"].unique())
    df =df.groupby(["SEMANA", "F. COSECHA", "F. PRODUCCION", "TURNO","CLIENTE",#"CONTENEDOR",
            "DESCRIPCION DEL PRODUCTO","FUNDO", "VARIEDAD" ])[["Nº CAJAS"]].sum().reset_index() 
    return df

def producto_terminado_procesado_transform(agrupador,agrupadorcajas):
    logger.info(f"✅ Procesando Datos")
    pt_df = producto_terminado_transform()
    
    pt_df = pd.merge(pt_df,agrupador,left_on=["DESCRIPCION DEL PRODUCTO"],
            right_on=["PRESENTACIONES PRODUCTO TERMINADO"],
            how="left"
    )
    
    pt_df = pt_df.rename(columns={"AGRUPADOR REPORTE DE PRODUCCION":"PRESENTACIONES"})
    
    pt_df = pd.merge(pt_df,agrupadorcajas,on=["PRESENTACIONES"],how="left")
    return pt_df

####input el producto_terminado_procesado_transform

def pivot_cajas_presentacion_pt_transform(df,LISTA_PRESENTACIONES):
    
    lista_presentaciones_ = list(df["PRESENTACIONES"].unique())
    
    LISTA_PRESENTACIONES_FALTANTES = [presentacion for presentacion in LISTA_PRESENTACIONES if presentacion not in lista_presentaciones_]
    pt_join_df = df.pivot_table(
        index=['SEMANA', 'F. COSECHA', 'F. PRODUCCION', 'TURNO', 'CLIENTE','FUNDO', 'VARIEDAD',],
        columns="PRESENTACIONES",
        values="Nº CAJAS",
        aggfunc="sum",
        fill_value=0
    )
    pt_join_df = pt_join_df.reset_index()
    for presentacion in LISTA_PRESENTACIONES_FALTANTES:
        pt_join_df[presentacion] = 0

    return pt_join_df

def pivot_cajas_agrupador_pt_transform(df,LISTA_AGRUPADOR):
    lista_agrupadores_ = list(df["AGRUPADOR"].unique())
    LISTA_AGRUPADORES_FALTANTES = [agrupador for agrupador in LISTA_AGRUPADOR if agrupador not in lista_agrupadores_]
    
    pt_regjoin_dff_agrupador = df.pivot_table(
        index=['SEMANA', 'F. COSECHA', 'F. PRODUCCION', 'TURNO', 'CLIENTE','FUNDO', 'VARIEDAD',],
        columns="AGRUPADOR",
        values="Nº CAJAS",
        aggfunc="sum",
        fill_value=0
    )
    pt_regjoin_dff_agrupador = pt_regjoin_dff_agrupador.reset_index()
    for agrupadores in LISTA_AGRUPADORES_FALTANTES:
        pt_regjoin_dff_agrupador[agrupadores] = 0
    return pt_regjoin_dff_agrupador

def pivot_cajas_agrupador_kg_transform(df,LISTA_AGRUPADOR):
    df["KILOS AGRUPADOR"] = df["Nº CAJAS"] * df["SPD"] * df["KG"]
    dff = df.pivot_table(
        index=['SEMANA', 'F. COSECHA', 'F. PRODUCCION', 'TURNO', 'CLIENTE','FUNDO', 'VARIEDAD',],
        columns="AGRUPADOR",
        values="KILOS AGRUPADOR",
        aggfunc="sum",
        fill_value=0
    )
    dff = dff.reset_index()
    lista_agrupadores_ = list(df["AGRUPADOR"].unique())
    LISTA_AGRUPADORES_FALTANTES = [agrupador for agrupador in LISTA_AGRUPADOR if agrupador not in lista_agrupadores_]
    for agrupadores in LISTA_AGRUPADORES_FALTANTES:
        dff[agrupadores] = 0
    cols_to_rename3 = dff.columns.tolist()
    logger.info(f"🔍 Columnas disponibles: {cols_to_rename3}")
    
    if 'VARIEDAD' in cols_to_rename3:
        idx_variedad3 = cols_to_rename3.index('VARIEDAD')
        agrupadorKG_cols = cols_to_rename3[idx_variedad3+1:]
    else:
        logger.warning(f"⚠️ Columna 'VARIEDAD' no encontrada. Usando columnas desde posición 7")
        agrupadorKG_cols = cols_to_rename3[7:] if len(cols_to_rename3) > 7 else []
    rename_dict3 = {col: f"KG {col}" for col in agrupadorKG_cols}
    dff = dff.rename(columns=rename_dict3)
    return dff


def kg_exportables_transform(df):
    df["KILOS AGRUPADOR"] = df["Nº CAJAS"] * df["KG"]
   
    dff = df.pivot_table(   
        index=['SEMANA', 'F. COSECHA', 'F. PRODUCCION', 'TURNO', 'CLIENTE','FUNDO', 'VARIEDAD',],
        columns="AGRUPADOR",
        values="KILOS AGRUPADOR",
        aggfunc="sum",
        fill_value=0
    )
    dff = dff.reset_index()
    
    lista_agrupadores_ = list(df["AGRUPADOR"].unique())
    dff["Kg Exportables"] = dff[lista_agrupadores_].sum(axis=1)
    dff = dff.rename(
        columns={"F. COSECHA":"FECHA DE COSECHA","F. PRODUCCION":"FECHA DE PROCESO","CLIENTE":"EMPRESA","TURNO":"TURNO DE PROCESO"}
    )
    dff = dff.drop(columns=lista_agrupadores_)
    return dff


def joins_pt_transform():
    agrupador = pd.read_parquet(r"./src/storage/AGRUPADOR RP.parquet")
    nueva_fila_agrupador = pd.DataFrame({
        'PRESENTACIONES PRODUCTO TERMINADO':['125 GRS C/E BERRY FRESH+20MM-M','125 GRS C/E BERRY FRESH+22MM-M','125 GRS C/E BERRY FRESH+24MM-M','125 GRS C/E SAN LUCAR +20MM-M','125 GRS C/E SAN LUCAR +22MM-M','125 GRS C/E SAN LUCAR +24MM-M'],
        'AGRUPADOR REPORTE DE PRODUCCION':  ['4.4OZ C/E BERRY FRESH CHINA','4.4OZ C/E BERRY FRESH CHINA','4.4OZ C/E BERRY FRESH CHINA','CAJA 12x125 gr 1.5 KG (4.4 OZ)','CAJA 12x125 gr 1.5 KG (4.4 OZ)','CAJA 12x125 gr 1.5 KG (4.4 OZ)']
    
    })
    agrupador = pd.concat([agrupador,nueva_fila_agrupador],ignore_index=True)
    agrupadorcajas = pd.read_parquet(r"./src/storage/AGRUPADOR CAJAS.parquet")
    volcado_dff = volcado_bm_descarte_transform()
    LISTA_PRESENTACIONES = list(agrupadorcajas["PRESENTACIONES"].unique())
    LISTA_AGRUPADOR = list(agrupadorcajas["AGRUPADOR"].unique())

    pt_transform = producto_terminado_procesado_transform(agrupador,agrupadorcajas)
    
    
    pt_cajas_presentacion_df = pivot_cajas_presentacion_pt_transform(pt_transform,LISTA_PRESENTACIONES)
    pt_cajas_agrupador_df =pivot_cajas_agrupador_pt_transform(pt_transform,LISTA_AGRUPADOR)
    pt_cajas_agrupador_kg_df =pivot_cajas_agrupador_kg_transform(pt_transform,LISTA_AGRUPADOR)
    kg_exportables_df = kg_exportables_transform(pt_transform)
   
    list_presentacion = list(pt_cajas_presentacion_df.columns[7:])
    lista_agrupadores_ = list(pt_transform["AGRUPADOR"].unique())
    list_agrupador = list(pt_cajas_agrupador_df.columns[7:])
    list_agrupador_kg = list(pt_cajas_agrupador_kg_df.columns[7:])
    
    rpt_dff_join1 = pd.merge(pt_cajas_presentacion_df,pt_cajas_agrupador_df,on=["SEMANA", "F. COSECHA", "F. PRODUCCION", "TURNO","CLIENTE", "FUNDO", "VARIEDAD"],how="left")
    rpt_dff_join2= pd.merge(rpt_dff_join1,pt_cajas_agrupador_kg_df,on=["SEMANA", "F. COSECHA", "F. PRODUCCION", "TURNO","CLIENTE", "FUNDO", "VARIEDAD"],how="left")
    rpt_dff_join2 = rpt_dff_join2.rename(
        columns={"F. COSECHA":"FECHA DE COSECHA","F. PRODUCCION":"FECHA DE PROCESO","CLIENTE":"EMPRESA","TURNO":"TURNO DE PROCESO"}
    )
    dataframe_ = pd.merge(rpt_dff_join2,volcado_dff,on=["SEMANA", "FECHA DE COSECHA", "FECHA DE PROCESO", "TURNO DE PROCESO","EMPRESA", "FUNDO", "VARIEDAD"],how="left")
    dataframe_["Kg Procesados"] = dataframe_["Kg Procesados"].fillna(0)
    dataframe_["KG DESCARTE"] = dataframe_["KG DESCARTE"].fillna(0)
    dataframe_["% Descarte"] = (dataframe_["KG DESCARTE"] / dataframe_["Kg Procesados"])#s.round(1)
    dataframe_ = pd.merge(dataframe_,kg_exportables_df,on=["SEMANA", "FECHA DE COSECHA", "FECHA DE PROCESO", "TURNO DE PROCESO","EMPRESA", "FUNDO", "VARIEDAD"],how="left")
    dataframe_["Kg Exportables"] = dataframe_["Kg Exportables"].fillna(0)
    dataframe_["Kg Sobre Peso1"] = dataframe_["Kg Procesados"] - dataframe_["Kg Exportables"] - dataframe_["KG DESCARTE"]
    dataframe_["% Sobre Peso"] = (dataframe_["Kg Sobre Peso1"] / dataframe_["Kg Procesados"])
    dataframe_["% Merma"] = dataframe_["% Sobre Peso"].apply(lambda x: x - 0.04 if x > 0.04 else 0)
    dataframe_["Kg merma"] = dataframe_["% Merma"] * dataframe_["Kg Procesados"]
    dataframe_["Kg Sobre Peso"] = dataframe_["Kg Procesados"] - dataframe_["Kg Exportables"] - dataframe_["KG DESCARTE"]-dataframe_["Kg merma"]
    dataframe_["% Rendimiento MP"] = (dataframe_["Kg Exportables"] / dataframe_["Kg Procesados"])
    dataframe_["% Kg Exportables"] = (dataframe_["Kg Exportables"] / dataframe_["Kg Procesados"])
    #################################
    dataframe_["N° Bandejas Nacional"] = 0
    dataframe_["Formato"] = 0
    dataframe_["Kg Nacionales"] = 0
    dataframe_["Kg Fruta Jumbo "] = 0
    dataframe_["% Fruta Jumbo"] = 0
    dataframe_["% Fruta Convencional"] = 0
    dataframe_["% Kg Nacionales"] = 0
    dataframe_["TOTAL CAJAS EXPORTADAS"] = dataframe_[list_presentacion].sum(axis=1)
    dataframe_["TOTAL DE CAJAS EXPORTADAS + MUESTRAS"] = dataframe_["TOTAL CAJAS EXPORTADAS"]
    list_complemento_1 = ['N° Bandejas Nacional','Formato','Kg Nacionales','Kg Exportables','Kg Fruta Jumbo ','% Fruta Jumbo','% Fruta Convencional','% Kg Exportables','% Kg Nacionales','TOTAL CAJAS EXPORTADAS',
    
    ]
    dataframe_ = dataframe_[['SEMANA', 'FECHA DE COSECHA', 'FECHA DE PROCESO', 'TURNO DE PROCESO',
       'EMPRESA','TIPO DE PRODUCTO', 'FUNDO', 'VARIEDAD','Kg Procesados' ,'KG DESCARTE',
       '% Descarte',  'Kg Sobre Peso1', '% Sobre Peso',
       '% Merma', 'Kg merma', 'Kg Sobre Peso', '% Rendimiento MP']+list_presentacion+list_complemento_1+list_agrupador+list_agrupador_kg]
    return dataframe_




def reporte_produccion_transform():
    df = reporte_produccion_extract()
    df["Kg Procesados"] = df["Kg Procesados"].apply(transform_kg_text_rp_packing)
    df["Kg Procesados"] = df["Kg Procesados"].astype(float)
    df["%. Kg Exportables"] = df["%. Kg Exportables"].str.replace(",", ".", regex=False).astype(float)
    
    df["Kg Exportables"] = df["Kg Procesados"].astype(float) * (df["%. Kg Exportables"].astype(float)/100)
    df["TOTAL CAJAS EXPORTADAS"] = df["TOTAL CAJAS EXPORTADAS"].astype(int)
    
    # Convertir columnas numéricas
    for col in ["Kg Descarte","% Descarte","Kg Sobre Peso","% Sobre Peso","Kg Merma","% Merma","% Rendimiento MP",]:
        df[col] = df[col].str.replace(",", ".", regex=False).astype(float)
    
    #
    df["Fecha de cosecha"] = pd.to_datetime(df["Fecha de cosecha"],dayfirst=True).dt.strftime('%Y-%m-%d')
    df["Fecha de proceso"] = pd.to_datetime(df["Fecha de proceso"],dayfirst=True).dt.strftime('%Y-%m-%d')


    return df

def reporte_produccion_costos_transform():
    df = reporte_produccion_transform()
    df = df.rename(columns={
            "Semana":"SEMANA","Fecha de proceso":"FECHA", 'Variedad':"VARIEDAD",'Fundo':"FUNDO",'Empresa':"EMPRESA",
            "Kg Exportables":"KG_EXPORTABLES","Kg Descarte":"KG_DESCARTE","Kg Procesados":"KG_PROCESADOS"
        })
    
    df = df.groupby(["SEMANA","FECHA","VARIEDAD","FUNDO","EMPRESA"])[["KG_EXPORTABLES","KG_DESCARTE","KG_PROCESADOS"]].sum().reset_index()
    df["FECHA"] = pd.to_datetime(df["FECHA"]).dt.date
    
    return df

def agrupadores_rp_transform():
    agrupador_rp_df,agrupador_cajas_df = agrupador_rp_extract()
    agrupador_rp_df = agrupador_rp_df[agrupador_rp_df["AGRUPADOR REPORTE DE PRODUCCION"].notna()]
    agrupador_rp_df["AGRUPADOR REPORTE DE PRODUCCION"] = agrupador_rp_df["AGRUPADOR REPORTE DE PRODUCCION"].str.strip()
    agrupador_rp_df["PRESENTACIONES PRODUCTO TERMINADO"] = agrupador_rp_df["PRESENTACIONES PRODUCTO TERMINADO"].str.strip()
    agrupador_cajas_df = agrupador_cajas_df[agrupador_cajas_df["AGRUPADOR"].notna()]
    agrupador_cajas_df["AGRUPADOR"] = agrupador_cajas_df["AGRUPADOR"].str.strip()
    agrupador_cajas_df["PRESENTACIONES"] = agrupador_cajas_df["PRESENTACIONES"].str.strip()
    agrupador_cajas_df = agrupador_cajas_df[["PRESENTACIONES","AGRUPADOR"]]
    return agrupador_rp_df,agrupador_cajas_df


def registro_phl_pt_formatos_transform(access_token):
    agrupador_rp,agrupador_cajas = agrupadores_rp_transform()
    
    
    
    df = registro_phl_pt_extract(access_token)
    df["DESCRIPCION DEL PRODUCTO"] = df["DESCRIPCION DEL PRODUCTO"].str.strip()
    df = df[df["F. PRODUCCION"].notna()]
    df = df.groupby(["SEMANA","F. PRODUCCION","DESCRIPCION DEL PRODUCTO"]).agg(
        {"CLIENTE":"count"}#"Nº CAJAS":"sum","KG EXPORTABLES ":"sum",
    ).reset_index()
    df["DESCRIPCION DEL PRODUCTO"] = df["DESCRIPCION DEL PRODUCTO"].replace(
        {
            "125 GRS C/E SAN LUCAR +22MM-M":"125 GRS C/E SAN LUCAR+22MM-M",
            "125 GRS C/E SAN LUCAR +24MM-M":"125 GRS C/E SAN LUCAR+24MM-M",
            "125 GRS C/E SAN LUCAR +20MM-M":"125 GRS C/E SAN LUCAR+20MM-M",
        }
    )
    
    agrupador_rp = agrupador_rp.rename(columns={"PRESENTACIONES PRODUCTO TERMINADO":"DESCRIPCION DEL PRODUCTO"})
    agrupador_cajas = agrupador_cajas.rename(columns={"PRESENTACIONES":"AGRUPADOR REPORTE DE PRODUCCION"})
    df = pd.merge(df,agrupador_rp,on=["DESCRIPCION DEL PRODUCTO"],how="left")
    df = pd.merge(df,agrupador_cajas,on=["AGRUPADOR REPORTE DE PRODUCCION"],how="left")
    df = df.rename(columns={"CLIENTE":"N° PALLETS"})
    #df = df.rename(columns={"AGRUPADOR CAJAS":"AGRUPADOR"})
    
    return df