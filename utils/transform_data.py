import pandas as pd
import logging
from utils.helpers import corregir_hora_tarde

def recepcion_clean_data(df :pd.DataFrame):
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
    return df



def tiempos_transform_packing_data(recpdf,enfridf,volcadodf):
    #RECEPCION
    logger = logging.getLogger(__name__)
    if recpdf["FECHA RECEPCION"].dtype == 'object':
        recpdf["FECHA RECEPCION"] = pd.to_datetime(recpdf["FECHA RECEPCION"], errors='coerce')
    if recpdf["HORA RECEPCION"].dtype == 'object' or 'time' in str(recpdf["HORA RECEPCION"].dtype):
        recpdf["HORA RECEPCION"] = recpdf["HORA RECEPCION"].astype(str).str.extract(r'(\d{2}:\d{2}:\d{2})')[0]
    elif pd.api.types.is_datetime64_any_dtype(recpdf["HORA RECEPCION"]):
        recpdf["HORA RECEPCION"] = recpdf["HORA RECEPCION"].dt.strftime('%H:%M:%S')

    if "HORA RECEPCION" in recpdf.columns:
        recpdf["HORA RECEPCION"] = recpdf["HORA RECEPCION"].apply(corregir_hora_tarde)

    recpdf = recpdf.groupby(["FECHA RECEPCION","HORA RECEPCION","N° PALLET","CODIGO QR"])[["KILOS BRUTO"]].sum().reset_index()
    recpdf = recpdf[recpdf["FECHA RECEPCION"] >= "2025-07-10"]
    recpdf = recpdf.rename(columns={"CODIGO QR":"QR"})
    recpdf = recpdf.drop(columns=["KILOS BRUTO"])
    #ENFRIAMIENTO
    enfridf['FECHA'] = pd.to_datetime(enfridf['FECHA'], dayfirst=True).dt.strftime('%Y-%m-%d')
    enfridf = enfridf.groupby(["FECHA","HORA INICIAL","QR","HORA FINAL"])[["FORMATO"]].count().reset_index()
    enfridf = enfridf[enfridf["FECHA"] >= "2025-07-10"]
    enfridf = enfridf.rename(columns={
            "FECHA": "FECHA ENFRIAMIENTO",
            "HORA FINAL": "HORA FINAL ENFRIAMIENTO",
            #"HORA INTERMEDIA": "HORA INTERMEDIA ENFRIAMIENTO",
            "HORA INICIAL": "HORA INICIAL ENFRIAMIENTO"
    })
    enfridf = enfridf.drop(columns=["FORMATO"])
    enfridf["HORA INICIAL ENFRIAMIENTO"] = pd.to_datetime(enfridf["HORA INICIAL ENFRIAMIENTO"], format='%H:%M', errors='coerce').dt.strftime('%H:%M:%S')
    enfridf["HORA FINAL ENFRIAMIENTO"] = pd.to_datetime(enfridf["HORA FINAL ENFRIAMIENTO"], format='%H:%M', errors='coerce').dt.strftime('%H:%M:%S')
    #VOLCADO
    
    volcadodf['FECHA DE COSECHA'] = pd.to_datetime(volcadodf['FECHA DE COSECHA'], dayfirst=True).dt.strftime('%Y-%m-%d')
    volcadodf['FECHA DE PROCESO'] = pd.to_datetime(volcadodf['FECHA DE PROCESO'], dayfirst=True).dt.strftime('%Y-%m-%d')
    volcadodf["PESO NETO"] = volcadodf["PESO NETO"].str.replace(",", ".", regex=False).astype(float)
    if volcadodf["FECHA DE PROCESO"].dtype == 'object':
        volcadodf["FECHA DE PROCESO"] = pd.to_datetime(volcadodf["FECHA DE PROCESO"], errors='coerce')
    volcadodf = volcadodf.groupby(["FECHA DE PROCESO","HORA INICIO","HORA FINAL","QR","PROVEEDOR","FORMATO"])[["TIPO DE PRODUCTO"]].count().reset_index()
    volcadodf = volcadodf[volcadodf["FECHA DE PROCESO"] >= "2025-07-10"]
    volcadodf["QR"] = volcadodf["QR"].str.strip()
    volcadodf["HORA INICIO"] = pd.to_datetime(volcadodf["HORA INICIO"], format='%H:%M', errors='coerce').dt.strftime('%H:%M:%S')
    volcadodf["HORA FINAL"] = pd.to_datetime(volcadodf["HORA FINAL"], format='%H:%M', errors='coerce').dt.strftime('%H:%M:%S')
    volcadodf = volcadodf.rename(columns={"HORA INICIO": "HORA INICIO PROCESO","HORA FINAL": "HORA FINAL PROCESO"})
    volcadodf = volcadodf.drop(columns=["TIPO DE PRODUCTO"])
    logger.info(f"QR nulos volcado: {volcadodf['QR'].isnull().sum()}")
    volcadodf["QR"] = volcadodf["QR"].fillna("N/A")
    #JOINS
    dff = pd.merge(recpdf,enfridf,on="QR",how="left")
    
    dff = pd.merge(dff,volcadodf,on="QR",how="left")
    
    dff["FECHA RECEPCION"] = dff["FECHA RECEPCION"].dt.date
    
    #dff["FECHA ENFRIAMIENTO"] = dff["FECHA ENFRIAMIENTO"].dt.date
    
    dff["FECHA DE PROCESO"] = dff["FECHA DE PROCESO"].dt.date
    
    return dff