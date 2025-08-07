import pandas as pd
import logging
from data.extract.costos_extract import *
from utils.helpers import *
from utils.get_api import get_tc_sunat_diario
from utils.suppress_warnings import setup_pandas_warnings

setup_pandas_warnings()


logger = logging.getLogger(__name__)

def tipo_cambio_transform(access_token):
    now = datetime.now().strftime('%Y-%m-%d')
    df = tipo_cambio_extract(access_token)
    df["FECHA"] = pd.to_datetime(df["FECHA"])
    ultima_fecha = df["FECHA"].max()
    fecha_str = ultima_fecha.strftime('%Y-%m-%d')
    if now >fecha_str:
        logger.info(f"🔍 Buscando tipo de cambio para la fecha: {now}")
        json_tc = get_tc_sunat_diario(date= now)
        df_tc = pd.DataFrame([json_tc])
        df_tc.columns = ["PrecioCompra","PrecioVenta","Moneda","FECHA"]
        df_tc["FECHA"] = pd.to_datetime(df_tc["FECHA"])
        df = pd.concat([df,df_tc])
        return df
    else:
        logger.info(f"🔍 Tipo de cambio para la fecha: {now} ya existe")
        return df


def ocupacion_transporte_packing_transform(access_token):
    logger.info("🚀 Iniciando proceso de consulta de datos de costos transporte packing...")
    df = costos_transporte_packing_extract(access_token)
    df["COSTO"] = df["COSTO"].fillna(0)
    df = df.groupby(["SEMANA","FECHA"])[["N° ASIENTOS OCUPADOS","CAPACIDAD DE ASIENTOS"]].sum().reset_index()
    return df

def costos_transporte_packing_transform(access_token):
    logger.info("🚀 Iniciando proceso de consulta de datos de costos transporte packing...")
    df = costos_transporte_packing_extract(access_token)
    df["COSTO"] = df["COSTO"].fillna(0)
    df["TIPO DE MOVILIDAD"] = df["TIPO DE MOVILIDAD"].str.strip()
    return df

def costos_concesionario_packing_transform():
    logger.info("🚀 Iniciando proceso de consulta de datos de costos concesionario packing...")
    df = costos_concesionario_packing_extract()
    df = df[df["CANTIDAD"].notna()]
    #df = df.drop(columns=['NRO. DOC.','APELLIDOS, NOMBRE','Unnamed: 8'])
    df = df.dropna(how='all')
    df["TIPO TRABAJADOR"] = df["TIPO TRABAJADOR"].fillna("NO ESPECIFICADO")
    df["TIPO TRABAJADOR"] = df["TIPO TRABAJADOR"].str.strip()

    df["AREA"] = df["AREA"].fillna("NO ESPECIFICADO")
    df["AREA"] = df["AREA"].str.strip()
    df["PUESTO/LABOR"] =  df["PUESTO/LABOR"].fillna("NO ESPECIFICADO")
    df["PUESTO/LABOR"] =  df["PUESTO/LABOR"].str.strip()
    df["TIPO MENU"] =  df["TIPO MENU"].fillna("NO ESPECIFICADO")
    df["TIPO MENU"] =  df["TIPO MENU"].str.strip()

    df["TOTAL"] = df["CANTIDAD"]*4
    
    df['FECHA'] = pd.to_datetime(df['FECHA'],format='%d/%m/%Y')  # Asegúrate de que sea tipo datetime
    
    df['SEMANA'] = df['FECHA'].dt.isocalendar().week 
    df = df.rename(columns={"PUESTO/LABOR":"LABOR"})    
    df = df.groupby(["SEMANA","FECHA","LABOR"])[["TOTAL"]].sum().reset_index()
    return df

def costos_planilla_adm_packing_transform(access_token,centro_costos_df):
    logger.info("🚀 Iniciando proceso de consulta de datos de costos planilla adm packing...")
    df_adm_2025 = planilla_adm_packing_extract(access_token)
    #centro_costos_df = centro_costos_packing_extract(access_token)[1]
    
    centro_costos_df = centro_costos_df.rename(columns={"PROYECTO":"COD PROYECTO"})
    centro_costos_df["COD PROYECTO"] = centro_costos_df["COD PROYECTO"].str.strip()
    centro_costos_df = centro_costos_df.groupby(["COD PROYECTO","DESCRIPCION PROYECTO"]).size().reset_index()
    centro_costos_df = centro_costos_df[["COD PROYECTO","DESCRIPCION PROYECTO"]]
    centro_costos_df["COD PROYECTO"] = centro_costos_df["COD PROYECTO"].str.strip()
    df_adm_2025 = df_adm_2025[df_adm_2025["Mes"].notna()]
    columns = ['Mes','Cargo', 'AREA','Fecha de Ingreso', 'Afp','REM BASE', 'Asignacion Familiar','Total Ingresos','Seguro Afp', 'Total Afp', 'Essalud', 'Costos', 'ID Actividad','COD PROYECTO' ]
    df_adm_2025["ID Actividad"] = df_adm_2025["ID Actividad"].astype(int).astype(str)
    df = df_adm_2025[columns]
    
    df["COD PROYECTO"] = df["COD PROYECTO"].str.strip()
    df["COD PROYECTO"] = df["COD PROYECTO"].replace({
        "PO024": "PO084","PO071": "PO084","PO061": "PO084","PO063":"PO084",
        "PO058":"PO078","PO059":"PO079","PO043":"PO083","PO006":"PO082",
        "PO067":"PO078","PO068":"PO079","PO069":"PO081","PO049":"PO083",
        "PO070":"PO080","PO064":"PO084","PO062":"PO084","PO050":"PO083"
    })
    df = pd.merge(df,centro_costos_df,on="COD PROYECTO",how="left")
    df = df[["Mes","DESCRIPCION PROYECTO","Costos"]]
    return df

def costos_planilla_obreros_packing_transform(access_token):
    logger.info("🚀 Iniciando proceso de consulta de datos de costos planilla obreros packing...")
    df = planilla_obreros_packing_extract(access_token)
    df = df[df["SEMANA"].notna()]
    return df

def horas_trabajadas_obreros_packing_transform(access_token):
    logger.info("🚀 Iniciando proceso de consulta de datos de costos planilla obreros packing...")
    df = costos_planilla_obreros_packing_transform(access_token)
    df = df[["SEMANA","FECHA","LABOR","Hrs. Laboradas - Planta","CODIGO LABOR"]]
    df["FECHA"] = pd.to_datetime(df["FECHA"])
    df["SEMANA"] = df["FECHA"].dt.isocalendar().week
    df["Hrs. Laboradas - Planta"] = df["Hrs. Laboradas - Planta"].fillna(0)
    df["Hrs. Laboradas - Planta"] = df["Hrs. Laboradas - Planta"].replace({"DM":0,"-":0,"":0})
    df["LABOR"] = df["LABOR"].fillna("NO ESPECIFICADO")
    df["LABOR"] = df["LABOR"].str.strip()
    df["CODIGO LABOR"] = df["CODIGO LABOR"].fillna(0)
    #df["CODIGO LABOR"] = df["CODIGO LABOR"].str.strip()
    df = df[(df["CODIGO LABOR"].isin([209,210]))]#(df["LABOR"].isin(["PESADORES","ABASTECEDOR","PALETIZADORES","ENCAJADOR"]))&
    df = df.groupby(["SEMANA","FECHA"])[["Hrs. Laboradas - Planta"]].sum().reset_index()
    df = df.rename(columns={"Hrs. Laboradas - Planta":"HORAS LABORADAS"})
    return df


def mayor_analitico_obreros_packing_transform(access_token):
    df = mayor_analitico_packing_extract(access_token)
    print(df.info())
    
    df["Cod. Actividad"] = df["Cod. Actividad"].str.strip()
    df["Glosa"] = df["Glosa"].str.strip()
    #&
    df =df[(df["Cod. Actividad"].isin(["209","210"])) ]#
    
    df =df.groupby(["Fecha","Glosa"])[["Razón Social"]].nunique().reset_index()
    df["Semana"] = df["Fecha"].dt.isocalendar().week
    df = df.rename(columns={"Fecha":"FECHA","Razón Social":"N° TRABAJADORES","Semana":"SEMANA"})
    df = df.groupby(["SEMANA","FECHA"])[["N° TRABAJADORES"]].sum().reset_index()
    return df


def seg_obreros_packing_transform(access_token):
    horas_df = horas_trabajadas_obreros_packing_transform(access_token)
    obreros_df = mayor_analitico_obreros_packing_transform(access_token)
    df = pd.merge(horas_df,obreros_df,on=["SEMANA","FECHA"],how="left")
    df["HORAS LABORADAS"] = df["HORAS LABORADAS"].fillna(0)
    df["N° TRABAJADORES"] = df["N° TRABAJADORES"].fillna(0)
    df["N° TRABAJADORES"] = df["N° TRABAJADORES"].astype(int)
    df["HORAS LABORADAS"] = df["HORAS LABORADAS"].astype(int)
    
    return df


def procesamiento_costos_packing_transform(access_token,agrupador_costos_df,centro_costos_df):
    logger.info("🚀 Iniciando proceso de consulta de datos de costos packing...")
    
    tc_df = tipo_cambio_extract(access_token)
    transporte_df = costos_transporte_packing_transform(access_token)
    concesonario_df = costos_concesionario_packing_transform()
    plan_adm_df = costos_planilla_adm_packing_transform(access_token,centro_costos_df)
    
    logger.info("🚀 Iniciando proceso de procesamiento de costos packing...")
    agrupador_costos_df = agrupador_costos_df.rename(columns={"ITEM":"DESCRIPCION PROYECTO"})
    agrupador_costos_df["DESCRIPCION PROYECTO"] = agrupador_costos_df["DESCRIPCION PROYECTO"].str.strip()
    #TRANSPORTE
    transporte_dff = transporte_df.groupby(['SEMANA', 'FECHA',])[['COSTO']].sum().reset_index()
    transporte_dff = transporte_dff.rename(columns={"COSTO":"TOTAL"})
    transporte_dff["DESCRIPCION PROYECTO"] = "TRANSPORTE DE PERSONAL PACKING"
    transporte_dff["FUENTE DATOS"] = "TRANSPORTE"
    #CONCESIONARIO
    
    concesonario_dff = concesonario_df.groupby(['SEMANA', 'FECHA'])[['TOTAL']].sum().reset_index()
    concesonario_dff["DESCRIPCION PROYECTO"] = "BIENESTAR SOCIAL - CONCESIONARIO" 
    concesonario_dff["FUENTE DATOS"] = "ALMUERZOS Y CENAS" 

    plani_df = structure_planilla_historica_like_estimate(plan_adm_df)
    
    try:
        plani_df_last_month = estimate_current_planilla_by_previous(plan_adm_df)
        
    except Exception as e:
        print(e)

    plani_df_last_month["FECHA"] = pd.to_datetime(plani_df_last_month["FECHA"])
    fecha_hoy= datetime.now()
    plani_df_last_month = plani_df_last_month[(plani_df_last_month['FECHA']<= fecha_hoy)]
    plani_dff = pd.concat([plani_df,plani_df_last_month])
    plani_dff["FECHA"] = pd.to_datetime(plani_dff["FECHA"]).dt.date
    plani_dff["SEMANA"] = pd.to_datetime(plani_dff["FECHA"]).dt.isocalendar().week
    plani_dff["FUENTE DATOS"] = "PLANILLA ADMINISTRATIVA"
    
    concesonario_dff["FECHA"] = pd.to_datetime(concesonario_dff["FECHA"]).dt.date
    transporte_dff["FECHA"] = pd.to_datetime(transporte_dff["FECHA"]).dt.date
    plani_dff["FECHA"] = pd.to_datetime(plani_dff["FECHA"]).dt.date
    data_concat_df = pd.concat([concesonario_dff,transporte_dff,plani_dff])
    data_concat_df["DESCRIPCION PROYECTO"] = data_concat_df["DESCRIPCION PROYECTO"].str.strip()
    data_concat_df["DESCRIPCION PROYECTO"] = data_concat_df["DESCRIPCION PROYECTO"].replace({
         "REMUNERACIONES RR.HH": "REMUNERACIONES RRHH.",
        
    })
    
    data_concat_df = pd.merge(data_concat_df,agrupador_costos_df,on="DESCRIPCION PROYECTO",how="left")
    data_concat_df["FECHA"] = pd.to_datetime(data_concat_df["FECHA"])

    data_concat_df = pd.merge(data_concat_df,tc_df,on="FECHA",how="left")
    data_concat_df['year'] = data_concat_df['FECHA'].dt.year
    data_concat_df = data_concat_df[data_concat_df["year"]==2025]
    data_concat_df["TOTAL"] = data_concat_df["TOTAL"]/data_concat_df["PrecioVenta"]
    data_concat_df = data_concat_df[[
        'SEMANA', 'FECHA', 'TOTAL', 'DESCRIPCION PROYECTO', 'FUENTE DATOS',
        'COD PROYECTO', 'AGRUPADOR', 'SUB AGRUPADOR',
    ]]
    data_concat_df["COD PROYECTO"] = data_concat_df["COD PROYECTO"].str.strip()

    return data_concat_df








def mayor_analitico_opex_transform(access_token,agrupador_costos_df):
    logger.info("🚀 Iniciando proceso de consulta de datos de mayor analitico opex...")
    df = mayor_analitico_packing_extract(access_token)
    df = df[df["Cuenta"].notna()]
    agrupador_costos_df = agrupador_costos_df.rename(columns={"ITEM":"Descripción Proyecto"})
    agrupador_costos_df["Descripción Proyecto"] = agrupador_costos_df["Descripción Proyecto"].str.strip()
    
    df[["Cod Cta. Contable",'Nombre Cta. Contable']] = df['Nombre Cta. Contable'].apply(split_if_colon_at_3).apply(pd.Series)
    var_category = ['Cuenta', 'Nombre Cta. Contable','Numero Operacion', 'Documento Referencia', 'Glosa','Voucher Contable','Código Cliente/Proveedor', 'Razón Social',
       'IDCCOSTO ', 'Doc. Origen Moneda', 'Descripción Moneda','Cod. Proyecto', 'Descripción Proyecto', 'Cod. Actividad','Descripción Actividad', 'Cod Cta. Contable',]
    
    df["Dólares Cargo"] = df["Dólares Cargo"] - df["Dólares Abono"] 
    #############################3

    df["Cod Cta. Contable"] = df["Cod Cta. Contable"].astype(str)
    df["Cod. Actividad"] = df["Cod. Actividad"].astype(str)
    df["Cod. Proyecto"] = df["Cod. Proyecto"].astype(str)
    df["Doc. Origen Moneda"] = df["Doc. Origen Moneda"].astype(str)
    df["Código Cliente/Proveedor"] = df["Código Cliente/Proveedor"].astype(str)
    df["Numero Operacion"] = df["Numero Operacion"].astype(str)
    df["Voucher Contable"] = df["Voucher Contable"].astype(str)
    df["Cuenta"] = df["Cuenta"].astype(str)
    for col in var_category:
        df[col] = df[col].str.strip()
    df["Descripción Actividad"] = df["Descripción Actividad"].fillna("NO ESPECIFICADO")
    df["Descripción Proyecto"] = df["Descripción Proyecto"].fillna("NO ESPECIFICADO")
    df["Descripción Moneda"] = df["Descripción Moneda"].fillna("-")
    df["Razón Social"] = df["Razón Social"].fillna("NO ESPECIFICADO")
    df["Glosa"] = df["Glosa"].fillna("NO ESPECIFICADO")
    df["Descripción Proyecto"] = df["Descripción Proyecto"].replace("", "OTROS_")
    df["Cod Cta. Contable"] = df["Cod Cta. Contable"].fillna("XX").replace("None", "XX")
    #manalitico_df["Cod Cta. Contable"] = manalitico_df["Cod Cta. Contable"]
    

    df["Fecha"] = pd.to_datetime(df["Fecha"])
    df["AÑO"] = df["Fecha"].dt.year
    df["MES"] = df["Fecha"].dt.month
    
    df["MES"] = df["MES"].apply(get_month_name)
    
    df["Cod Cta. Contable"] = df["Cod Cta. Contable"].replace("XX", "OTROS")
    ##### CONDICIONES MAYOR ANALITICO
    
    df["COD_DESCARTE"] = df["Cod. Proyecto"].fillna("PP000").str[:-3]
    df["COD_CUENTA_DESCARTE"] = df["Cuenta"].str[:2]
    df["COD_VAUCHER"] = df["Voucher Contable"].str[:3]
    df = df[df["COD_DESCARTE"]=="PO"]
    df = df[df["COD_CUENTA_DESCARTE"]!="95"]
    df = df[df["COD_VAUCHER"]!="020"]
    df = df.drop(columns=["COD_DESCARTE","COD_CUENTA_DESCARTE","COD_VAUCHER"])
    df = df[df["Descripción Proyecto"]!="INTERESES FINANCIEROS"]
    df["Descripción Proyecto"] = df["Descripción Proyecto"].replace({
        
        "SERVICIOS TI" : "SERVICIOS T.I.",
        "AGUA":"AGUA POTABLE",
        "AGUA PARA BEBER":"AGUA PARA BEBER + VASOS DESCARTABLES",
        "BUS PACKING (PERSONAL)":"BUS (PERSONAL)",
        "ENERGÍA ELÉCTRICA / GAS":"ENERGÍA ELÉCTRICA / PETRÓLEO",
        "UTENSILIOS PRODUCCIÓN":"UTENSILIOS DE PRODUCCIÓN",
        "MATERIAL ESCRITORIO":"MATERIAL DE ESCRITORIO",
        "REMUNERACIONES RR.HH":"REMUNERACIONES RRHH.",
        "PETRÓLEO / GASOLINA":"GLP / GASOLINA"

    })
    df = pd.merge(df,agrupador_costos_df,on="Descripción Proyecto",how="left")
    df["AGRUPADOR"] = df["AGRUPADOR"].fillna("IMPREVISTOS")
    df["SUB AGRUPADOR"] = df["SUB AGRUPADOR"].fillna("IMPREVISTOS")
    return df

def mayor_analitico_packing_transform(access_token):
    agrupador,centro_costos_df = centro_costos_packing_extract(access_token)
    ma_df = mayor_analitico_opex_transform(access_token,agrupador)
    agrupador_costos_df = procesamiento_costos_packing_transform(access_token,agrupador,centro_costos_df)

    ma_group_df = ma_df.groupby(['AGRUPADOR','SUB AGRUPADOR',"Cod. Proyecto","Descripción Proyecto",'Fecha'])[["Dólares Cargo"]].sum().reset_index()
    cod_list_exclude = ['PO018', 'PO084' ,'PO0X1', 'PO0X2' ,'PO066' ,'PO078' ,'PO079', 'PO080' ,'PO081','PO082' ,'PO083']
    dfff = ma_group_df[~ma_group_df["Cod. Proyecto"].isin(cod_list_exclude)]    

    dfff["FUENTE DE DATOS"] = "MAYOR ANALITICO"
    del ma_df,ma_group_df
    dfff = dfff.rename(columns = {"Cod. Proyecto":"COD PROYECTO","Descripción Proyecto":"DESCRIPCION PROYECTO","Fecha":"FECHA","Dólares Cargo":"TOTAL"})
    #agrupador_py_df = pd.read_parquet(url_path_agrupador)
    agrupador_costos_df["COD PROYECTO"] = agrupador_costos_df["COD PROYECTO"].str.strip()
    agrupador_costos_df["FUENTE DE DATOS"] = "PACKING"
    agrupador_costos_df = agrupador_costos_df[['AGRUPADOR', 'SUB AGRUPADOR', 'COD PROYECTO', 'DESCRIPCION PROYECTO','FECHA', 'TOTAL', 'FUENTE DE DATOS']]
    dfff = pd.concat([dfff,agrupador_costos_df])
    dfff['SEMANA'] = dfff['FECHA'].dt.isocalendar().week
    dfff = dfff.groupby(["AGRUPADOR","COD PROYECTO","DESCRIPCION PROYECTO","FECHA","SEMANA","FUENTE DE DATOS"])[["TOTAL"]].sum().reset_index()
    return dfff

def presupuesto_packing_transform(access_token):
    df = presupuesto_packing_extract(access_token)
    var_category = ['EMPRESA', 'SEDE', 'AGRUPADOR', 'CUENTA', 'SUBCUENTA','TIPO PRESUPUESTO', 'ITEM','NOMBRE', 'VALIDAR_BLANCO', 'ITEM_CORREGIDO']
    for col in var_category:
        df[col] = df[col].str.strip()
    df["NOMBRE"] = df["NOMBRE"].fillna("XXXX")
    df["ITEM_CORREGIDO"] = df["ITEM_CORREGIDO"].replace("SEVICIOS T.I.","SERVICIOS T.I.")
    df["PERIODO"] = df["PERIODO"].astype(str)
    return df

def kg_presupuesto_packing_transform(access_token):
    df = kg_presupuesto_packing_extract(access_token)
    df = df[df["SEMANA"].notna()]
    df["MES"] = df["MES"].replace(change_month)	
    
    df["AÑO"] = df["AÑO"].astype(int).astype(str)
    df["AÑO"] = df["AÑO"].str.strip()
    df = df.rename(columns={
        "KG PROCESADOS - PPTO":"KG PPTO PROCESADOS ",
        "KG EXPORTADOS - PPTO" :"KG PPTO EXPORTADOS"
        }
    )
    return df