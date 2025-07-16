import pandas as pd

def dataTranformTransporteControl(control_tran_df):
    control_tran_df["COSTO"] = control_tran_df["COSTO"].fillna(0)
    control_tran_df["TIPO DE MOVILIDAD"] = control_tran_df["TIPO DE MOVILIDAD"].str.strip()
    return control_tran_df