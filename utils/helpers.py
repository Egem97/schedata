import re
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter
import re
from openpyxl.worksheet.table import Table, TableStyleInfo
import io

# Validar y corregir HORA RECEPCION para que siempre sea de la tarde
def corregir_hora_tarde(hora_str):
        if pd.isna(hora_str):
            return hora_str
        match = re.match(r"(\d{2}):(\d{2}):(\d{2})", str(hora_str))
        if not match:
            return hora_str
        h, m, s = map(int, match.groups())
        # Si la hora es menor a 12, sumamos 12 horas para que sea PM
        if h < 12:
            h += 12
            if h == 24:
                h = 12  # 12 PM
        return f"{h:02d}:{m:02d}:{s:02d}"


def create_format_excel(dff: pd.DataFrame, nombre_archivo: str) -> str:
     with pd.ExcelWriter(nombre_archivo, engine="openpyxl") as writer:
        dff.to_excel(writer, index=False, sheet_name="TIEMPOS")
        ws = writer.sheets["TIEMPOS"]

        # Encabezados en negrita y fondo azul claro
        header_fill = PatternFill(start_color="B7DEE8", end_color="B7DEE8", fill_type="solid")
        for col_num, col in enumerate(dff.columns, 1):
            cell = ws.cell(row=1, column=col_num)
            cell.font = Font(bold=True)
            cell.fill = header_fill

        # Ajustar ancho de columnas automáticamente
        for i, col in enumerate(dff.columns, 1):
            max_length = max(
                [len(str(cell.value)) if cell.value is not None else 0 for cell in ws[get_column_letter(i)]]
            )
            ws.column_dimensions[get_column_letter(i)].width = max_length + 2

        # Congelar la primera fila
        ws.freeze_panes = "A2"

        # Validar encabezados para tabla de Excel
        columnas_validas = True
        colnames = list(dff.columns)
        if any(pd.isna(col) or str(col).strip() == '' for col in colnames):
            columnas_validas = False
        if len(set(colnames)) != len(colnames):
            columnas_validas = False
        if any(any(c in str(col) for c in ['[', ']', '*', '?', '/', '\\']) for col in colnames):
            columnas_validas = False

        # Crear tabla de Excel real solo si los encabezados son válidos
        if columnas_validas:
            nrows = dff.shape[0] + 1  # +1 por encabezado
            ncols = dff.shape[1]
            last_col = get_column_letter(ncols)
            table_ref = f"A1:{last_col}{nrows}"
            tabla = Table(displayName="TIEMPOS_TABLA", ref=table_ref)
            style = TableStyleInfo(name="TableStyleMedium9", showFirstColumn=False, showLastColumn=False, showRowStripes=True, showColumnStripes=False)
            tabla.tableStyleInfo = style
            ws.add_table(tabla)
        else:
            print("No se pudo crear la tabla de Excel porque los encabezados no son válidos para Excel. Solo se exportó el formato básico.")

def create_format_excel_in_memory(dff: pd.DataFrame) -> bytes:
    """
    Crea un archivo Excel formateado en memoria y retorna los bytes
    
    Args:
        dff: DataFrame de pandas a formatear
    
    Returns:
        bytes: Contenido del archivo Excel formateado
    """
    # Crear buffer en memoria
    excel_buffer = io.BytesIO()
    
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        dff.to_excel(writer, index=False, sheet_name="TIEMPOS")
        ws = writer.sheets["TIEMPOS"]

        # Encabezados en negrita y fondo azul claro
        header_fill = PatternFill(start_color="B7DEE8", end_color="B7DEE8", fill_type="solid")
        for col_num, col in enumerate(dff.columns, 1):
            cell = ws.cell(row=1, column=col_num)
            cell.font = Font(bold=True)
            cell.fill = header_fill

        # Ajustar ancho de columnas automáticamente
        for i, col in enumerate(dff.columns, 1):
            max_length = max(
                [len(str(cell.value)) if cell.value is not None else 0 for cell in ws[get_column_letter(i)]]
            )
            ws.column_dimensions[get_column_letter(i)].width = max_length + 2

        # Congelar la primera fila
        ws.freeze_panes = "A2"

        # Validar encabezados para tabla de Excel
        columnas_validas = True
        colnames = list(dff.columns)
        if any(pd.isna(col) or str(col).strip() == '' for col in colnames):
            columnas_validas = False
        if len(set(colnames)) != len(colnames):
            columnas_validas = False
        if any(any(c in str(col) for c in ['[', ']', '*', '?', '/', '\\']) for col in colnames):
            columnas_validas = False

        # Crear tabla de Excel real solo si los encabezados son válidos
        if columnas_validas:
            nrows = dff.shape[0] + 1  # +1 por encabezado
            ncols = dff.shape[1]
            last_col = get_column_letter(ncols)
            table_ref = f"A1:{last_col}{nrows}"
            tabla = Table(displayName="TIEMPOS_TABLA", ref=table_ref)
            style = TableStyleInfo(name="TableStyleMedium9", showFirstColumn=False, showLastColumn=False, showRowStripes=True, showColumnStripes=False)
            tabla.tableStyleInfo = style
            ws.add_table(tabla)
        else:
            print("⚠️  No se pudo crear la tabla de Excel porque los encabezados no son válidos. Solo se aplicó el formato básico.")
    
    # Obtener los bytes del archivo Excel formateado
    excel_data = excel_buffer.getvalue()
    excel_buffer.close()
    
    return excel_data