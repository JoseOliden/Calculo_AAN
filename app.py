"""
Interfaz Streamlit para procesar archivos .k0s, .RPT y realizar el cruce con la Base de Datos.
Basado en el código proporcionado por el usuario (adaptado para Streamlit).

Funciones principales incluidas:
 - procesar_k0s_independiente
 - extract_and_tokenize_metadata
 - extraer_variables_clave
 - limpieza y cruce RPT <-> Base de Datos (doble validación: energía + nombre)
 - lectura de RDN según geometría
 - procesamiento del comparador Au desde .RPT
Outputs:
 - _k0s.xlsx (metadatos)
 - _VERIFICADO.xlsx
 - _FINAL_UNIFICADO.xlsx

Notas:
 - Puedes subir archivos individualmente con los uploaders.
 - Opcionalmente puedes especificar una ruta local si ejecutas la app en máquina con acceso al filesystem.
"""

import streamlit as st
import pandas as pd
import numpy as np
import os
import re
import io
from datetime import datetime

st.set_page_config(layout="wide", page_title="AAN - Procesador k0s / RPT")

# ----------------------------
# Configuración por defecto
# ----------------------------
DEFAULT_DIRECTORIO = ""  # si deseas un path por defecto en servidor, ponlo aquí
NOMBRE_BASE_DATOS_INFO = 'Base de datos.xlsx'
COLUMNA_BD_ENERGIA = 'EGKEV'           # Columna G (Energía BD)
COLUMNA_BD_NUCLIDO = 'NUCLIDES'        # Columna B (Nombre Nucleido BD)
TOLERANCIA_ENERGIA = 1.5
FILAS_A_OMITIR_RPT = 17
CODIFICACION = 'latin-1'
LINES_TO_READ_K0S = 10

# ----------------------------
# FUNCIONES AUXILIARES
# ----------------------------
def limpiar_nombre(texto):
    """Normaliza nombres para comparación (quita guiones, espacios, mayúsculas)."""
    if pd.isna(texto): return ""
    return str(texto).upper().replace('-', '').replace(' ', '').strip()

def procesar_k0s_independiente(ruta_entrada, ruta_salida, lines_to_read=LINES_TO_READ_K0S):
    """Lee el K0S y genera un Excel independiente."""
    metadata = []
    try:
        with open(ruta_entrada, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= lines_to_read: break
                cleaned_line = line.strip()
                tokens = re.split(r'\s+', cleaned_line) if cleaned_line else ['']
                metadata.append(tokens)
        df = pd.DataFrame(metadata)
        # Guardar a ruta_salida local
        df.to_excel(ruta_salida, index=False, header=False, na_rep='')
        return df
    except Exception as e:
        st.error(f"[K0S] Error: {e}")
        return None

def extract_and_tokenize_metadata_filelike(filelike, num_lines=LINES_TO_READ_K0S):
    """
    Dado un file-like (BytesIO / UploadedFile), extrae las primeras 'num_lines',
    tokeniza y devuelve DataFrame.
    """
    metadata = []
    try:
        # filelike may be BytesIO or UploadedFile; decode to text lines
        text = filelike.read().decode('utf-8', errors='replace')
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if i >= num_lines:
                break
            cleaned_line = line.strip()
            if cleaned_line:
                tokens = re.split(r'\s+', cleaned_line)
            else:
                tokens = ['']
            metadata.append(tokens)
        df = pd.DataFrame(metadata)
        return df
    except Exception as e:
        st.error(f"Error extrayendo metadata: {e}")
        return None

def extraer_variables_clave(df):
    """
    Extrae fecha, hora de medición, t_vivo y t_real desde un DataFrame resultante de k0s.
    Se asume que los índices están en posiciones similares a tu script original.
    """
    try:
        if df is not None and df.shape[0] > 5 and df.shape[1] > 1:
            f_med = df.iloc[3, 0]
            hora_med = df.iloc[3, 1]
            t_v = df.iloc[5, 0]
            t_r = df.iloc[5, 1]
            return f_med, hora_med, t_v, t_r
        else:
            return None, None, None, None
    except Exception as e:
        st.error(f"Error al extraer variables: {e}")
        return None, None, None, None

def read_rpt_to_df(ruta_o_filelike, skiprows=FILAS_A_OMITIR_RPT, encoding=CODIFICACION):
    """
    Lee un archivo RPT y devuelve un DataFrame limpio con las columnas esperadas.
    ruta_o_filelike: puede ser path str o UploadedFile
    """
    cols_rpt = ['F/M', 'Peak_No', 'ROI_Start', 'ROI_End', 'Peak_Centroid',
                'Energy_keV', 'Net_Peak_Area', 'Net_Area_Uncert', 'Continuum_Counts',
                'Tentative_Nuclide', 'Info_Extra']
    try:
        if hasattr(ruta_o_filelike, "read"):
            # UploadedFile: leer bytes y decodificar en stringIO
            content = ruta_o_filelike.read().decode('latin-1', errors='replace')
            df_rpt = pd.read_csv(io.StringIO(content), sep=r'\s+', skiprows=skiprows,
                                 names=cols_rpt, skipinitialspace=True, engine='python').dropna(how='all')
        else:
            df_rpt = pd.read_csv(ruta_o_filelike, sep=r'\s+', skiprows=skiprows,
                                 names=cols_rpt, skipinitialspace=True, encoding=encoding, engine='python').dropna(how='all')

        df_rpt['Tentative_Nuclide'] = df_rpt['Tentative_Nuclide'].fillna('').astype(str)
        df_rpt['Info_Extra'] = df_rpt['Info_Extra'].fillna('').astype(str)
        mask = df_rpt['Info_Extra'] != ''
        if mask.any():
            df_rpt.loc[mask, 'Tentative_Nuclide'] = df_rpt.loc[mask, 'Tentative_Nuclide'] + " " + df_rpt.loc[mask, 'Info_Extra']
        df_rpt.drop(columns=['Info_Extra'], inplace=True)

        for col in ['Energy_keV', 'Net_Peak_Area']:
            df_rpt[col] = pd.to_numeric(df_rpt[col], errors='coerce')
        df_rpt.dropna(subset=['Energy_keV'], inplace=True)

        return df_rpt
    except Exception as e:
        st.error(f"Error leyendo RPT: {e}")
        return None

def buscar_identidad_por_energia(df_nuclidos, valor_pico, tolerancia=TOLERANCIA_ENERGIA):
    matches = df_nuclidos[
        (df_nuclidos.iloc[:,1] >= valor_pico - tolerancia) &
        (df_nuclidos.iloc[:,1] <= valor_pico + tolerancia)
    ]
    return ", ".join(matches.iloc[:,0].astype(str).tolist()) if not matches.empty else 'Desconocido'

def process_double_validation(df_rpt_verificado, df_info_extra, tolerancia=TOLERANCIA_ENERGIA):
    """
    Realiza el cruce doble: energía + nombre, devolviendo df_final (o None).
    """
    filas_finales = []
    for _, row_rpt in df_rpt_verificado.iterrows():
        e_rpt = row_rpt['Energy_keV']
        candidatos_bd = df_info_extra[
            (df_info_extra[COLUMNA_BD_ENERGIA] >= e_rpt - tolerancia) &
            (df_info_extra[COLUMNA_BD_ENERGIA] <= e_rpt + tolerancia)
        ]
        if not candidatos_bd.empty:
            nombre_tentativo_rpt = limpiar_nombre(row_rpt['Tentative_Nuclide'])
            nombre_verificado_rpt = limpiar_nombre(row_rpt.get('Identidad_Verificada_Energia', ''))
            for _, row_bd in candidatos_bd.iterrows():
                nombre_bd = row_bd['NOMBRE_LIMPIO_BD']
                coincide_nombre = (nombre_bd in nombre_tentativo_rpt) or (nombre_bd in nombre_verificado_rpt)
                if coincide_nombre:
                    data_combinada = {**row_rpt.to_dict(), **row_bd.to_dict()}
                    if 'NOMBRE_LIMPIO_BD' in data_combinada: del data_combinada['NOMBRE_LIMPIO_BD']
                    filas_finales.append(data_combinada)
    if filas_finales:
        df_final = pd.DataFrame(filas_finales)
        return df_final
    else:
        return None

def find_line_with_target(content_text, target):
    target_upper = target.upper()
    for line in content_text.splitlines():
        if target_upper in line.upper():
            return line.strip()
    return None

# ---------- INTEGRAR ESTAS IMPORTS si no están en app.py ----------
import numpy as np
import sympy as sp
from scipy.optimize import root
# -----------------------------------------------------------------

# ===========================
# FUNCIONES AUXILIARES DE CÁLCULO
# ===========================

# --- Funciones elementales de Aesp / factores ---
def cal_D(lam, td):
    """D = exp(lam * td)"""
    return np.exp(lam * td)

def cal_C(lam, tr):
    """C = lam / (1 - exp(-lam * tr))"""
    # evitar división por cero para lam*tr muy pequeños
    denom = 1.0 - np.exp(-lam * tr)
    return lam / denom if denom != 0 else np.nan

def cal_H(tr, tv):
    """H = tr / tv"""
    return tr / tv if tv != 0 else np.nan

def cal_S(lam, ti):
    """S = 1 - exp(-lam * ti)"""
    return 1.0 - np.exp(-lam * ti)

def Aesp(Cn_i, w_i, lam, tr, td, ti, tv, e=None):
    """
    Calcula la actividad específica Aesp.
    Observación: e está presente en la firma original pero no se usa en la fórmula.
    """
    C_i = cal_C(lam, tr)
    D_i = cal_D(lam, td)
    H_i = cal_H(tr, tv)
    S_i = cal_S(lam, ti)
    # Evitar division por cero
    if S_i == 0 or w_i == 0 or tv == 0:
        return np.nan
    return Cn_i * D_i * C_i * H_i / (S_i * w_i)


# --- Sistema para hallar alfa (si aún lo quieres usar) ---
def equations_alfa(vars_arr, *par):
    """
    Ecuación para resolver alfa.
    vars_arr: array-like con [alfa]
    par: (Aesp1, k0_1, e1, Er1, Q0_1, Aesp2, k0_2, e2, Er2, Q0_2, Aesp3, k0_3, e3, Er3, Q0_3)
    """
    alfa = vars_arr[0]
    (Aesp_1, k0_1, e_1, Er_1, Q0_1,
     Aesp_2, k0_2, e_2, Er_2, Q0_2,
     Aesp_3, k0_3, e_3, Er_3, Q0_3) = par

    term12 = (1 - (Aesp_2 / Aesp_1) * (k0_1 / k0_2) * (e_1 / e_2))
    term13 = (1 - (Aesp_3 / Aesp_1) * (k0_1 / k0_3) * (e_1 / e_3))

    # Evitar potencias con negativos/ceros en denominadores
    with np.errstate(all='ignore'):
        eq1 = (term12 ** (-1) - term13 ** (-1)) * (Q0_1 - 0.429) / (Er_1 ** alfa) \
              - (term12 ** (-1)) * (Q0_2 - 0.429) / (Er_2 ** alfa) \
              + (term13 ** (-1)) * (Q0_3 - 0.429) / (Er_3 ** alfa)
    return [eq1]


def cal_Q0_alfa_i(Q0, Er, alfa):
    """Calcula Q0_alfa para un elemento."""
    # Fórmula tomada de tu código
    return (Q0 - 0.429) / (Er ** alfa) + 0.429 / (2 * alfa + 0.55 ** alfa)


def cal_f_alfa(Q0_alfa_c, Aesp_c, e_c, k0_c):
    """
    Calcula f a partir de arrays de comparadores (típicamente 2 o más valores).
    Q0_alfa_c, Aesp_c, e_c, k0_c son arrays numpy o listas.
    """
    # Convertir a numpy para operaciones seguras
    Q0_alfa_c = np.asarray(Q0_alfa_c)
    Aesp_c = np.asarray(Aesp_c)
    e_c = np.asarray(e_c)
    k0_c = np.asarray(k0_c)

    num = (k0_c[0] / k0_c[1]) * (e_c[0] / e_c[1]) * Q0_alfa_c[0] - (Aesp_c[0] / Aesp_c[1]) * Q0_alfa_c[1]
    den = (Aesp_c[0] / Aesp_c[1]) - (k0_c[0] / k0_c[1]) * (e_c[0] / e_c[1])
    return num / den if den != 0 else np.nan


def cal_alfa(par_comp, equations_func=equations_alfa, guess=0.2):
    """
    Calcula alfa resolviendo el sistema con scipy.optimize.root.
    par_comp: tu tuple (k0_c, e_c, Q0_c, Cn_c, w_c, lam_c, Er_c, td_c, tr_c, ti_c, tv_c)
    Devuelve alfa (float) o np.nan si no converge.
    """
    k0_c, e_c, Q0_c, Cn_c, w_c, lam_c, Er_c, td_c, tr_c, ti_c, tv_c = par_comp
    # Calcular Aesp de los comparadores
    Aesp_c = np.zeros(len(k0_c))
    for i in range(len(k0_c)):
        Aesp_c[i] = Aesp(Cn_c[i], w_c[i], lam_c[i], tr_c[i], td_c[i], ti_c[i], tv_c[i], e_c[i])

    # Preparar parámetros para la ecuación
    par = (Aesp_c[0], k0_c[0], e_c[0], Er_c[0], Q0_c[0],
           Aesp_c[1], k0_c[1], e_c[1], Er_c[1], Q0_c[1],
           Aesp_c[2], k0_c[2], e_c[2], Er_c[2], Q0_c[2])

    sol = root(equations_func, x0=[guess], args=par)
    if sol.success:
        return float(sol.x[0])
    else:
        # No convergió
        return np.nan


def conc(par_ele, par_comp, par_comp_Au, equations_func=equations_alfa, alfa_override=None, f_override=None):
    """
    Calcula la concentración C de un elemento.
    par_ele: (k0_i, e_i, Q0_i, Cn_i, w_i, lamb_i, Er_i, td_i, tr_i, ti_i, tv_i)
    par_comp: tuple de comparadores
    par_comp_Au: tuple del comparador Au
    alfa_override: si se pasa, usa ese alfa en lugar de solucionarlo
    f_override: si se pasa, usa ese f en lugar de calcularlo
    Retorna: (C, alfa, f, variables_2_U)
    variables_2_U es una tupla con los valores que tu código esperaba para cálculo de incertidumbre.
    """
    k0_i, e_i, Q0_i, Cn_i, w_i, lamb_i, Er_i, td_i, tr_i, ti_i, tv_i = par_ele
    k0_c, e_c, Q0_c, Cn_c, w_c, lam_c, Er_c, td_c, tr_c, ti_c, tv_c = par_comp
    k0_c_Au, e_c_Au, Q0_c_Au, Cn_c_Au, w_c_Au, lam_c_Au, Er_c_Au, td_c_Au, tr_c_Au, ti_c_Au, tv_c_Au = par_comp_Au

    # Actividades específicas
    Aesp_i = Aesp(Cn_i, w_i, lamb_i, tr_i, td_i, ti_i, tv_i, e_i)
    Aesp_c_Au = Aesp(Cn_c_Au, w_c_Au, lam_c_Au, tr_c_Au, td_c_Au, ti_c_Au, tv_c_Au, e_c_Au)

    Aesp_c = np.zeros(len(k0_c))
    for j in range(len(k0_c)):
        Aesp_c[j] = Aesp(Cn_c[j], w_c[j], lam_c[j], tr_c[j], td_c[j], ti_c[j], tv_c[j], e_c[j])

    # alfa
    alfa = cal_alfa(par_comp, equations_func) if alfa_override is None else alfa_override
    # Q0_alfa
    Q0_alfa_i = cal_Q0_alfa_i(Q0_i, Er_i, alfa)
    Q0_alfa_c_Au = cal_Q0_alfa_i(Q0_c_Au, Er_c_Au, alfa)
    Q0_alfa_c = np.zeros(len(k0_c))
    for j in range(len(k0_c)):
        Q0_alfa_c[j] = cal_Q0_alfa_i(Q0_c[j], Er_c[j], alfa)

    # f
    f = cal_f_alfa(Q0_alfa_c, Aesp_c, e_c, k0_c) if f_override is None else f_override

    # Concentración
    with np.errstate(all='ignore'):
        C = (Aesp_i / Aesp_c_Au) * (k0_c_Au / k0_i) * (e_c_Au / e_i) * ((f + Q0_alfa_c_Au) / (f + Q0_alfa_i))

    # Construir variables_2_U exactamente como tu código original esperaba
    variables_2_U = (Cn_i, Cn_c[0], Cn_c[1], Cn_c_Au,
                     Er_i, Er_c[0], Er_c[1], Er_c_Au,
                     Q0_i, Q0_c[0], Q0_c[1], Q0_c_Au,
                     alfa, e_i, e_c[0], e_c[1], e_c_Au,
                     k0_i, k0_c[0], k0_c[1], k0_c_Au,
                     lamb_i, lam_c[0], lam_c[1], lam_c_Au,
                     td_i, td_c[0], td_c[1], td_c_Au,
                     ti_i, ti_c[0], ti_c[1], ti_c_Au,
                     tr_i, tr_c[0], tr_c[1], tr_c_Au,
                     tv_i, tv_c[0], tv_c[1], tv_c_Au,
                     w_i, w_c[0], w_c[1], w_c_Au)

    return C, alfa, f, variables_2_U


# --- Incertidumbre: cal_U y cal_U_Aesp usando sympy derivadas ---
def cal_U_Aesp(Val_ini, u_v_ini):
    """
    Calcula incertidumbre para la expresión Aesp (fórmula interna).
    Val_ini: (Cn, lamb, td, ti, tr, tv, w)
    u_v_ini: mismas longitudes con incertidumbres absolutas
    Retorna (u_y, y_val)
    """
    # fórmula dada en tu código
    formula_str = "(Cn*exp(lamb*td)*lamb*tr)/((1-exp(-lamb*ti))*(1-exp(-lamb*tr))*w*tv)"
    # convertir a sympy
    try:
        formula_sym = sp.sympify(formula_str)
    except Exception as e:
        raise RuntimeError(f"Error al interpretar fórmula simbólica en cal_U_Aesp: {e}")

    # extraer variables en orden consistente
    variables = sorted(list(formula_sym.free_symbols), key=lambda x: str(x))
    # mapear valores e incertidumbres por nombre
    valores = {str(v): Val_ini[i] for i, v in enumerate(variables)}
    incertidumbres = {str(v): u_v_ini[i] for i, v in enumerate(variables)}

    # evaluar valor central
    y_val = float(formula_sym.evalf(subs=valores))

    # calcular derivadas parciales y contribuciones
    u_y_squared = 0.0
    contribuciones = []
    for v in variables:
        derivada = float(sp.diff(formula_sym, v).evalf(subs=valores))
        u_i = incertidumbres[str(v)]
        contrib = (derivada * u_i) ** 2
        contribuciones.append((str(v), derivada, u_i, contrib))
        u_y_squared += contrib

    u_y = float(np.sqrt(u_y_squared)) if u_y_squared >= 0 else np.nan
    return u_y, y_val


def cal_U(Val_ini, u_v_ini):
    """
    Calcula la incertidumbre combinada para la fórmula compleja (tu fórmula grande).
    Val_ini: tu tupla larga de valores
    u_v_ini: tu tupla larga de incertidumbres absolutas
    Retorna (u_y, y_val)
    """
    # Montamos la misma fórmula simbólica que tenías en tu código (usada para evaluación y derivadas)
    formula_str = "(Aesp/Aesp_c_Au)*(k0_c_Au/k0)*(e_c_Au/e)*(((k0_1/k0_2)*(e_1/e_2)*((Q0_1 -0.429)/((Er_1)**alfa)+0.429/((2*alfa-1)*0.55**alfa))-(Aesp_1/Aesp_2)*((Q0_2 -0.429)/((Er_2)**alfa)+0.429/((2*alfa-1)*0.55**alfa)))/((Aesp_1/Aesp_2)-(k0_1/k0_2)*(e_1/e_2))+((Q0_c_Au -0.429)/((Er_c_Au)**alfa)+0.429/((2*alfa-1)*0.55**alfa))) / (((k0_1/k0_2)*(e_1/e_2)*((Q0_1 -0.429)/((Er_1)**alfa)+0.429/((2*alfa-1)*0.55**alfa))-(Aesp_1/Aesp_2)*((Q0_2 -0.429)/((Er_2)**alfa)+0.429/((2*alfa-1)*0.55**alfa)))/((Aesp_1/Aesp_2)-(k0_1/k0_2)*(e_1/e_2))+((Q0 -0.429)/((Er)**alfa)+0.429/((2*alfa-1)*0.55**alfa)))"

    try:
        formula_sym = sp.sympify(formula_str)
    except Exception as e:
        raise RuntimeError(f"Error al interpretar fórmula simbólica en cal_U: {e}")

    variables = sorted(list(formula_sym.free_symbols), key=lambda x: str(x))

    # Mapear valores e incertidumbres a los símbolos en el mismo orden
    valores = {str(v): Val_ini[i] for i, v in enumerate(variables)}
    incertidumbres = {str(v): u_v_ini[i] for i, v in enumerate(variables)}

    # Evaluar valor central
    y_val = float(formula_sym.evalf(subs=valores))

    # Calcular derivadas parciales y contribuciones
    u_y_squared = 0.0
    for v in variables:
        derivada = float(sp.diff(formula_sym, v).evalf(subs=valores))
        u_i = incertidumbres[str(v)]
        contrib = (derivada * u_i) ** 2
        u_y_squared += contrib

    u_y = float(np.sqrt(u_y_squared)) if u_y_squared >= 0 else np.nan
    return u_y, y_val



# ----------------------------
# INTERFAZ STREAMLIT
# ----------------------------
st.title("AAN - Procesador k0s / RPT (Streamlit)")
st.markdown("Interfaz adaptada para procesar `.k0s` y `.RPT`, hacer doble validación con Base de Datos, y generar archivos Excel descargables.")

# --- Sidebar: inputs generales
st.sidebar.header("Opciones generales")
# Modo de entrada: Uploads o Ruta local
modo_entrada = st.sidebar.radio("Modo de entrada de archivos", ("Subir archivos (recomendado)", "Ruta local en servidor (opcional)"))

directorio_base_input = ""
if modo_entrada.startswith("Ruta"):
    directorio_base_input = st.sidebar.text_input("Ruta base (ej. /home/user/k0)", value=DEFAULT_DIRECTORIO)

st.sidebar.markdown("---")
st.sidebar.write("Parámetros de lectura")
FILAS_A_OMITIR_RPT = st.sidebar.number_input("Filas a omitir en RPT (skiprows)", value=int(FILAS_A_OMITIR_RPT), min_value=0)
TOLERANCIA_ENERGIA = st.sidebar.number_input("Tolerancia energía (keV)", value=float(TOLERANCIA_ENERGIA))
LINES_TO_READ_K0S = st.sidebar.number_input("Líneas a leer de .k0s", value=int(LINES_TO_READ_K0S), min_value=1)

# --------------------
# STEP 1: Cargar Base de Datos (Excel)
# --------------------
st.header("Paso 1 — Base de Datos (RPT reference / BD)") 
col1, col2 = st.columns([2,1])

with col1:
    st.write("Sube tu archivo `Base de datos.xlsx` (ej. con columnas NUCLIDES y EGKEV).")
    uploaded_bd = st.file_uploader("Subir Base de Datos (Excel)", type=["xlsx", "xls"])
    if modo_entrada.startswith("Ruta"):
        ruta_bd_local = st.text_input("O especifica ruta local al archivo Base de datos.xlsx", value=os.path.join(directorio_base_input, NOMBRE_BASE_DATOS_INFO) if directorio_base_input else "")
    else:
        ruta_bd_local = ""
with col2:
    st.write("Resumen")
    st.write("- Columna energía:", COLUMNA_BD_ENERGIA)
    st.write("- Columna nombre:", COLUMNA_BD_NUCLIDO)

df_info_extra = None
if uploaded_bd is not None:
    try:
        df_info_extra = pd.read_excel(uploaded_bd)
    except Exception as e:
        st.error(f"Error leyendo Base de Datos subida: {e}")
elif ruta_bd_local:
    if os.path.exists(ruta_bd_local):
        try:
            df_info_extra = pd.read_excel(ruta_bd_local)
        except Exception as e:
            st.error(f"Error leyendo Base de Datos local: {e}")
    else:
        st.info("Ruta local Base de Datos no encontrada (aún).")

if df_info_extra is not None:
    # Ajustes de columnas como en tu script
    if COLUMNA_BD_ENERGIA not in df_info_extra.columns:
        if len(df_info_extra.columns) > 6:
            df_info_extra.rename(columns={df_info_extra.columns[6]: COLUMNA_BD_ENERGIA}, inplace=True)
    if COLUMNA_BD_NUCLIDO not in df_info_extra.columns:
        if len(df_info_extra.columns) > 1:
            df_info_extra.rename(columns={df_info_extra.columns[1]: COLUMNA_BD_NUCLIDO}, inplace=True)

    df_info_extra[COLUMNA_BD_ENERGIA] = pd.to_numeric(df_info_extra[COLUMNA_BD_ENERGIA], errors='coerce')
    df_info_extra.dropna(subset=[COLUMNA_BD_ENERGIA], inplace=True)
    df_info_extra['NOMBRE_LIMPIO_BD'] = df_info_extra[COLUMNA_BD_NUCLIDO].apply(limpiar_nombre)
    st.success("Base de Datos cargada correctamente.")
    st.dataframe(df_info_extra.head())

# --------------------
# STEP 2: Cargar archivo .k0s (Muestra)
# --------------------
st.header("Paso 2 — Archivo .k0s (muestra)")
col1, col2 = st.columns([2,1])
with col1:
    uploaded_k0s = st.file_uploader("Subir archivo muestra `.k0s`", type=["k0s","txt"])
    if modo_entrada.startswith("Ruta"):
        ruta_k0s_local = st.text_input("O ruta local a .k0s (muestra)", value=os.path.join(directorio_base_input, "muestra.k0s") if directorio_base_input else "")
    else:
        ruta_k0s_local = ""
with col2:
    st.write("Acciones")
    btn_generate_k0s = st.button("Generar _k0s.xlsx desde .k0s (muestra)")

df_k0s_metadata = None
k0s_xlsx_bytes = None
if btn_generate_k0s:
    if uploaded_k0s is None and not ruta_k0s_local:
        st.warning("Sube un archivo .k0s o especifica ruta local.")
    else:
        if uploaded_k0s is not None:
            df_k0s_metadata = extract_and_tokenize_metadata_filelike(uploaded_k0s, num_lines=int(LINES_TO_READ_K0S))
        else:
            # ruta local
            if os.path.exists(ruta_k0s_local):
                df_k0s_metadata = procesar_k0s_independiente(ruta_k0s_local, ruta_k0s_local + "_tmp.xlsx", lines_to_read=int(LINES_TO_READ_K0S))
                # eliminar temporal
                try:
                    os.remove(ruta_k0s_local + "_tmp.xlsx")
                except: pass
            else:
                st.error("Ruta local .k0s no encontrada.")
                df_k0s_metadata = None

        if df_k0s_metadata is not None:
            st.success("Metadatos extraídos del .k0s")
            st.dataframe(df_k0s_metadata)
            # preparar archivo excel para descarga
            towrite = io.BytesIO()
            with pd.ExcelWriter(towrite, engine='openpyxl') as writer:
                df_k0s_metadata.to_excel(writer, index=False, header=False)
            towrite.seek(0)
            k0s_xlsx_bytes = towrite.read()
            st.download_button("Descargar _k0s.xlsx", data=k0s_xlsx_bytes, file_name="muestra_k0s.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            # Extraer variables clave
            f_med, hora_med, t_v, t_r = extraer_variables_clave(df_k0s_metadata)
            if f_med is not None:
                st.write("Variables extraídas:")
                st.write(f"- Fecha medición: {f_med}")
                st.write(f"- Hora medición: {hora_med}")
                st.write(f"- Tiempo vivo (t_v): {t_v}")
                st.write(f"- Tiempo real (t_r): {t_r}")
        else:
            st.error("No se pudo extraer metadatos del .k0s.")

# --------------------
# STEP 3: Cargar archivo .RPT (muestra)
# --------------------
st.header("Paso 3 — Archivo .RPT (muestra)")
col1, col2 = st.columns([2,1])
with col1:
    uploaded_rpt = st.file_uploader("Subir archivo muestra `.RPT`", type=["rpt","txt","csv"])
    if modo_entrada.startswith("Ruta"):
        ruta_rpt_local = st.text_input("O ruta local a .RPT (muestra)", value=os.path.join(directorio_base_input, "muestra.RPT") if directorio_base_input else "")
    else:
        ruta_rpt_local = ""
with col2:
    st.write("Acciones")
    nombre_geometria = st.radio("Selecciona geometría RDN:", ("C","M","L"))
    uploaded_rdn = st.file_uploader("Subir RDN_xxx.xlsx (Referencia energética) (opcional)", type=["xlsx","xls"])
    btn_process_rpt = st.button("Procesar RPT (muestra) y comparar energías")

df_rpt = None
df_verificado = None
ruta_verif_bytes = None
df_final_unificado = None
ruta_final_bytes = None

if btn_process_rpt:
    # Cargar RPT
    if uploaded_rpt is None and not ruta_rpt_local:
        st.warning("Sube un archivo .RPT o especifica ruta local.")
    else:
        df_rpt = read_rpt_to_df(uploaded_rpt if uploaded_rpt is not None else ruta_rpt_local, skiprows=int(FILAS_A_OMITIR_RPT))
        if df_rpt is None:
            st.error("No se pudo leer el RPT.")
        else:
            st.success("RPT leído correctamente.")
            st.dataframe(df_rpt.head())

            # Cargar RDN según geometría: desde upload o ruta local por convención de nombre RDN_C.xlsx, etc.
            df_nuclidos = None
            if uploaded_rdn is not None:
                try:
                    df_nuclidos = pd.read_excel(uploaded_rdn)
                except Exception as e:
                    st.error(f"Error leyendo RDN subido: {e}")
            else:
                # intentar ruta local
                nombre_ref_energia = f'RDN_{nombre_geometria}.xlsx'
                ruta_ref_local = os.path.join(directorio_base_input, nombre_ref_energia) if directorio_base_input else nombre_ref_energia
                if os.path.exists(ruta_ref_local):
                    try:
                        df_nuclidos = pd.read_excel(ruta_ref_local)
                    except Exception as e:
                        st.error(f"Error leyendo RDN local: {e}")
                else:
                    st.info("No se proporcionó RDN; puedes subirlo para mejorar la verificación por energía.")

            if df_nuclidos is not None:
                # normalizar
                col_en_excel = df_nuclidos.columns[1]
                df_nuclidos[col_en_excel] = pd.to_numeric(df_nuclidos[col_en_excel], errors='coerce')
                df_nuclidos.dropna(subset=[col_en_excel], inplace=True)
                st.write("Referencia RDN cargada. Mostrando primeras filas:")
                st.dataframe(df_nuclidos.head())

            # Buscar por energía
            st.write("Comparando energías con referencia RDN (si está disponible)...")
            if df_nuclidos is not None:
                # crear función lambda que use df_nuclidos
                df_rpt['Identidad_Verificada_Energia'] = df_rpt['Energy_keV'].apply(lambda v: buscar_identidad_por_energia(df_nuclidos, v, tolerancia=float(TOLERANCIA_ENERGIA)))
            else:
                df_rpt['Identidad_Verificada_Energia'] = 'Desconocido'

            df_verificado = df_rpt[df_rpt['Identidad_Verificada_Energia'] != 'Desconocido'].copy()
            if not df_verificado.empty:
                st.success(f"Se encontraron {len(df_verificado)} coincidencias por energía.")
                st.dataframe(df_verificado.head())
                # preparar verificado para descarga
                towrite = io.BytesIO()
                with pd.ExcelWriter(towrite, engine='openpyxl') as writer:
                    df_verificado.to_excel(writer, index=False)
                towrite.seek(0)
                ruta_verif_bytes = towrite.read()
                st.download_button("Descargar _VERIFICADO.xlsx", data=ruta_verif_bytes, file_name="muestra_VERIFICADO.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.warning("RPT sin coincidencias de energía. No se generó archivo VERIFICADO.")

            # Si existe Base de Datos cargada, hacer doble validación (energía + nombre)
            if df_info_extra is not None and df_verificado is not None and not df_verificado.empty:
                st.write("Realizando cruce doble (energía + nombre) con Base de Datos...")
                df_final_unificado = process_double_validation(df_verificado, df_info_extra, tolerancia=float(TOLERANCIA_ENERGIA))
                if df_final_unificado is not None:
                    st.success(f"Se encontraron {len(df_final_unificado)} filas con doble coincidencia (Energía + Nombre).")
                    st.dataframe(df_final_unificado.head())
                    # preparar descarga
                    towrite = io.BytesIO()
                    with pd.ExcelWriter(towrite, engine='openpyxl') as writer:
                        df_final_unificado.to_excel(writer, index=False)
                    towrite.seek(0)
                    ruta_final_bytes = towrite.read()
                    st.download_button("Descargar _FINAL_UNIFICADO.xlsx", data=ruta_final_bytes, file_name="muestra_FINAL_UNIFICADO.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                else:
                    st.warning("Hubo coincidencias por energía pero ninguna coincidió también por nombre.")
            else:
                st.info("No se pudo realizar la doble validación porque falta la Base de Datos o no hay coincidencias por energía.")

# --------------------
# STEP 4: Procesador comparador Au
# --------------------
st.header("Paso 4 — Procesador comparador Au (archivo .k0s y .RPT del comparador)")
col1, col2 = st.columns([2,1])
with col1:
    uploaded_k0s_au = st.file_uploader("Subir .k0s del comparador Au", type=["k0s","txt"], key="k0s_au")
    uploaded_rpt_au = st.file_uploader("Subir .RPT del comparador Au", type=["rpt","txt","csv"], key="rpt_au")
    if modo_entrada.startswith("Ruta"):
        ruta_k0s_au_local = st.text_input("O ruta local a .k0s comparador Au", value=os.path.join(directorio_base_input, "au.k0s") if directorio_base_input else "", key="ruta_k0s_au")
        ruta_rpt_au_local = st.text_input("O ruta local a .RPT comparador Au", value=os.path.join(directorio_base_input, "au.RPT") if directorio_base_input else "", key="ruta_rpt_au")
    else:
        ruta_k0s_au_local = ""
        ruta_rpt_au_local = ""
with col2:
    w_i = st.text_input("Masa de la muestra w_i (g)", value="")
    w_i_c_Au = st.text_input("Masa del comparador w_i_c_Au (ug)", value="")
    btn_process_au = st.button("Procesar comparador Au")

if btn_process_au:
    # 1) Extraer metadata k0s comparador
    df_k0s_au = None
    if uploaded_k0s_au is not None:
        df_k0s_au = extract_and_tokenize_metadata_filelike(uploaded_k0s_au, num_lines=int(LINES_TO_READ_K0S))
    elif ruta_k0s_au_local and os.path.exists(ruta_k0s_au_local):
        df_k0s_au = procesar_k0s_independiente(ruta_k0s_au_local, ruta_k0s_au_local + "_tmp.xlsx", lines_to_read=int(LINES_TO_READ_K0S))
        try:
            os.remove(ruta_k0s_au_local + "_tmp.xlsx")
        except: pass
    else:
        st.warning("No se proporcionó .k0s del comparador Au.")

    if df_k0s_au is not None:
        f_med_c_Au, hora_med_c_Au, t_v_c_Au, t_r_c_Au = extraer_variables_clave(df_k0s_au)
        st.write("Variables extraídas del k0s (Au):")
        st.write(f"- Fecha: {f_med_c_Au}")
        st.write(f"- Hora: {hora_med_c_Au}")
        st.write(f"- Tiempo vivo: {t_v_c_Au}")
        st.write(f"- Tiempo real: {t_r_c_Au}")
    else:
        f_med_c_Au = hora_med_c_Au = t_v_c_Au = t_r_c_Au = None

    # 2) Leer RPT comparador Au y buscar línea AU-198
    rpt_content = None
    if uploaded_rpt_au is not None:
        rpt_content = uploaded_rpt_au.read().decode('latin-1', errors='replace')
    elif ruta_rpt_au_local and os.path.exists(ruta_rpt_au_local):
        with open(ruta_rpt_au_local, 'r', encoding='latin-1', errors='ignore') as f:
            rpt_content = f.read()
    else:
        st.warning("No se proporcionó .RPT del comparador Au.")

    if rpt_content:
        line_found = find_line_with_target(rpt_content, "AU-198")
        if line_found is None:
            st.error("No se encontró 'AU-198' en el archivo RPT del comparador.")
        else:
            st.write("Línea encontrada para AU-198:")
            st.text(line_found)
            tokens = re.split(r"\s{2,}", line_found)
            columnas = [
                "Peak",
                "Peak No",
                "ROI start",
                "ROI end",
                "peak centroid",
                "Energy (keV)",
                "Net Peak Area",
                "Net Area Uncert.",
                "Continuum Counts",
                "Tentative Nuclide"
            ]
            data = {col: [tokens[i]] if i < len(tokens) else [None] for i, col in enumerate(columnas)}
            df_au = pd.DataFrame(data)
            # Convertir a num donde corresponde
            try:
                Cn_c_Au_input = float(df_au.loc[0,"Net Peak Area"])
            except:
                Cn_c_Au_input = np.nan
            try:
                u_Cn_c_Au_input = float(df_au.loc[0,"Net Area Uncert."])
            except:
                u_Cn_c_Au_input = np.nan

            st.write(f"Área del pico AU (Cn_c_Au): {Cn_c_Au_input}")
            st.write(f"Incertidumbre área AU (u_Cn_c_Au): {u_Cn_c_Au_input}")

            # Ofrecer descarga de la fila procesada en excel
            towrite = io.BytesIO()
            with pd.ExcelWriter(towrite, engine='openpyxl') as writer:
                df_au.to_excel(writer, index=False)
            towrite.seek(0)
            st.download_button("Descargar fila AU-198 (Excel)", data=towrite.read(), file_name="AU198_extracted.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# --------------------
# FIN
# --------------------
st.markdown("---")
st.write("Proceso finalizado. Usa los botones en cada paso para generar y descargar los archivos Excel. Si quieres que automatice pasos adicionales (cálculo de concentraciones con Au, cálculo de incertidumbres, o integración de picos), dime y extiendo la app con esos cálculos.")
