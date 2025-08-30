# assets.py
import pandas as pd
import os
from datetime import datetime, date
from dagster import (
    asset, AssetCheckResult, asset_check, AssetExecutionContext,
    AssetCheckExecutionContext, AssetCheckSeverity, MetadataValue
)

COVID_URL = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"
PAISES_COMPARACION = ["Ecuador", "Peru"]

@asset
def leer_datos(context: AssetExecutionContext) -> pd.DataFrame:
    try:
        df = pd.read_csv(COVID_URL)
        context.log.info("Datos descargados desde URL")
    except Exception as e:
        context.log.warning(f"No se pudo descargar: {e}")
        local_path = os.path.join(os.getcwd(), "compact.csv")
        if os.path.exists(local_path):
            df = pd.read_csv(local_path)
        else:
            df = pd.DataFrame(columns=["country","date","population"])
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

@asset
def limpiar_datos_para_checks(context: AssetExecutionContext, leer_datos: pd.DataFrame) -> pd.DataFrame:
    df = leer_datos.copy()
    if 'date' in df.columns:
        df = df[df['date'] <= pd.Timestamp.today()]
    columnas_requeridas = ['country','date','population']
    for col in columnas_requeridas:
        if col not in df.columns:
            df[col] = 1 if col=='population' else "Desconocido"
    df = df.drop_duplicates(subset=['country','date'])
    df.loc[df['population']<=0,'population']=1
    return df

# ------------------ CHECKS ------------------

@asset_check(asset=limpiar_datos_para_checks, name="check_fechas_futuras")
def check_fechas_futuras(context: AssetCheckExecutionContext, limpiar_datos_para_checks: pd.DataFrame) -> AssetCheckResult:
    if 'date' not in limpiar_datos_para_checks.columns:
        return AssetCheckResult(
            passed=False, 
            description="Columna date no existe", 
            severity=AssetCheckSeverity.FAILURE
        )
    max_date = limpiar_datos_para_checks['date'].max().date()
    passed = max_date <= date.today()
    return AssetCheckResult(
        passed=passed,
        description=f"Fecha máxima: {max_date}" if passed else f"ERROR: Fecha futura {max_date}",
        severity=AssetCheckSeverity.SUCCESS if passed else AssetCheckSeverity.FAILURE,
        metadata={"fecha_maxima": MetadataValue.text(str(max_date))}
    )

@asset_check(asset=limpiar_datos_para_checks, name="check_columnas_clave")
def check_columnas_clave(context: AssetCheckExecutionContext, limpiar_datos_para_checks: pd.DataFrame) -> AssetCheckResult:
    columnas = ['country','date','population']
    faltantes = [c for c in columnas if c not in limpiar_datos_para_checks.columns]
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        description="Todas columnas presentes" if passed else f"Faltan columnas: {faltantes}",
        severity=AssetCheckSeverity.SUCCESS if passed else AssetCheckSeverity.FAILURE,
        metadata={
            "columnas_faltantes": MetadataValue.json(faltantes),
            "columnas_presentes": MetadataValue.json([c for c in columnas if c in limpiar_datos_para_checks.columns])
        }
    )

@asset_check(asset=limpiar_datos_para_checks, name="check_unicidad_country_date")
def check_unicidad_country_date(context: AssetCheckExecutionContext, limpiar_datos_para_checks: pd.DataFrame) -> AssetCheckResult:
    dup = limpiar_datos_para_checks.duplicated(subset=['country','date']).sum()
    passed = dup==0
    return AssetCheckResult(
        passed=passed,
        description="Sin duplicados" if passed else f"{dup} duplicados encontrados",
        severity=AssetCheckSeverity.SUCCESS if passed else AssetCheckSeverity.FAILURE,
        metadata={"duplicados": MetadataValue.int(dup)}
    )

@asset_check(asset=limpiar_datos_para_checks, name="check_population_positiva")
def check_population_positiva(context: AssetCheckExecutionContext, limpiar_datos_para_checks: pd.DataFrame) -> AssetCheckResult:
    if 'population' not in limpiar_datos_para_checks.columns:
        return AssetCheckResult(
            passed=False, 
            description="Columna population ausente", 
            severity=AssetCheckSeverity.FAILURE
        )
    neg = (limpiar_datos_para_checks['population']<=0).sum()
    passed = neg==0
    return AssetCheckResult(
        passed=passed,
        description="Population positiva" if passed else f"{neg} valores no positivos",
        severity=AssetCheckSeverity.SUCCESS if passed else AssetCheckSeverity.FAILURE,
        metadata={"filas_afectadas": MetadataValue.int(neg)}
    )

# ------------------ ASSETS PROCESADOS ------------------

@asset
def datos_procesados(context: AssetExecutionContext, leer_datos: pd.DataFrame) -> pd.DataFrame:
    context.log.info(f"Procesando datos para: {PAISES_COMPARACION}")
    df_filtrado = leer_datos[leer_datos['country'].isin(PAISES_COMPARACION)].copy()
    columnas_posibles = ['country', 'date', 'new_cases', 'people_vaccinated', 'population']
    columnas_existentes = [col for col in columnas_posibles if col in df_filtrado.columns]
    df_procesado = df_filtrado[columnas_existentes].copy()
    columnas_limpiar = [col for col in ['new_cases', 'people_vaccinated'] if col in df_procesado.columns]
    if columnas_limpiar:
        df_procesado = df_procesado.dropna(subset=columnas_limpiar)
    if 'country' in df_procesado.columns:
        df_procesado = df_procesado.rename(columns={'country': 'location'})
    df_procesado = df_procesado.sort_values(['location', 'date'])
    context.add_output_metadata({
        "registros_procesados": MetadataValue.int(len(df_procesado)),
        "columnas_finales": MetadataValue.json(list(df_procesado.columns)),
        "paises_procesados": MetadataValue.json(PAISES_COMPARACION)
    })
    return df_procesado

@asset
def metrica_incidencia_7d(context: AssetExecutionContext, datos_procesados: pd.DataFrame) -> pd.DataFrame:
    resultados = []
    for pais in PAISES_COMPARACION:
        df_pais = datos_procesados[datos_procesados['location'] == pais].copy()
        if len(df_pais) > 0:
            df_pais['incidencia_diaria'] = (df_pais['new_cases'] / df_pais['population']) * 100000
            df_pais['incidencia_7d'] = df_pais['incidencia_diaria'].rolling(7, min_periods=1).mean()
            resultado = df_pais[['date', 'location', 'incidencia_7d']]
            resultado.columns = ['fecha', 'pais', 'incidencia_7d']
            resultados.append(resultado)
    return pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()

@asset
def metrica_factor_crec_7d(context: AssetExecutionContext, datos_procesados: pd.DataFrame) -> pd.DataFrame:
    resultados = []
    for pais in PAISES_COMPARACION:
        df_pais = datos_procesados[datos_procesados['location'] == pais].copy()
        df_pais = df_pais.sort_values('date')
        if len(df_pais) >= 14:
            df_pais['casos_semana_actual'] = df_pais['new_cases'].rolling(7).sum()
            df_pais['casos_semana_prev'] = df_pais['casos_semana_actual'].shift(7)
            df_pais = df_pais.dropna(subset=['casos_semana_actual', 'casos_semana_prev'])
            df_pais['factor_crec_7d'] = df_pais['casos_semana_actual'] / df_pais['casos_semana_prev']
            df_pais = df_pais[df_pais['factor_crec_7d'].notna()]
            if len(df_pais) > 0:
                resultado = df_pais[['date', 'location', 'casos_semana_actual', 'factor_crec_7d']]
                resultado.columns = ['semana_fin', 'pais', 'casos_semana', 'factor_crec_7d']
                resultados.append(resultado)
    return pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()

@asset
def reporte_excel_covid(context: AssetExecutionContext, datos_procesados: pd.DataFrame, 
                        metrica_incidencia_7d: pd.DataFrame, metrica_factor_crec_7d: pd.DataFrame) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo = f"reporte_covid_{timestamp}.xlsx"
    with pd.ExcelWriter(archivo, engine='openpyxl') as writer:
        datos_procesados.to_excel(writer, sheet_name='Datos_Procesados', index=False)
        metrica_incidencia_7d.to_excel(writer, sheet_name='Incidencia_7d', index=False)
        metrica_factor_crec_7d.to_excel(writer, sheet_name='Factor_Crecimiento', index=False)
    context.log.info(f"Reporte exportado: {archivo}")
    return archivo
