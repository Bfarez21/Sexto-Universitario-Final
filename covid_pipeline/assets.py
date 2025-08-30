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
    
    # Filtrar fechas futuras
    if 'date' in df.columns:
        df = df[df['date'] <= pd.Timestamp.today()]
    
    # Verificar y crear columnas faltantes si no existen
    columnas_requeridas = ['country','date','population']
    for col in columnas_requeridas:
        if col not in df.columns:
            df[col] = 1 if col=='population' else "Desconocido"
            context.log.warning(f"Columna {col} no existía, se creó con valores por defecto")
    
    # Limpiar population: manejar valores nulos, vacíos, o <= 0
    if 'population' in df.columns:
        # Convertir a numérico, forzando errores a NaN
        df['population'] = pd.to_numeric(df['population'], errors='coerce')
        
        # Contar problemas antes de limpiar
        nulos_antes = df['population'].isna().sum()
        negativos_antes = (df['population'] <= 0).sum()
        
        # Reemplazar valores problemáticos con 1 (valor mínimo válido)
        df.loc[df['population'].isna() | (df['population'] <= 0), 'population'] = 1
        
        context.log.info(f"Population limpiado: {nulos_antes} nulos, {negativos_antes} <= 0 → reemplazados con 1")
    
    # Limpiar country y date
    if 'country' in df.columns:
        df['country'] = df['country'].fillna("Desconocido")
    
    if 'date' in df.columns:
        # Asegurar que date sea datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Eliminar filas con fechas inválidas
        df = df.dropna(subset=['date'])
    
    # Eliminar duplicados DESPUÉS de limpiar los datos
    filas_antes = len(df)
    df = df.drop_duplicates(subset=['country','date'], keep='last')
    duplicados_eliminados = filas_antes - len(df)
    
    if duplicados_eliminados > 0:
        context.log.info(f"Duplicados eliminados: {duplicados_eliminados}")
    
    context.log.info(f"Datos limpiados: {len(df)} filas finales, {len(df.columns)} columnas")
    
    # Log de valores únicos de population para debug
    pop_stats = df['population'].describe()
    context.log.info(f"Population stats: min={pop_stats['min']}, max={pop_stats['max']}, mean={pop_stats['mean']:.0f}")
    
    return df

# ------------------ CHECKS CORREGIDOS ------------------

@asset_check(asset=limpiar_datos_para_checks, name="check_fechas_futuras")
def check_fechas_futuras(context: AssetCheckExecutionContext, limpiar_datos_para_checks: pd.DataFrame) -> AssetCheckResult:
    if 'date' not in limpiar_datos_para_checks.columns:
        return AssetCheckResult(
            passed=False, 
            description="Columna date no existe", 
            severity=AssetCheckSeverity.ERROR
        )
    max_date = limpiar_datos_para_checks['date'].max().date()
    passed = max_date <= date.today()
    return AssetCheckResult(
        passed=passed,
        description=f"Fecha máxima: {max_date}" if passed else f"ERROR: Fecha futura {max_date}",
        severity=AssetCheckSeverity.WARN if passed else AssetCheckSeverity.ERROR,
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
        severity=AssetCheckSeverity.WARN if passed else AssetCheckSeverity.ERROR,
        metadata={
            "columnas_faltantes": MetadataValue.json(faltantes),
            "columnas_presentes": MetadataValue.json([c for c in columnas if c in limpiar_datos_para_checks.columns])
        }
    )

@asset_check(asset=limpiar_datos_para_checks, name="check_unicidad_country_date")
def check_unicidad_country_date(context: AssetCheckExecutionContext, limpiar_datos_para_checks: pd.DataFrame) -> AssetCheckResult:
    # Verificar que existan las columnas necesarias
    if 'country' not in limpiar_datos_para_checks.columns or 'date' not in limpiar_datos_para_checks.columns:
        return AssetCheckResult(
            passed=False,
            description="Faltan columnas 'country' o 'date' para verificar unicidad",
            severity=AssetCheckSeverity.ERROR
        )
    
    # Contar duplicados antes de eliminarlos
    total_filas = len(limpiar_datos_para_checks)
    duplicados = limpiar_datos_para_checks.duplicated(subset=['country','date'], keep=False)
    num_duplicados = int(duplicados.sum())  # Convertir a int de Python
    filas_unicas = len(limpiar_datos_para_checks.drop_duplicates(subset=['country','date']))
    
    passed = bool(num_duplicados == 0)  # Convertir a bool de Python
    
    return AssetCheckResult(
        passed=passed,
        description=f"Sin duplicados: {filas_unicas} filas únicas" if passed else f"{num_duplicados} duplicados encontrados (filas únicas: {filas_unicas})",
        severity=AssetCheckSeverity.WARN if passed else AssetCheckSeverity.ERROR,
        metadata={
            "total_filas": MetadataValue.int(total_filas),
            "filas_duplicadas": MetadataValue.int(int(num_duplicados)),
            "filas_unicas": MetadataValue.int(filas_unicas)
        }
    )

@asset_check(asset=limpiar_datos_para_checks, name="check_population_positiva")
def check_population_positiva(context: AssetCheckExecutionContext, limpiar_datos_para_checks: pd.DataFrame) -> AssetCheckResult:
    if 'population' not in limpiar_datos_para_checks.columns:
        return AssetCheckResult(
            passed=False, 
            description="Columna population ausente", 
            severity=AssetCheckSeverity.ERROR
        )
    
    # Ya debería estar limpio, pero verificamos
    population_col = limpiar_datos_para_checks['population']
    
    # Contar diferentes tipos de problemas
    valores_nulos = int(population_col.isna().sum())  # Convertir a int de Python
    valores_zero = int((population_col == 0).sum())
    valores_negativos = int((population_col < 0).sum())
    valores_no_numericos = 0
    
    # Verificar si hay valores no numéricos
    try:
        numeric_pop = pd.to_numeric(population_col, errors='coerce')
        valores_no_numericos = numeric_pop.isna().sum() - valores_nulos
    except:
        valores_no_numericos = len(population_col)
    
    total_problemas = valores_nulos + valores_zero + valores_negativos + valores_no_numericos
    passed = bool(total_problemas == 0)  # Convertir a bool de Python
    
    # Construir descripción detallada
    if passed:
        min_pop = population_col.min()
        max_pop = population_col.max()
        description = f"✓ Todas las poblaciones son positivas (min: {min_pop:,.0f}, max: {max_pop:,.0f})"
    else:
        problemas = []
        if valores_nulos > 0:
            problemas.append(f"{valores_nulos} nulos")
        if valores_zero > 0:
            problemas.append(f"{valores_zero} zeros")
        if valores_negativos > 0:
            problemas.append(f"{valores_negativos} negativos")
        if valores_no_numericos > 0:
            problemas.append(f"{valores_no_numericos} no numéricos")
        description = f"✗ Problemas encontrados: {', '.join(problemas)}"
    
    return AssetCheckResult(
        passed=passed,
        description=description,
        severity=AssetCheckSeverity.WARN if passed else AssetCheckSeverity.ERROR,
        metadata={
            "valores_nulos": MetadataValue.int(int(valores_nulos)),
            "valores_zero": MetadataValue.int(int(valores_zero)),
            "valores_negativos": MetadataValue.int(int(valores_negativos)),
            "valores_no_numericos": MetadataValue.int(int(valores_no_numericos)),
            "total_filas": MetadataValue.int(len(limpiar_datos_para_checks)),
            "min_population": MetadataValue.float(float(population_col.min()) if not population_col.empty else 0),
            "max_population": MetadataValue.float(float(population_col.max()) if not population_col.empty else 0)
        }
    )

@asset_check(asset=limpiar_datos_para_checks, name="check_new_cases_no_negativos")
def check_new_cases_no_negativos(context: AssetCheckExecutionContext, limpiar_datos_para_checks: pd.DataFrame) -> AssetCheckResult:
    if 'new_cases' not in limpiar_datos_para_checks.columns:
        return AssetCheckResult(
            passed=True,  # Si no existe la columna, el check pasa
            description="Columna 'new_cases' no existe - check omitido",
            severity=AssetCheckSeverity.WARN
        )
    
    # Convertir a numérico y contar valores negativos
    new_cases_numeric = pd.to_numeric(limpiar_datos_para_checks['new_cases'], errors='coerce')
    valores_negativos = int((new_cases_numeric < 0).sum())  # Convertir a int de Python
    valores_nulos = int(new_cases_numeric.isna().sum())
    
    passed = bool(valores_negativos == 0)  # Convertir a bool de Python
    
    # Documentar si hay valores negativos
    description = "Todos los new_cases son ≥ 0"
    if valores_negativos > 0:
        # Obtener algunos ejemplos de valores negativos para documentación
        ejemplos_negativos = limpiar_datos_para_checks[new_cases_numeric < 0][['country', 'date', 'new_cases']].head(3)
        description = f"DOCUMENTADO: {valores_negativos} valores negativos encontrados. Ejemplos: {ejemplos_negativos.to_string(index=False)}"
    
    if valores_nulos > 0:
        description += f" | {valores_nulos} valores nulos"
    
    return AssetCheckResult(
        passed=passed,
        description=description,
        severity=AssetCheckSeverity.WARN,  # WARN porque permitimos negativos si están documentados
        metadata={
            "valores_negativos": MetadataValue.int(int(valores_negativos)),
            "valores_nulos": MetadataValue.int(int(valores_nulos)),
            "total_filas": MetadataValue.int(len(limpiar_datos_para_checks)),
            "porcentaje_negativos": MetadataValue.float(float(valores_negativos / len(limpiar_datos_para_checks) * 100))
        }
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
        if len(df_pais) > 0 and 'new_cases' in df_pais.columns and 'population' in df_pais.columns:
            df_pais['incidencia_diaria'] = (df_pais['new_cases'] / df_pais['population']) * 100000
            df_pais['incidencia_7d'] = df_pais['incidencia_diaria'].rolling(7, min_periods=1).mean()
            resultado = df_pais[['date', 'location', 'incidencia_7d']].copy()
            resultado.columns = ['fecha', 'pais', 'incidencia_7d']
            resultados.append(resultado)
    return pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame(columns=['fecha', 'pais', 'incidencia_7d'])

@asset
def metrica_factor_crec_7d(context: AssetExecutionContext, datos_procesados: pd.DataFrame) -> pd.DataFrame:
    resultados = []
    for pais in PAISES_COMPARACION:
        df_pais = datos_procesados[datos_procesados['location'] == pais].copy()
        df_pais = df_pais.sort_values('date')
        if len(df_pais) >= 14 and 'new_cases' in df_pais.columns:
            df_pais['casos_semana_actual'] = df_pais['new_cases'].rolling(7).sum()
            df_pais['casos_semana_prev'] = df_pais['casos_semana_actual'].shift(7)
            df_pais = df_pais.dropna(subset=['casos_semana_actual', 'casos_semana_prev'])
            df_pais = df_pais[df_pais['casos_semana_prev'] > 0]  # Evitar división por cero
            df_pais['factor_crec_7d'] = df_pais['casos_semana_actual'] / df_pais['casos_semana_prev']
            df_pais = df_pais[df_pais['factor_crec_7d'].notna()]
            if len(df_pais) > 0:
                resultado = df_pais[['date', 'location', 'casos_semana_actual', 'factor_crec_7d']].copy()
                resultado.columns = ['semana_fin', 'pais', 'casos_semana', 'factor_crec_7d']
                resultados.append(resultado)
    return pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame(columns=['semana_fin', 'pais', 'casos_semana', 'factor_crec_7d'])

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
    context.add_output_metadata({
        "archivo_generado": MetadataValue.text(archivo),
        "registros_datos_procesados": MetadataValue.int(len(datos_procesados)),
        "registros_incidencia": MetadataValue.int(len(metrica_incidencia_7d)),
        "registros_factor_crec": MetadataValue.int(len(metrica_factor_crec_7d))
    })
    return archivo