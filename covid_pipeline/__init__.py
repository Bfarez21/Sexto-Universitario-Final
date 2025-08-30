from dagster import Definitions
from .assets import (
    leer_datos,
    limpiar_datos_para_checks,
    datos_procesados,
    metrica_incidencia_7d,
    metrica_factor_crec_7d,
    reporte_excel_covid,
    check_fechas_futuras,
    check_columnas_clave,
    check_unicidad_country_date,
    check_population_positiva,
    check_new_cases_no_negativos
)

defs = Definitions(
    assets=[
        leer_datos,
        limpiar_datos_para_checks,
        datos_procesados,
        metrica_incidencia_7d,
        metrica_factor_crec_7d,
        reporte_excel_covid
    ],
    asset_checks=[
        check_fechas_futuras,
        check_columnas_clave,
        check_unicidad_country_date,
        check_population_positiva,
        check_new_cases_no_negativos
    ]
)
