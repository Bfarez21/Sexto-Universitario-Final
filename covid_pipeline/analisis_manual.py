# manual_analysis.py
import pandas as pd
import requests
import numpy as np

# Descargar datos
url = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"
df = pd.read_csv(url)

# Exploración básica
print(" INFORMACIÓN BÁSICA:")
print(f"Filas: {len(df)}, Columnas: {len(df.columns)}")
print(f"Rango de fechas: {df['date'].min()} to {df['date'].max()}")
print(f"Países únicos: {df['country'].nunique()}")

# Verificar estructura
print("\n COLUMNAS DISPONIBLES:")
for i, col in enumerate(df.columns, 1):
    print(f"{i:3d}. {col}")

# Filtrar para Ecuador y Perú
paises = ["Ecuador", "Peru"]
df_filtrado = df[df['country'].isin(paises)]

print(f"\n DATOS PARA {paises}:")
print(f"Registros: {len(df_filtrado)}")
print(f"Rango fechas: {df_filtrado['date'].min()} to {df_filtrado['date'].max()}")

# Verificar columnas críticas
columnas_criticas = ['country', 'date', 'new_cases', 'people_vaccinated', 'population']
for col in columnas_criticas:
    exists = col in df.columns
    print(f"{col}: {'✅' if exists else '❌'}")

# Crear tabla de perfilado manual
perfilado = {
    'metrica': [
        'total_registros',
        'total_paises',
        'fecha_minima',
        'fecha_maxima',
        'columnas_totales',
        'new_cases_presente',
        'people_vaccinated_presente',
        'population_presente'
    ],
    'valor': [
        len(df),
        df['country'].nunique(),
        df['date'].min(),
        df['date'].max(),
        len(df.columns),
        'new_cases' in df.columns,
        'people_vaccinated' in df.columns,
        'population' in df.columns
    ],
    'descripcion': [
        'Total de registros en el dataset',
        'Número de países únicos',
        'Fecha más antigua en los datos',
        'Fecha más reciente en los datos',
        'Número total de columnas',
        'Columna new_cases presente',
        'Columna people_vaccinated presente',
        'Columna population presente'
    ]
}

df_perfilado = pd.DataFrame(perfilado)
df_perfilado.to_csv('tabla_perfilado_manual.csv', index=False)
print(" Tabla de perfilado manual guardada como 'tabla_perfilado_manual.csv'")

# Mostrar muestra de datos
print("\n🔍 MUESTRA DE DATOS (Ecuador y Perú):")
print(df_filtrado[['country', 'date', 'new_cases', 'people_vaccinated', 'population']].head(10))