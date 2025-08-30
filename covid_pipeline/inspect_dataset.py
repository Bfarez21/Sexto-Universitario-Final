
# script para visualizar la estructura de los datos a usar

import pandas as pd
import requests

def inspeccionar_dataset():
    """Inspecciona la estructura real del dataset COVID-19"""
    
    url = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"
    
    print("=== INSPECCIONANDO DATASET COVID-19 ===")
    print(f"URL: {url}")
    
    try:
        # Leer solo las primeras 5 filas para inspeccionar
        df = pd.read_csv(url, nrows=5)
        
        print(f"\n📊 INFORMACIÓN BÁSICA:")
        print(f"   - Filas de muestra: {len(df)}")
        print(f"   - Total de columnas: {len(df.columns)}")
        
        print(f"\n📋 COLUMNAS DISPONIBLES:")
        for i, col in enumerate(df.columns):
            print(f"   {i+1:2d}. {col}")
        
        print(f"\n🔍 DATOS DE MUESTRA:")
        print(df.head())
        
        print(f"\n🌍 BUSCANDO COLUMNAS DE PAÍS/UBICACIÓN:")
        columnas_pais = [col for col in df.columns if any(keyword in col.lower() 
                        for keyword in ['country', 'location', 'nation', 'region'])]
        if columnas_pais:
            print(f"   Posibles columnas de país: {columnas_pais}")
        else:
            print("   No se encontraron columnas obvias de país")
        
        print(f"\n📅 BUSCANDO COLUMNAS DE FECHA:")
        columnas_fecha = [col for col in df.columns if any(keyword in col.lower() 
                         for keyword in ['date', 'time', 'day'])]
        if columnas_fecha:
            print(f"   Posibles columnas de fecha: {columnas_fecha}")
        else:
            print("   No se encontraron columnas obvias de fecha")
        
        print(f"\n🦠 BUSCANDO COLUMNAS DE CASOS:")
        columnas_casos = [col for col in df.columns if any(keyword in col.lower() 
                         for keyword in ['cases', 'new_cases', 'daily_cases'])]
        if columnas_casos:
            print(f"   Posibles columnas de casos: {columnas_casos}")
        else:
            print("   No se encontraron columnas obvias de casos")
        
        print(f"\n💉 BUSCANDO COLUMNAS DE VACUNACIÓN:")
        columnas_vacunas = [col for col in df.columns if any(keyword in col.lower() 
                           for keyword in ['vaccin', 'vacc', 'immuniz'])]
        if columnas_vacunas:
            print(f"   Posibles columnas de vacunas: {columnas_vacunas}")
        else:
            print("   No se encontraron columnas obvias de vacunación")
        
        # Verificar si hay una columna que parezca ser el identificador de país
        primera_columna_data = df.iloc[:, 0].head()
        print(f"\n🔎 PRIMERA COLUMNA (posible país): '{df.columns[0]}'")
        print(f"   Valores de muestra: {list(primera_columna_data)}")
        
        return df.columns.tolist()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    columnas = inspeccionar_dataset()