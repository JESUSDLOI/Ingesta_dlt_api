import dlt
import requests
from typing import List, Dict
from datetime import datetime


# Configuración de Snowflake
#@dlt.resource(parallelized=True ,table_name="empresa_data", write_disposition={"disposition": "merge", "strategy": "scd2"})
@dlt.resource(table_name="empresa_data", write_disposition='append')

#Función para obtener datos de empresas
def obtener_datos_empresas(api_key: str, fecha_ingesta: datetime) -> List[Dict]:
    """
    Obtiene datos financieros de las principales empresas en el ETF del S&P 500 usando Alpha Vantage.
    
    Args:
        api_key (str): La clave de API para Alpha Vantage.

    Returns:
        List[Dict]: Lista de diccionarios con los datos de las empresas.
    """
    # URL para obtener los holdings del ETF del S&P 500
    url_etf_profile = f"https://www.alphavantage.co/query?function=ETF_PROFILE&symbol=QQQ&apikey={api_key}"
    
    try:
        # Solicitud inicial para obtener las empresas
        r = requests.get(url_etf_profile)
        r.raise_for_status()
        datos = r.json()
        holdings_data = datos.get("holdings", [])

        # Extraer símbolos de las cinco primeras empresas
        nombres_empresas = [item['symbol'] for item in holdings_data if 'symbol' in item and item['symbol'] != 'n/a']
        primeras_cinco_empresas = nombres_empresas[5:10]


        # Iterar sobre los nombres de las empresas y hacer solicitudes para cada una
        for symbol in primeras_cinco_empresas:
            datos_empresa = {
                'symbol': symbol,
                'overview': obtener_datos_api(api_key, "OVERVIEW", symbol, fecha_ingesta),
                'balance_sheet': obtener_datos_api(api_key, "BALANCE_SHEET", symbol, fecha_ingesta),
                'income_statement': obtener_datos_api(api_key, "INCOME_STATEMENT", symbol, fecha_ingesta),
                'cash_flow': obtener_datos_api(api_key, "CASH_FLOW", symbol, fecha_ingesta)
            }
            yield datos_empresa

        
    except Exception as e:
        dlt.log(f"Error al obtener datos de empresas: {e}", level="error")
        return []

def obtener_datos_api(api_key: str, function: str, symbol: str, fecha_ingesta: datetime) -> Dict:
    """
    Realiza una solicitud a la API de Alpha Vantage para obtener datos específicos.

    Args:
        api_key (str): La clave de API para Alpha Vantage.
        function (str): La función de la API a llamar.
        symbol (str): El símbolo de la empresa.

    Returns:
        Dict: Los datos obtenidos de la API para la función y símbolo especificados.
    """
    url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={api_key}"
    try:
        r = requests.get(url)
        r.raise_for_status()
        datos = r.json()
        
        # Añadir una nueva columna a cada fila de annualReports
        for report in datos.get('annualReports', []):
            report['simbolo'] = symbol
            report['fecha_carga'] = fecha_ingesta
            
        # Añadir una nueva columna a cada fila de annualReports
        for report in datos.get('quarterlyReports', []):
            report['simbolo'] = symbol
            report['fecha_carga'] = fecha_ingesta
        
        return datos
    
    except Exception as e:
        dlt.log(f"Error al obtener datos de la API para {symbol} con función {function}: {e}", level="error")
        return {}
    
    

# Crear el pipeline de DLT
pipeline = dlt.pipeline(
    import_schema_path="schemas/DATOS_FINANZAS",
    pipeline_name="pipeline_finanzas",
    destination="snowflake",
    dataset_name="Datos_finanzas"
)

# Ingestar datos al pipeline
def run_pipeline():
    """
    Ejecuta el pipeline para obtener datos de empresas y cargarlos en Snowflake.
    """
    try:
        fecha_ingesta = datetime.now().isoformat()
        api_key="WX5LRHH0MMDEIZOE"
        load_info = pipeline.run(obtener_datos_empresas(api_key, fecha_ingesta))
        print(f"Ingesta completada con éxito: {load_info}")
    except Exception as e:
        print(f"Error al ejecutar el pipeline: {e}")

if __name__ == "__main__":
    run_pipeline()


#XGZ8A5CYO2WO86AO
#WX5LRHH0MMDEIZOE 