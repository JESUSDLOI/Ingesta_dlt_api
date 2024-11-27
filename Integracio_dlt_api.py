import dlt
import requests
from typing import List, Dict

# Configuración de Snowflake
@dlt.resource(parallelized=True ,table_name="empresa_data", write_disposition={"disposition": "merge", "strategy": "scd2"})
#@dlt.resource(table_name="empresa_data", write_disposition='append')

#Función para obtener datos de empresas
def obtener_datos_empresas(api_key: str) -> List[Dict]:
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
        primeras_cinco_empresas = nombres_empresas[:5]

        # Lista para almacenar la información de cada empresa
        informacion_empresas = []

        # Iterar sobre los nombres de las empresas y hacer solicitudes para cada una
        for symbol in primeras_cinco_empresas:
            datos_empresa = {
                'symbol': symbol,
                'overview': agregar_symbol_a_datos(symbol, obtener_datos_api(api_key, "OVERVIEW", symbol)),
                'balance_sheet': agregar_symbol_a_datos(symbol, obtener_datos_api(api_key, "BALANCE_SHEET", symbol)),
                'income_statement': agregar_symbol_a_datos(symbol, obtener_datos_api(api_key, "INCOME_STATEMENT", symbol)),
                'cash_flow': agregar_symbol_a_datos(symbol, obtener_datos_api(api_key, "CASH_FLOW", symbol))
            }
            yield datos_empresa

        
    except Exception as e:
        dlt.log(f"Error al obtener datos de empresas: {e}", level="error")
        return []

def obtener_datos_api(api_key: str, function: str, symbol: str) -> Dict:
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
        return r.json()
    except Exception as e:
        dlt.log(f"Error al obtener datos de la API para {symbol} con función {function}: {e}", level="error")
        return {}
    
    
def agregar_symbol_a_datos(symbol: str, data: Dict) -> Dict:
    """
    Añade el símbolo de la empresa a los datos proporcionados.

    Args:
        symbol (str): El símbolo de la empresa.
        data (Dict): Datos obtenidos de la API.

    Returns:
        Dict: Datos con el símbolo añadido.
    """
    if isinstance(data, dict):
        data["symbol"] = symbol
    return data

# Crear el pipeline de DLT
pipeline = dlt.pipeline(
    import_schema_path="schemas/DATOS_FINANZAS",
    pipeline_name="alpha_vantage_ingestion",
    destination="snowflake",
    dataset_name="Datos_finanzas"
)

# Ingestar datos al pipeline
def run_pipeline():
    """
    Ejecuta el pipeline para obtener datos de empresas y cargarlos en Snowflake.
    """
    try:
        load_info = pipeline.run(obtener_datos_empresas(api_key="8XIKTJXHEGE7W9YJ"))
        print(f"Ingesta completada con éxito: {load_info}")
    except Exception as e:
        print(f"Error al ejecutar el pipeline: {e}")

if __name__ == "__main__":
    run_pipeline()
