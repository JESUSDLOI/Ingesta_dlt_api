import dlt
from dlt.sources.helpers import requests


def extraer_nombres_empresas(data):
    """
    Extrae los nombres de las empresas desde la clave 'description' en una lista de diccionarios.

    Args:
        data (list): Lista de diccionarios que contiene datos de empresas.

    Returns:
        list: Lista con los nombres de las empresas.
    """
    try:
        # Extraer la descripción de cada empresa si está disponible
        nombres_empresas = [item['symbol'] for item in data if 'symbol' in item and item['symbol'] != 'n/a']
        return nombres_empresas
    except Exception as e:
        print(f"Error procesando los datos: {e}")
        return []

url = 'https://www.alphavantage.co/query?function=ETF_PROFILE&symbol=QQQ&apikey=demo'
r = requests.get(url)
datos = r.json()

print(datos)


# Acceder a la clave 'holdings'
holdings_data = datos.get('holdings', [])



# Extraer los nombres de las empresas
nombres_empresas = extraer_nombres_empresas(holdings_data)



# Quedarse solo con las cinco primeras empresas, la api no permite más de 25 solicitudes al día
primeras_cinco_empresas = nombres_empresas[:5]

print(primeras_cinco_empresas)

# Lista para almacenar la información de cada empresa
informacion_empresas = []

# Iterar sobre los nombres de las empresas y hacer una solicitud para cada una
for symbol in primeras_cinco_empresas:
    url_1 = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol=IBM&apikey=demo'
    r_empresa = requests.get(url_1)
    url_2 = f'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol=IBM&apikey=demo'
    balance_sheet = requests.get(url_2)
    url_3 = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol=IBM&apikey=demo'
    income_statement = requests.get(url_3)
    url_4 = f'https://www.alphavantage.co/query?function=CASH_FLOW&symbol=IBM&apikey=demo'
    cash_flow = requests.get(url_4)
    datos_empresa = {
        'symbol': symbol,
        'overview': r_empresa.json(),
        'balance_sheet': balance_sheet.json(),
        'income_statement': income_statement.json(),
        'cash_flow': cash_flow.json()
    }
    
    informacion_empresas.append(datos_empresa)
    
print(informacion_empresas)
    



