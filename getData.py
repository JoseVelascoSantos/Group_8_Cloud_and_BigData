#!/usr/bin/python

from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import sys
import csv

if len(sys.argv) != 4:
    print('Invalid argument')
    sys.exit()

url = 'https://rest.coinapi.io'
urlSymbols = url + '/v1/symbols/'
urlOHLCV = url + '/v1/ohlcv/'
exchanges = ['BINANCE']

headers = {
    'Accepts': 'application/json',
    'X-CoinAPI-Key': sys.argv[1],
}

session = Session()
session.headers.update(headers)

try:
    dictionary = {}

    # Get all symbols
    with open('symbols.csv', mode='w') as file:
        fieldnames = ['exchange', 'id', 'data_start']
        file_writer = csv.DictWriter(file, fieldnames=fieldnames)
        file_writer.writeheader()

        for exchange in exchanges:
            response = session.get(urlSymbols, params={'filter_exchange_id': exchange})
            responseJson = response.json()
            if response.status_code != 200:
                print('API error: ' + responseJson["error"])
                sys.exit()

            try:
                for token in responseJson:
                    json = {
                        "exchange":  exchange,
                        'id': token["symbol_id"],
                        'data_start': token["data_start"]
                    }
                    file_writer.writerow(json)
                    if token["asset_id_base"] in dictionary:
                        dictionary[token["asset_id_base"]].append(token)
                    else:
                        dictionary.update({token["asset_id_base"]: [token]})
            except KeyError:
                pass

    symbols = []
    for key in dictionary:
        symbol = {}
        for _symbol in dictionary.get(key):
            if _symbol["data_start"].split("-")[0] == '2021' and (_symbol["asset_id_quote"] == 'USDT' or _symbol["asset_id_quote"] == 'USDC' or _symbol["asset_id_quote"] == 'BUSD' or _symbol["asset_id_quote"] == 'UST'):
                symbol = _symbol
                symbols.append(_symbol)
                break

    # Get all OHLCV from symbols with 1 year range
    for symbol in symbols:
        with open('data/' + symbol.get('symbol_id') + '.csv', 'w') as file:

            ohlcvParams = {'period_id': sys.argv[2], 'time_start': "2021-01-01T00:00:00", 'limit': sys.argv[3]}
            print(urlOHLCV + symbol.get('symbol_id') + '/history')
            print(ohlcvParams)
            response = session.get(urlOHLCV + symbol.get('symbol_id') + '/history', params=ohlcvParams)
            print(response)
            if response.status_code != 200:
                print('API error')
                sys.exit()

            try:
                responseJson = response.json()
                fieldnames = responseJson[0].keys()
                file_writer = csv.DictWriter(file, fieldnames=fieldnames)
                file_writer.writeheader()
                for data in responseJson:
                    file_writer.writerow(data)
            except KeyError:
                pass

except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)