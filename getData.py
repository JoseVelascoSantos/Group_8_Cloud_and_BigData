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

session = Session()

try:
    symbols = [
        'BINANCE_SPOT_XEC_BUSD',
        'BINANCE_SPOT_XEM_BUSD',
        'BINANCE_SPOT_XVG_BUSD',
        'BINANCE_SPOT_YGG_BUSD',
        'BINANCE_SPOT_ZEN_BUSD'
    ]

    keys = [
        'EBCCCA2D-D3CE-4D8B-AF16-4724FC2989DF',
        '5A366079-ABE2-44DC-B169-571E45737F88',
        '1E248DED-102B-428F-950C-089BF8370349'
    ]

    i = -1
    keyIndex = 0

    session.headers.update({
        'Accepts': 'application/json',
        'X-CoinAPI-Key': keys[keyIndex],
    })

    # Get all OHLCV from symbols with custom
    for symbol in symbols:
        if i == 2:
            i = 0
            keyIndex += 1
            session.headers.update({
                'Accepts': 'application/json',
                'X-CoinAPI-Key': keys[keyIndex],
            })
        else:
            i += 1

        print(symbol)
        print(keys[keyIndex])
        with open('data/' + symbol + '.csv', 'w') as file:

            ohlcvParams = {'period_id': '15MIN', 'time_start': "2021-01-01T00:00:00", 'limit': 100000}
            response = session.get(urlOHLCV + symbol + '/history', params=ohlcvParams)
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