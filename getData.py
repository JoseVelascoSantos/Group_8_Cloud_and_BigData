#!/usr/bin/python

from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import sys
import csv

if len(sys.argv) != 2:
    print('Invalid argument')
    sys.exit()

urlTokens = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parametersTokens = {
    'start': '1',
    'limit': '5000',
    'convert': 'USD',
    'cryptocurrency_type': 'tokens',
    'sort': 'date_added',
    'sort_dir': 'desc'
}

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': sys.argv[1],
}

session = Session()
session.headers.update(headers)

try:
    response = session.get(urlTokens, params=parametersTokens)
    responseJson = response.json()
    if response.status_code != 200:
        print('API error: ' + responseJson["status"]["error_message"])
        sys.exit()

    with open('data.csv', mode='w') as file:
        fieldnames = ['id', 'name', 'date_added', 'price']
        file_writer = csv.DictWriter(file, fieldnames=fieldnames)
        file_writer.writeheader()

        for token in responseJson["data"]:
            file_writer.writerow({
                'id': token["id"],
                'name': token["name"],
                'date_added': token["date_added"],
                'price': token["quote"]["USD"]["price"]
            })

except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)
