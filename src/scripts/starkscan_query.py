import requests
import pandas as pd
import json
import os


STARKSCAN_URL = 'https://api.starkscan.co/api/v0/contract/'
CSV_FOLDER = '../csv'


if __name__ == '__main__':
    api_key = os.environ['API_KEY']
    df = pd.read_csv(CSV_FOLDER + '/top_contracts.csv')
    df = df['CONTRACT']
    name_tags = []
    # Search for the names of the top 1000 contracts.
    for contract in df.iloc[:1000]:
        contract = '0x' + '0'*(66-len(contract)) + contract[2:]
        request = requests.get(STARKSCAN_URL + str(contract), headers={'x-api-key': api_key})
        if request.status_code == 200:
            name_tags.append(json.loads(request.content)['name_tag'])
        else:
            name_tags.append('ERROR')

    df = pd.DataFrame(data=df)
    df['NAMES'] = pd.Series(data=name_tags)
    df.to_csv(CSV_FOLDER + '/names.csv')