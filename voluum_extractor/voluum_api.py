# import warnings
# warnings.filterwarnings('ignore')

import datetime as dt
import logging
import time
from itertools import chain

import pandas as pd
import requests

from voluum_extractor import FORMAT

logging.basicConfig(level=logging.INFO, filename='extractor.log', format=FORMAT, datefmt='%d-%b-%y %H:%M:%S')

fetch_columns = ['postbackTimestamp', 'visitTimestamp', 'transactionId', 'clickId', 'trafficSourceId',
                 'trafficSourceName', 'affiliateNetworkId', 'affiliateNetworkName', 'campaignId',
                 'campaignName', 'landerId', 'landerName', 'offerId', 'offerName', 'conversionType',
                 'conversionTypeId', 'conversions', 'revenue', 'externalId', 'customVariable1', 'customVariable2',
                 'customVariable3', 'customVariable4', 'customVariable5', 'customVariable6', 'customVariable7',
                 'customVariable8', 'customVariable9', 'customVariable10', 'countryCode', 'countryName', 'region',
                 'city', 'browser', 'browserVersion', 'connectionType', 'connectionTypeName', 'ip', 'isp', 'brand',
                 'deviceName', 'model', 'mobileCarrier', 'os', 'osVersion']


# Authorization using Access Token:
# https://doc.voluum.com/en/voluum_api_docs.html#al_idm46360581774784

def get_session_authorization(access_id, access_key):
    auth_url = "https://api.voluum.com/auth/access/session"
    headers = {
        'content-type': 'application/json; charset=utf-8',
        'accept': 'application/json'
    }
    auth_payload = {
        'accessId': access_id,
        'accessKey': access_key
    }
    response = requests.post(auth_url, headers=headers, json=auth_payload)
    headers['cwauth-token'] = response.json()['token']
    return headers


def json_to_csv_string(json_response, columns, partial_extract=True):
    df = pd.json_normalize(json_response)
    df = df[columns] if partial_extract else df
    df['postbackTimestamp'] = pd.to_datetime(df['postbackTimestamp']).dt.date
    df['visitTimestamp'] = pd.to_datetime(df['visitTimestamp']).dt.date
    return df


def extract_conversions_data(reporting_period, retrive_columns, my_credentials, filter_by_col=None, predicate=None):
    voluum_auth = my_credentials.get('voluum')
    access_id = voluum_auth.get('access_id')
    access_key = voluum_auth.get('access_key')
    conversion_url = 'https://api.voluum.com/report/conversions'
    headers = get_session_authorization(access_id, access_key)
    date_from = (dt.datetime.utcnow() - dt.timedelta(days=reporting_period)).strftime("%Y-%m-%dT00:00:00Z")
    date_to = dt.datetime.utcnow().strftime("%Y-%m-%dT00:00:00Z")

    rows_pending = True
    rows_fetched = 100000
    total_rows_fetched = 0
    total_retrived_data = list()
    while rows_pending > 0:
        params = {
            'offset': total_rows_fetched,
            'limit': rows_fetched,
            'tz': 'America/New_York',
            'from': date_from,
            'to': date_to,
            'columns': [filter_by_col],
            'filter': predicate,
            'sort': 'postbackTimestamp',
            'direction': 'ASC'
        }

        while True:
            response = requests.get(
                conversion_url,
                headers=headers,
                params=params
            )
            print(response.url)

            if response.status_code == 200:
                break
            else:
                time.sleep(10)
                print('Trying Again ....')

        retrived_data = response.json()['rows']
        total_retrived_data.append(retrived_data)

        total_rows = response.json()['totalRows']
        total_rows_fetched += rows_fetched
        rows_pending = total_rows - total_rows_fetched
        rows_fetched = min(response.json()['limit'], rows_pending)

        print(f'TOTAL ROWS:{total_rows} : TOTAL ROWS FETCHED:{total_rows_fetched} : ROWS PENDING:{rows_pending}')

    list_dd = list(chain.from_iterable(total_retrived_data))
    if len(list_dd):
        df = json_to_csv_string(list_dd, retrive_columns, partial_extract=True)
        return df
