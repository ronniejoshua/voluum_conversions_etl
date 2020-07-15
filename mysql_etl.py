import datetime as dt
import logging

import voluum_extractor.mysql_uploader as mysql_db
import voluum_extractor.voluum_api as vol
import voluum_extractor.voluum_mapper as cvm
from voluum_extractor import FORMAT
from voluum_extractor.config import credentials
from voluum_extractor.utils import gen_date_intervals, remove_files_locally

logging.basicConfig(level=logging.INFO, filename='extractor.log', format=FORMAT, datefmt='%d-%b-%y %H:%M:%S')

if __name__ == "__main__":

    with open('extractor.log', 'w'):
        pass

    fetch_columns = ['postbackTimestamp', 'visitTimestamp', 'transactionId', 'clickId', 'trafficSourceId',
                     'trafficSourceName', 'affiliateNetworkId', 'affiliateNetworkName', 'campaignId',
                     'campaignName', 'landerId', 'landerName', 'offerId', 'offerName', 'conversionType',
                     'conversionTypeId', 'conversions', 'revenue', 'externalId', 'customVariable1', 'customVariable2',
                     'customVariable3', 'customVariable4', 'customVariable5', 'customVariable6', 'customVariable7',
                     'customVariable8', 'customVariable9', 'customVariable10', 'countryCode', 'countryName', 'region',
                     'city', 'browser', 'browserVersion', 'connectionType', 'connectionTypeName', 'ip', 'isp', 'brand',
                     'deviceName', 'model', 'mobileCarrier', 'os', 'osVersion']

    backfill_dates = gen_date_intervals('2020-05-03', '2020-05-04', inv_size=1)
    total_revenue = 0

    mysql_db.mysql_create_table(mysql_table_id='conversions',
                                mysql_table_schema=mysql_db.mysql_voluum_conversion_schema())
    mysql_db.mysql_create_table(mysql_table_id='data_integrity',
                                mysql_table_schema="(`postbackTimestamp`  DATE, `revenue`  INTEGER)")

    for interval in backfill_dates:
        logging.info(f'Extracting and Processing Data for {interval}')
        voluum_conv = vol.extract_conversions_data(interval, fetch_columns, credentials)
        total_revenue += voluum_conv.revenue.sum()
        voluum_conv.to_csv(f"{interval.get('date_from')}_voluum_conversions.csv", index=False)
        output_df = cvm.remap_voluum_conversion(voluum_conv)
        output_df.to_csv(f"{interval.get('date_from')}_remapped_voluum_conversions.csv", index=False)

        chk_df = output_df.groupby("postbackTimestamp")["revenue"].sum().reset_index()
        chk_df.postbackTimestamp = dt.datetime.fromtimestamp(chk_df.postbackTimestamp).strftime('%Y-%m-%d')
        val_tuple = [(m, int(n)) for m, n in chk_df.itertuples(index=False, name=None)][0]
        logging.info(f'Processed Extracted data for {interval} with values {val_tuple}')

        if not mysql_db.mysql_data_integrity_checker(*val_tuple):
            mysql_db.mysql_upload_conversions_v0(df=output_df, query=mysql_db.mysql_conversion_insert_query())
            mysql_db.mysql_data_integrity_insert_query(val_tuple)

    logging.info(f'TOTAL REVENUE {total_revenue}')
    remove_files_locally()
