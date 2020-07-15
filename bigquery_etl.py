import logging
import voluum_extractor.bigquery_uploader as bq_db
import voluum_extractor.voluum_api as vol
import voluum_extractor.voluum_mapper as cvm
from voluum_extractor import FORMAT
from voluum_extractor.config import credentials
from voluum_extractor.utils import gen_date_intervals, remove_files_locally

logging.basicConfig(level=logging.INFO, filename='bq_extractor.log', format=FORMAT, datefmt='%d-%b-%y %H:%M:%S')

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

    backfill_dates = gen_date_intervals('2020-04-30', '2020-04-30', inv_size=1)
    total_revenue = 0

    for interval in backfill_dates:
        logging.info(f'Extracting and Processing Data for {interval}')
        voluum_conv = vol.extract_conversions_data(interval, fetch_columns, credentials)
        total_revenue += voluum_conv.revenue.sum()
        voluum_conv.to_csv(f"{interval.get('date_from')}_voluum_conversions.csv", index=False)
        output_df = cvm.remap_voluum_conversion(voluum_conv)
        output_df.to_csv(f"{interval.get('date_from')}_remapped_voluum_conversions.csv", index=False)
        filename_prefix = interval.get('date_from')

        bq_project_id = 'MY_PROJECT_ID'
        bq_dataset_id = 'voluum'
        bq_table_id = 'voluum_conversions'
        gcs_bucket_name = 'voluum_conversions'
        local_data_file = f'{filename_prefix}_voluum_conversions.ndjson'
        gcs_destination_blob = f'{filename_prefix}_voluum_conversions.ndjson'

        if bq_db.generate_ndjson_from_df(local_data_file, output_df):
            if bq_db.upload_blob_to_gcs(gcs_bucket_name=gcs_bucket_name,
                                        local_source_file_name=local_data_file,
                                        gcs_destination_blob_name=gcs_destination_blob
                                        ):
                bq_db.upload_ndjson_to_bigquery(project_id=bq_project_id,
                                                dataset_id=bq_dataset_id,
                                                table_id=bq_table_id,
                                                gcs_bucket_name=gcs_bucket_name,
                                                gcs_destination_blob_name=gcs_destination_blob)
    bq_db.remove_files_from_gcs_bucket(gcs_bucket_name)
    logging.info(f'TOTAL REVENUE {total_revenue}')
    remove_files_locally()