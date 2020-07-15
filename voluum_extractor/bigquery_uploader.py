import json
import ndjson
import os
import datetime as dt
from google.cloud import storage
from google.cloud import bigquery
import logging
from voluum_extractor.config import credentials
from voluum_extractor import FORMAT

logging.basicConfig(level=logging.INFO, filename='extractor.log', format=FORMAT, datefmt='%d-%b-%y %H:%M:%S')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials.get('gcp_bigquery_credentials')


def generate_ndjson_from_df(local_data_file, df):
    json_string = df.to_json(date_format='iso', date_unit='s', orient='records')
    ndjson_object = ndjson.loads(json_string)[0]

    # Writing items to a ndjson file
    with open(local_data_file, 'w', encoding='utf-8') as f:
        writer = ndjson.writer(f, ensure_ascii=False)
        for old_dd in ndjson_object:
            new_dd = dict()
            new_dd['_sdc_sequence'] = dt.datetime.now(tz=dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            new_dd['postbackTimestamp'] = dt.datetime.fromtimestamp(old_dd.get('postbackTimestamp')).strftime(
                '%Y-%m-%d')
            new_dd['visitTimestamp'] = dt.datetime.fromtimestamp(old_dd.get('visitTimestamp')).strftime('%Y-%m-%d')
            new_dd['transactionId'] = old_dd.get('transactionId')
            new_dd['clickId'] = old_dd.get('clickId')
            new_dd['trafficSourceId'] = old_dd.get('trafficSourceId')
            new_dd['trafficSourceName'] = old_dd.get('trafficSourceName')
            new_dd['affiliateNetworkId'] = old_dd.get('affiliateNetworkId')
            new_dd['affiliateNetworkName'] = old_dd.get('affiliateNetworkName')
            new_dd['campaignId'] = old_dd.get('campaignId')
            new_dd['campaignName'] = old_dd.get('campaignName')
            new_dd['landerId'] = old_dd.get('landerId')
            new_dd['landerName'] = old_dd.get('landerName')
            new_dd['offerId'] = old_dd.get('offerId')
            new_dd['offerName'] = old_dd.get('offerName')
            new_dd['conversionType'] = old_dd.get('conversionType')
            new_dd['conversionTypeId'] = old_dd.get('conversionTypeId')
            new_dd['conversions'] = old_dd.get('conversions')
            new_dd['revenue'] = old_dd.get('revenue')
            new_dd['externalId'] = old_dd.get('externalId')
            new_dd['customVariable1'] = old_dd.get('customVariable1')
            new_dd['customVariable2'] = old_dd.get('customVariable2')
            new_dd['customVariable3'] = old_dd.get('customVariable3')
            new_dd['customVariable4'] = old_dd.get('customVariable4')
            new_dd['customVariable5'] = old_dd.get('customVariable5')
            new_dd['customVariable6'] = old_dd.get('customVariable6')
            new_dd['customVariable7'] = old_dd.get('customVariable7')
            new_dd['customVariable8'] = old_dd.get('customVariable8')
            new_dd['customVariable9'] = old_dd.get('customVariable9')
            new_dd['customVariable10'] = old_dd.get('customVariable10')
            new_dd['countryCode'] = old_dd.get('countryCode')
            new_dd['countryName'] = old_dd.get('countryName')
            new_dd['region'] = old_dd.get('region')
            new_dd['city'] = old_dd.get('city')
            new_dd['browser'] = old_dd.get('browser')
            new_dd['browserVersion'] = old_dd.get('browserVersion')
            new_dd['connectionType'] = old_dd.get('connectionType')
            new_dd['connectionTypeName'] = old_dd.get('connectionTypeName')
            new_dd['ip'] = old_dd.get('ip')
            new_dd['isp'] = old_dd.get('isp')
            new_dd['brand'] = old_dd.get('brand')
            new_dd['deviceName'] = old_dd.get('deviceName')
            new_dd['model'] = old_dd.get('model')
            new_dd['mobileCarrier'] = old_dd.get('mobileCarrier')
            new_dd['os'] = old_dd.get('os')
            new_dd['osVersion'] = old_dd.get('osVersion')
            writer.writerow(new_dd)
    return True


def upload_blob_to_gcs(gcs_bucket_name, local_source_file_name, gcs_destination_blob_name):
    """
        Uploads a file to the bucket.
        upload_blob_to_gcs(gcs_bucket_name='voluum_conversions', local_source_file_name = 'voluum_conversions.ndjson',
                    gcs_destination_blob_name = 'upload_voluum_conversions.ndjson')
    """
    gcs_bucket_name = gcs_bucket_name
    local_source_file_name = local_source_file_name
    gcs_destination_blob_name = gcs_destination_blob_name

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_destination_blob_name)

    if blob.exists():
        blob.delete()
        print("Blob {} deleted.".format(gcs_destination_blob_name))

    blob.upload_from_filename(local_source_file_name)

    with open(local_source_file_name, 'rb') as file:
        try:
            blob.upload_from_file(file)
            msg = 'Successfully uploaded {0} to Cloud Storage'
            logging.info(msg.format(local_source_file_name))
            return True
        except Exception as error:
            msg = 'Failed to upload {0} to Cloud Storage: {1}'
            logging.error(msg.format(local_source_file_name, error))
            return False


def upload_ndjson_to_bigquery(project_id, dataset_id, table_id, gcs_bucket_name, gcs_destination_blob_name):
    gbq_project_id = project_id
    gbq_dataset_id = dataset_id
    gbq_table_id = table_id

    client = bigquery.Client(gbq_project_id)
    dataset_ref = client.dataset(gbq_dataset_id)
    uri = f"gs://{gcs_bucket_name}/{gcs_destination_blob_name}"

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = [bigquery.SchemaField('_sdc_sequence', 'DATETIME'),
                         bigquery.SchemaField('postbackTimestamp', 'DATE'),
                         bigquery.SchemaField('visitTimestamp', 'DATE'),
                         bigquery.SchemaField('transactionId', 'STRING'),
                         bigquery.SchemaField('clickId', 'STRING'),
                         bigquery.SchemaField('trafficSourceId', 'STRING'),
                         bigquery.SchemaField('trafficSourceName', 'STRING'),
                         bigquery.SchemaField('affiliateNetworkId', 'STRING'),
                         bigquery.SchemaField('affiliateNetworkName', 'STRING'),
                         bigquery.SchemaField('campaignId', 'STRING'),
                         bigquery.SchemaField('campaignName', 'STRING'),
                         bigquery.SchemaField('landerId', 'STRING'),
                         bigquery.SchemaField('landerName', 'STRING'),
                         bigquery.SchemaField('offerId', 'STRING'),
                         bigquery.SchemaField('offerName', 'STRING'),
                         bigquery.SchemaField('conversionType', 'STRING'),
                         bigquery.SchemaField('conversionTypeId', 'INTEGER'),
                         bigquery.SchemaField('conversions', 'INTEGER'),
                         bigquery.SchemaField('revenue', 'FLOAT'),
                         bigquery.SchemaField('externalId', 'STRING'),
                         bigquery.SchemaField('customVariable1', 'STRING'),
                         bigquery.SchemaField('customVariable2', 'STRING'),
                         bigquery.SchemaField('customVariable3', 'STRING'),
                         bigquery.SchemaField('customVariable4', 'STRING'),
                         bigquery.SchemaField('customVariable5', 'STRING'),
                         bigquery.SchemaField('customVariable6', 'STRING'),
                         bigquery.SchemaField('customVariable7', 'STRING'),
                         bigquery.SchemaField('customVariable8', 'STRING'),
                         bigquery.SchemaField('customVariable9', 'STRING'),
                         bigquery.SchemaField('customVariable10', 'STRING'),
                         bigquery.SchemaField('countryCode', 'STRING'),
                         bigquery.SchemaField('countryName', 'STRING'),
                         bigquery.SchemaField('region', 'STRING'),
                         bigquery.SchemaField('city', 'STRING'),
                         bigquery.SchemaField('browser', 'STRING'),
                         bigquery.SchemaField('browserVersion', 'STRING'),
                         bigquery.SchemaField('connectionType', 'STRING'),
                         bigquery.SchemaField('connectionTypeName', 'STRING'),
                         bigquery.SchemaField('ip', 'STRING'),
                         bigquery.SchemaField('isp', 'STRING'),
                         bigquery.SchemaField('brand', 'STRING'),
                         bigquery.SchemaField('deviceName', 'STRING'),
                         bigquery.SchemaField('model', 'STRING'),
                         bigquery.SchemaField('mobileCarrier', 'STRING'),
                         bigquery.SchemaField('os', 'STRING'),
                         bigquery.SchemaField('osVersion', 'STRING')]

    load_job = client.load_table_from_uri(uri, dataset_ref.table(gbq_table_id), job_config=job_config)
    logging.info(f"Starting job {load_job.job_id}")
    load_job.result()
    logging.info("Job finished.")
    destination_table = client.get_table(dataset_ref.table(gbq_table_id))
    logging.info(f"Loaded {destination_table.num_rows} rows.")


def remove_files_from_gcs_bucket(gcs_bucket_name, extension=".ndjson", list_blobs=None):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(gcs_bucket_name)
    for blob in blobs:
        if blob.name.endswith(extension) or blob.name in list_blobs:
            logging.info(f"Deleting Blob {blob.name}.")
            blob.delete()


def get_bigquery_table_schema(bq_dataset_id, bq_table_id):
    """Get BigQuery Table Schema."""
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(bq_dataset_id)
    bg_tableref = bigquery.table.TableReference(dataset_ref, bq_table_id)
    bg_table = bigquery_client.get_table(bg_tableref)
    return bg_table.schema
