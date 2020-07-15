import mysql.connector as mysql
from voluum_extractor.config import credentials
import logging
import json
import datetime as dt
from voluum_extractor import FORMAT

logging.basicConfig(level=logging.INFO, filename='extractor.log', format=FORMAT, datefmt='%d-%b-%y %H:%M:%S')


def mysql_connector(func):
    def with_connection(*args, **kwargs):
        conn = mysql.connect(**credentials.get('voluum_conversions'))
        try:
            return_value = func(conn, *args, **kwargs)
        except Exception:
            conn.rollback()
            logging.error("Database connection error")
            raise
        else:
            conn.commit()
        finally:
            conn.close()
        return return_value

    return with_connection


@mysql_connector
def mysql_drop_table(conn, mysql_table_id):
    cursor = conn.cursor()
    sql_query = f'DROP TABLE IF EXISTS {mysql_table_id}'
    cursor.execute(sql_query)


@mysql_connector
def mysql_create_table(conn, mysql_table_id, mysql_table_schema):
    cursor = conn.cursor()
    sql_query = f'CREATE TABLE IF NOT EXISTS {mysql_table_id} {mysql_table_schema}'
    cursor.execute(sql_query)


@mysql_connector
def mysql_table_exists(conn, mysql_table_id):
    cursor = conn.cursor()
    sql_query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{mysql_table_id}'"
    cursor.execute(sql_query)
    if cursor.fetchone()[0] == 1:
        return True
    return False


@mysql_connector
def mysql_data_integrity_checker(conn, key, value):
    cursor = conn.cursor()
    sql_query = f"SELECT * FROM voluum_conversions.data_integrity WHERE postbackTimestamp='{key}'"
    cursor.execute(sql_query)
    list_ret_val = cursor.fetchall()
    logging.info(list_ret_val)
    if len(list_ret_val) > 1:
        sql_query_1 = f"DELETE FROM voluum_conversions.conversions WHERE postbackTimestamp='{key}'"
        cursor.execute(sql_query_1)

        sql_query_2 = f"DELETE FROM voluum_conversions.data_integrity WHERE postbackTimestamp='{key}'"
        cursor.execute(sql_query_2)
        return False
    elif len(list_ret_val):
        _key, _value = list_ret_val[0][0].strftime("%Y-%m-%d"), list_ret_val[0][1]
        if _key == key and _value == value:
            logging.info("Data Already in Database")
            return True
        elif _key == key and _value != value:
            logging.info("Deleting the records")
            sql_query_1 = f"DELETE FROM voluum_conversions.conversions WHERE postbackTimestamp='{_key}'"
            cursor.execute(sql_query_1)

            sql_query_2 = f"DELETE FROM voluum_conversions.data_integrity WHERE postbackTimestamp='{_key}'"
            cursor.execute(sql_query_2)
            return False
    return False


def mysql_voluum_conversion_schema():
    """
    """
    return """(
            `_sdc_sequence`  TIMESTAMP,
            `postbackTimestamp`  DATE,
            `visitTimestamp`  DATE,
            `transactionId`  VARCHAR(256),
            `clickId`  VARCHAR(256),
            `trafficSourceId`  VARCHAR(256),
            `trafficSourceName`  VARCHAR(256),
            `affiliateNetworkId`  VARCHAR(256),
            `affiliateNetworkName`  VARCHAR(256),
            `campaignId`  VARCHAR(256),
            `campaignName`  VARCHAR(256),
            `landerId`  VARCHAR(256),
            `landerName`  VARCHAR(256),
            `offerId`  VARCHAR(256),
            `offerName`  VARCHAR(256),
            `conversionType`  VARCHAR(256),
            `conversionTypeId`  VARCHAR(256),
            `conversions`  INT,
            `revenue`  FLOAT,
            `externalId`  VARCHAR(256),
            `customVariable1`  VARCHAR(256),
            `customVariable2`  VARCHAR(256),
            `customVariable3`  VARCHAR(256),
            `customVariable4`  VARCHAR(256),
            `customVariable5`  VARCHAR(256),
            `customVariable6`  VARCHAR(256),
            `customVariable7`  VARCHAR(256),
            `customVariable8`  VARCHAR(256),
            `customVariable9`  VARCHAR(256),
            `customVariable10`  VARCHAR(256),
            `countryCode`  VARCHAR(256),
            `countryName`  VARCHAR(256),
            `region`  VARCHAR(256),
            `city`  VARCHAR(256),
            `browser`  VARCHAR(256),
            `browserVersion`  VARCHAR(256),
            `connectionType`  VARCHAR(256),
            `connectionTypeName`  VARCHAR(256),
            `ip`  VARCHAR(256),
            `isp`  VARCHAR(256),
            `brand`  VARCHAR(256),
            `deviceName`  VARCHAR(256),
            `model`  VARCHAR(256),
            `mobileCarrier`  VARCHAR(256),
            `os`  VARCHAR(256),
            `osVersion`  VARCHAR(256)
        )"""


def mysql_conversion_insert_query():
    """
    """
    return """INSERT INTO conversions (
            _sdc_sequence, postbackTimestamp, visitTimestamp, transactionId, clickId, trafficSourceId, trafficSourceName, 
            affiliateNetworkId, affiliateNetworkName, campaignId, campaignName, landerId, landerName, 
            offerId, offerName, conversionType, conversionTypeId, conversions, revenue, externalId, 
            customVariable1, customVariable2, customVariable3, customVariable4, customVariable5, 
            customVariable6, customVariable7, customVariable8, customVariable9, customVariable10, 
            countryCode, countryName, region, city, browser, browserVersion, connectionType, connectionTypeName, 
            ip, isp, brand, deviceName, model, mobileCarrier, os, osVersion
            ) VALUES (%s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """


@mysql_connector
def mysql_data_integrity_insert_query(conn, data_tuple):
    cursor = conn.cursor()
    query = "INSERT INTO data_integrity (postbackTimestamp, revenue) VALUES (%s, %s)"
    cursor.execute(query, data_tuple)


@mysql_connector
def mysql_upload_conversions_v0(conn, df, query):
    """
    Uploading voluum conversion to mysql database.
    most efficient alternative.
    """
    json_string = df.to_json(date_format=None, date_unit='s', orient='records')
    json_object = json.loads(json_string)
    cursor = conn.cursor()
    time_stamp = dt.datetime.now(tz=dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    for dd in json_object:
        _sdc_sequence = time_stamp
        postbackTimestamp = dt.datetime.fromtimestamp(dd.get('postbackTimestamp')).strftime('%Y-%m-%d')
        visitTimestamp = dt.datetime.fromtimestamp(dd.get('visitTimestamp')).strftime('%Y-%m-%d')
        transactionId = dd.get('transactionId')
        clickId = dd.get('clickId')
        trafficSourceId = dd.get('trafficSourceId')
        trafficSourceName = dd.get('trafficSourceName')
        affiliateNetworkId = dd.get('affiliateNetworkId')
        affiliateNetworkName = dd.get('affiliateNetworkName')
        campaignId = dd.get('campaignId')
        campaignName = dd.get('campaignName')
        landerId = dd.get('landerId')
        landerName = dd.get('landerName')
        offerId = dd.get('offerId')
        offerName = dd.get('offerName')
        conversionType = dd.get('conversionType')
        conversionTypeId = dd.get('conversionTypeId')
        conversions = dd.get('conversions')
        revenue = dd.get('revenue')
        externalId = dd.get('externalId')
        customVariable1 = dd.get('customVariable1')
        customVariable2 = dd.get('customVariable2')
        customVariable3 = dd.get('customVariable3')
        customVariable4 = dd.get('customVariable4')
        customVariable5 = dd.get('customVariable5')
        customVariable6 = dd.get('customVariable6')
        customVariable7 = dd.get('customVariable7')
        customVariable8 = dd.get('customVariable8')
        customVariable9 = dd.get('customVariable9')
        customVariable10 = dd.get('customVariable10')
        countryCode = dd.get('countryCode')
        countryName = dd.get('countryName')
        region = dd.get('region')
        city = dd.get('city')
        browser = dd.get('browser')
        browserVersion = dd.get('browserVersion')
        connectionType = dd.get('connectionType')
        connectionTypeName = dd.get('connectionTypeName')
        ip = dd.get('ip')
        isp = dd.get('isp')
        brand = dd.get('brand')
        deviceName = dd.get('deviceName')
        model = dd.get('model')
        mobileCarrier = dd.get('mobileCarrier')
        os = dd.get('os')
        osVersion = dd.get('osVersion')
        data_tuple = (
            _sdc_sequence, postbackTimestamp, visitTimestamp, transactionId, clickId, trafficSourceId,
            trafficSourceName,
            affiliateNetworkId, affiliateNetworkName, campaignId, campaignName, landerId, landerName,
            offerId, offerName, conversionType, conversionTypeId, conversions, revenue, externalId,
            customVariable1, customVariable2, customVariable3, customVariable4, customVariable5,
            customVariable6, customVariable7, customVariable8, customVariable9, customVariable10,
            countryCode, countryName, region, city, browser, browserVersion, connectionType, connectionTypeName,
            ip, isp, brand, deviceName, model, mobileCarrier, os, osVersion)

        cursor.execute(query, data_tuple)


@mysql_connector
def mysql_upload_conversions_v1(conn, df, query):
    """
    Uploading Voluum Conversions using cursor.executemany
    Requires max_allowed_packet adjustment
    """
    json_string = df.to_json(date_format=None, date_unit='s', orient='records')
    json_object = json.loads(json_string)
    cursor = conn.cursor()
    time_stamp = dt.datetime.now(tz=dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    data = list()
    for dd in json_object:
        _sdc_sequence = time_stamp
        postbackTimestamp = dt.datetime.fromtimestamp(dd.get('postbackTimestamp')).strftime('%Y-%m-%d')
        visitTimestamp = dt.datetime.fromtimestamp(dd.get('visitTimestamp')).strftime('%Y-%m-%d')
        transactionId = dd.get('transactionId')
        clickId = dd.get('clickId')
        trafficSourceId = dd.get('trafficSourceId')
        trafficSourceName = dd.get('trafficSourceName')
        affiliateNetworkId = dd.get('affiliateNetworkId')
        affiliateNetworkName = dd.get('affiliateNetworkName')
        campaignId = dd.get('campaignId')
        campaignName = dd.get('campaignName')
        landerId = dd.get('landerId')
        landerName = dd.get('landerName')
        offerId = dd.get('offerId')
        offerName = dd.get('offerName')
        conversionType = dd.get('conversionType')
        conversionTypeId = dd.get('conversionTypeId')
        conversions = dd.get('conversions')
        revenue = dd.get('revenue')
        externalId = dd.get('externalId')
        customVariable1 = dd.get('customVariable1')
        customVariable2 = dd.get('customVariable2')
        customVariable3 = dd.get('customVariable3')
        customVariable4 = dd.get('customVariable4')
        customVariable5 = dd.get('customVariable5')
        customVariable6 = dd.get('customVariable6')
        customVariable7 = dd.get('customVariable7')
        customVariable8 = dd.get('customVariable8')
        customVariable9 = dd.get('customVariable9')
        customVariable10 = dd.get('customVariable10')
        countryCode = dd.get('countryCode')
        countryName = dd.get('countryName')
        region = dd.get('region')
        city = dd.get('city')
        browser = dd.get('browser')
        browserVersion = dd.get('browserVersion')
        connectionType = dd.get('connectionType')
        connectionTypeName = dd.get('connectionTypeName')
        ip = dd.get('ip')
        isp = dd.get('isp')
        brand = dd.get('brand')
        deviceName = dd.get('deviceName')
        model = dd.get('model')
        mobileCarrier = dd.get('mobileCarrier')
        os = dd.get('os')
        osVersion = dd.get('osVersion')
        dtup = (
            _sdc_sequence, postbackTimestamp, visitTimestamp, transactionId, clickId, trafficSourceId,
            trafficSourceName,
            affiliateNetworkId, affiliateNetworkName, campaignId, campaignName, landerId, landerName,
            offerId, offerName, conversionType, conversionTypeId, conversions, revenue, externalId,
            customVariable1, customVariable2, customVariable3, customVariable4, customVariable5,
            customVariable6, customVariable7, customVariable8, customVariable9, customVariable10,
            countryCode, countryName, region, city, browser, browserVersion, connectionType, connectionTypeName,
            ip, isp, brand, deviceName, model, mobileCarrier, os, osVersion)
        data.append(dtup)
    cursor.executemany(query, data)


@mysql_connector
def mysql_insert_record(conn, mysql_table_id, record):
    cursor = conn.cursor()
    columns = ', '.join(record.keys())
    placeholders = ', '.join(['%s' for _ in range(len(record))])
    sql_query = 'INSERT INTO {} ({}) VALUES ({})'.format(mysql_table_id, columns, placeholders)
    cursor.execute(sql_query, tuple(record.values()))


@mysql_connector
def mysql_upload_conversions_v2(conn, df, mysql_table_id):
    """
    Uploading voluum conversions - the thrid alternative
    Using ordered dictionary.
    This is not very efficient.
    """
    import collections
    json_string = df.to_json(date_format=None, date_unit='s', orient='records')
    json_object = json.loads(json_string)
    time_stamp = dt.datetime.now(tz=dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    for dd in json_object:
        _dd = collections.OrderedDict()
        _dd['_sdc_sequence'] = time_stamp
        _dd['postbackTimestamp'] = dt.datetime.fromtimestamp(dd.get('postbackTimestamp')).strftime('%Y-%m-%d')
        _dd['visitTimestamp'] = dt.datetime.fromtimestamp(dd.get('visitTimestamp')).strftime('%Y-%m-%d')
        _dd['transactionId'] = dd.get('transactionId')
        _dd['clickId'] = dd.get('clickId')
        _dd['trafficSourceId'] = dd.get('trafficSourceId')
        _dd['trafficSourceName'] = dd.get('trafficSourceName')
        _dd['affiliateNetworkId'] = dd.get('affiliateNetworkId')
        _dd['affiliateNetworkName'] = dd.get('affiliateNetworkName')
        _dd['campaignId'] = dd.get('campaignId')
        _dd['campaignName'] = dd.get('campaignName')
        _dd['landerId'] = dd.get('landerId')
        _dd['landerName'] = dd.get('landerName')
        _dd['offerId'] = dd.get('offerId')
        _dd['offerName'] = dd.get('offerName')
        _dd['conversionType'] = dd.get('conversionType')
        _dd['conversionTypeId'] = dd.get('conversionTypeId')
        _dd['conversions'] = dd.get('conversions')
        _dd['revenue'] = dd.get('revenue')
        _dd['externalId'] = dd.get('externalId')
        _dd['customVariable1'] = dd.get('customVariable1')
        _dd['customVariable2'] = dd.get('customVariable2')
        _dd['customVariable3'] = dd.get('customVariable3')
        _dd['customVariable4'] = dd.get('customVariable4')
        _dd['customVariable5'] = dd.get('customVariable5')
        _dd['customVariable6'] = dd.get('customVariable6')
        _dd['customVariable7'] = dd.get('customVariable7')
        _dd['customVariable8'] = dd.get('customVariable8')
        _dd['customVariable9'] = dd.get('customVariable9')
        _dd['customVariable10'] = dd.get('customVariable10')
        _dd['countryCode'] = dd.get('countryCode')
        _dd['countryName'] = dd.get('countryName')
        _dd['region'] = dd.get('region')
        _dd['city'] = dd.get('city')
        _dd['browser'] = dd.get('browser')
        _dd['browserVersion'] = dd.get('browserVersion')
        _dd['connectionType'] = dd.get('connectionType')
        _dd['connectionTypeName'] = dd.get('connectionTypeName')
        _dd['ip'] = dd.get('ip')
        _dd['isp'] = dd.get('isp')
        _dd['brand'] = dd.get('brand')
        _dd['deviceName'] = dd.get('deviceName')
        _dd['model'] = dd.get('model')
        _dd['mobileCarrier'] = dd.get('mobileCarrier')
        _dd['os'] = dd.get('os')
        _dd['osVersion'] = dd.get('osVersion')

        mysql_insert_record(mysql_table_id, _dd)
