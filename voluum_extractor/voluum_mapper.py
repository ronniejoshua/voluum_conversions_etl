import pandas as pd
import json
import re
import logging
from voluum_extractor import FORMAT

# pd.set_option('display.max_columns', 50)

logging.basicConfig(level=logging.INFO, filename='extractor.log', format=FORMAT, datefmt='%d-%b-%y %H:%M:%S')
# result.groupby(['trafficSourceId', 'trafficSourceName'])['revenue'].sum()

ts_ids_mapped = {
    "verizon_ads_display_1": "d31c1162-b25e-45dd-b9d6-fd2a725b59bb",
    "verizon_ads_display_2": "d31c1162-b25e-45dd-b9d6-fd2a725b59gg",
    "verizon_ads_dsp": "d31c1162-b25e-45dd-b9d6-fd2a725b59oo",
    "google_ads_search_1": "d31c1162-b25e-45dd-b9d6-fd2a725b59aa",
    "google_ads_search_2": "d31c1162-b25e-45dd-b9d6-fd2a725b59ee",
    "google_ads_search_3": "d31c1162-b25e-45dd-b9d6-fd2a725b59ff",
    "google_ads_search_4": "d31c1162-b25e-45dd-b9d6-fd2a725b59jj",
    "google_ads_search_5": "d31c1162-b25e-45dd-b9d6-fd2a725b59kk",
    "google_ads_search_6": "d31c1162-b25e-45dd-b9d6-fd2a725b59nn",
    "ms_ads_search_1": "d31c1162-b25e-45dd-b9d6-fd2a725b59cc",
    "ms_ads_search_2": "d31c1162-b25e-45dd-b9d6-fd2a725b59dd",
    "ms_ads_search_3": "d31c1162-b25e-45dd-b9d6-fd2a725b59hh",
    "ms_ads_search_4": "d31c1162-b25e-45dd-b9d6-fd2a725b59mm",
    "snap_ads": "d31c1162-b25e-45dd-b9d6-fd2a725b59ii",
    "youtube_organic": "d31c1162-b25e-45dd-b9d6-fd2a725b59ll"
}


# noinspection PyUnreachableCode
def extract_kw(dd, key):
    try:
        kwd_obj = dd.get(key, '')
        return str(int(re.findall(r'(\d+)', kwd_obj)[0]))
    except:
        pass
    else:
        return ''


def remap_traffic_source_vads_1(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = ''
            new_dd['customVariable1'] = 'yahoo'
            new_dd['customVariable2'] = 'cpc'
            new_dd['customVariable3'] = dd.get('customVariable1', '')
            new_dd['customVariable4'] = dd.get('customVariable2', '')
            new_dd['customVariable5'] = dd.get('customVariable3', '')
            new_dd['customVariable6'] = ''
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = dd.get('customVariable5', '')
            new_dd['customVariable10'] = dd.get('customVariable4', '')
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_vads_2(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = ''
            new_dd['customVariable1'] = 'yahoo'
            new_dd['customVariable2'] = 'cpc'
            new_dd['customVariable3'] = dd.get('customVariable1', '')
            new_dd['customVariable4'] = ''
            new_dd['customVariable5'] = dd.get('customVariable3', '')
            new_dd['customVariable6'] = ''
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = ''
            new_dd['customVariable10'] = dd.get('customVariable4', '')
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_ydsp_1(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_gads_1(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = dd.get('customVariable7', '')
            new_dd['customVariable1'] = 'google'
            new_dd['customVariable2'] = 'cpc'
            new_dd['customVariable3'] = dd.get('customVariable2', '')
            new_dd['customVariable4'] = dd.get('customVariable6', '')
            new_dd['customVariable5'] = dd.get('customVariable4', '')
            new_dd['customVariable6'] = extract_kw(dd, 'customVariable3')
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = ''
            new_dd['customVariable10'] = ''
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_gads_2(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = dd.get('customVariable5', '')
            new_dd['customVariable1'] = 'google'
            new_dd['customVariable2'] = 'cpc'
            new_dd['customVariable3'] = dd.get('customVariable2', '')
            new_dd['customVariable4'] = dd.get('customVariable4', '')
            new_dd['customVariable5'] = dd.get('customVariable1', '')
            new_dd['customVariable6'] = ''
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = dd.get('customVariable3', '')
            new_dd['customVariable10'] = ''
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_gads_3(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = dd.get('customVariable5', '')
            new_dd['customVariable1'] = 'google'
            new_dd['customVariable2'] = 'cpc'
            new_dd['customVariable3'] = dd.get('customVariable2', '')
            new_dd['customVariable4'] = ''
            new_dd['customVariable5'] = dd.get('customVariable4', '')
            new_dd['customVariable6'] = extract_kw(dd, 'customVariable3')
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = ''
            new_dd['customVariable10'] = ''
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_gads_4(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = dd.get('customVariable7', '')
            new_dd['customVariable1'] = 'google'
            new_dd['customVariable2'] = 'cpc'
            new_dd['customVariable3'] = dd.get('customVariable2', '')
            new_dd['customVariable4'] = dd.get('customVariable6', '')
            new_dd['customVariable5'] = dd.get('customVariable4', '')
            new_dd['customVariable6'] = extract_kw(dd, 'customVariable3')
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = ''
            new_dd['customVariable10'] = ''
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_gads_5(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = dd.get('customVariable6', '')
            new_dd['customVariable1'] = 'google'
            new_dd['customVariable2'] = 'cpc'
            new_dd['customVariable3'] = dd.get('customVariable2', '')
            new_dd['customVariable4'] = dd.get('customVariable3', '')
            new_dd['customVariable5'] = dd.get('customVariable5', '')
            new_dd['customVariable6'] = extract_kw(dd, 'customVariable4')
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = ''
            new_dd['customVariable10'] = ''
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_msads_1(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = dd.get('customVariable8', '')
            new_dd['customVariable1'] = 'bing'
            new_dd['customVariable2'] = 'cpc'
            new_dd['customVariable3'] = dd.get('customVariable2', '')
            new_dd['customVariable4'] = dd.get('customVariable7', '')
            new_dd['customVariable5'] = dd.get('customVariable4', '')
            new_dd['customVariable6'] = extract_kw(dd, 'customVariable3')
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = ''
            new_dd['customVariable10'] = ''
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_msads_2(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = dd.get('customVariable7', '')
            new_dd['customVariable1'] = 'bing'
            new_dd['customVariable2'] = 'cpc'
            new_dd['customVariable3'] = dd.get('customVariable2', '')
            new_dd['customVariable4'] = ''
            new_dd['customVariable5'] = dd.get('customVariable4', '')
            new_dd['customVariable6'] = extract_kw(dd, 'customVariable3')
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = ''
            new_dd['customVariable10'] = ''
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_snapads_1(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = ''
            new_dd['customVariable1'] = 'snapchat'
            new_dd['customVariable2'] = 'cpc'
            new_dd['customVariable3'] = ''
            new_dd['customVariable4'] = ''
            new_dd['customVariable5'] = ''
            new_dd['customVariable6'] = ''
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = ''
            new_dd['customVariable10'] = ''
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_ytorg_1(vol_conv, ts_ids=None):
    df = vol_conv[vol_conv.trafficSourceId.isin(ts_ids)]
    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = ''
            new_dd['customVariable1'] = 'youtube'
            new_dd['customVariable2'] = 'organic'
            new_dd['customVariable3'] = dd.get('customVariable1', '')
            new_dd['customVariable4'] = ''
            new_dd['customVariable5'] = ''
            new_dd['customVariable6'] = ''
            new_dd['customVariable7'] = ''
            new_dd['customVariable8'] = ''
            new_dd['customVariable9'] = ''
            new_dd['customVariable10'] = ''
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_traffic_source_unmapped(vol_conv, mapped_ts_ids):
    ts_ids_in_df = list(vol_conv.trafficSourceId.unique())
    mapped_ts_ids = list(mapped_ts_ids.values())
    upmapped_ts_ids = [id for id in ts_ids_in_df if id not in mapped_ts_ids]
    df = vol_conv[vol_conv.trafficSourceId.isin(upmapped_ts_ids)]

    if not df.empty:
        json_string = df.to_json(date_format=None, date_unit='s', orient='records')
        json_object = json.loads(json_string)
        data_list = list()
        for dd in json_object:
            new_dd = dd.copy()
            new_dd['externalId'] = dd.get('externalId', '')
            new_dd['customVariable1'] = dd.get('customVariable1', '')
            new_dd['customVariable2'] = dd.get('customVariable2', '')
            new_dd['customVariable3'] = dd.get('customVariable3', '')
            new_dd['customVariable4'] = dd.get('customVariable4', '')
            new_dd['customVariable5'] = dd.get('customVariable5', '')
            new_dd['customVariable6'] = dd.get('customVariable6', '')
            new_dd['customVariable7'] = dd.get('customVariable7', '')
            new_dd['customVariable8'] = dd.get('customVariable8', '')
            new_dd['customVariable9'] = dd.get('customVariable9', '')
            new_dd['customVariable10'] = dd.get('customVariable10', '')
            data_list.append(new_dd)
        str_data = json.dumps(data_list)
        _df = pd.read_json(str_data, orient='records', encoding='utf-8', convert_dates=True, dtype=False)
        print(f'Traffic IDs {upmapped_ts_ids} resulted in {_df.revenue.sum():.2f}')
        return _df
    else:
        return None


def remap_voluum_conversion(df):
    """
    # result.groupby(['trafficSourceId', 'trafficSourceName'])['revenue'].sum()
    """
    yads_1 = remap_traffic_source_vads_1(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59bb'])
    yads_2 = remap_traffic_source_vads_2(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59gg'])
    ydsp_1 = remap_traffic_source_ydsp_1(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59oo'])

    gads_1 = remap_traffic_source_gads_1(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59aa',
                                                     'd31c1162-b25e-45dd-b9d6-fd2a725b59ee'])

    gads_2 = remap_traffic_source_gads_2(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59ff'])
    gads_3 = remap_traffic_source_gads_3(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59jj'])
    gads_4 = remap_traffic_source_gads_4(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59kk'])
    gads_5 = remap_traffic_source_gads_5(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59mm'])

    msads_1 = remap_traffic_source_msads_1(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59cc',
                                                       'd31c1162-b25e-45dd-b9d6-fd2a725b59dd',
                                                       'd31c1162-b25e-45dd-b9d6-fd2a725b59hh'])

    msads_2 = remap_traffic_source_msads_2(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59mm'])

    snapads_1 = remap_traffic_source_snapads_1(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59ii'])
    ytorg_1 = remap_traffic_source_ytorg_1(df, ts_ids=['d31c1162-b25e-45dd-b9d6-fd2a725b59ll'])
    unmapped = remap_traffic_source_unmapped(df, ts_ids_mapped)

    rmp_dfs = [yads_1, yads_2, snapads_1, msads_1, msads_2, gads_1, gads_2,
               gads_3, gads_4, gads_5, ydsp_1, ytorg_1, unmapped]
    conv_df = [rmp_df for rmp_df in rmp_dfs if isinstance(rmp_df, pd.DataFrame) and not rmp_df.empty]
    output_df = pd.concat(conv_df, sort=False)
    print(output_df.revenue.sum())
    return output_df
