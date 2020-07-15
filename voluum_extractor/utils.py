import datetime as dt
import os
import logging
from voluum_extractor import FORMAT

logging.basicConfig(level=logging.INFO, filename='extractor.log', format=FORMAT, datefmt='%d-%b-%y %H:%M:%S')


def gen_date_intervals(start_date, end_date, inv_size=2):
    start_date_obj = dt.datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date_obj = dt.datetime.strptime(end_date, '%Y-%m-%d').date()
    delta = (end_date_obj - start_date_obj).days + 1
    range_quo, adj_rem = delta / inv_size, delta % inv_size
    print(f'num_days:{delta}, interval_size:{inv_size}, full_interval:{int(range_quo)}, days_in_adj_interval:{adj_rem}')
    assert delta >= inv_size, f"days between dates {delta} should be >= to the interval size {inv_size}"

    date_range = list()

    for inv in range(int(range_quo)):
        record = {
            'date_from': (end_date_obj - dt.timedelta(((inv + 1) * inv_size) - 1)).strftime('%Y-%m-%d'),
            'date_to': (end_date_obj - dt.timedelta(inv * inv_size)).strftime('%Y-%m-%d')
        }
        date_range.append(record)

    if adj_rem:
        l_rec = date_range[-1]
        l_date_from = dt.datetime.strptime(l_rec.get('date_from'), '%Y-%m-%d').date()

        record = {
            'date_from': (l_date_from - dt.timedelta(adj_rem)).strftime('%Y-%m-%d'),
            'date_to': (l_date_from - dt.timedelta(1)).strftime('%Y-%m-%d')
        }
        date_range.append(record)

    return date_range


def remove_files_locally(list_extension: str = [".csv", ".ndjson"]):
    directory = os.getcwd()
    files_in_directory = os.listdir(directory)

    for file_extension in list_extension:
        if file_extension in [".py", ".log", ".ipynb"]:
            raise Exception('You Cannot Delete those Files')

        filtered_files = [file for file in files_in_directory if file.endswith(file_extension)]
        for file in filtered_files:
            path_to_file = os.path.join(directory, file)
            logging.info(f'Deleting file : {path_to_file}')
            os.remove(path_to_file)