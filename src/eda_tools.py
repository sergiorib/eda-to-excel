import os 
from pathlib import Path
from xmlrpc.client import Boolean
from charset_normalizer import from_path
import pandas as pd
from pandas import DataFrame
import re
import config as cfg
import csv

def encode(file_path:Path): 
    encode_search = from_path(file_path).best() 
    assert encode_search is not None
    encode = encode_search.encoding
    return encode

def is_numeric(value):
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

def int_or_decimal(value: str, dec_sep: str) -> str:
    value_std = value.replace(dec_sep, '.')
    try:
        f_value = float(value_std)
        if f_value == int(f_value):
            return "int"
        else:
            return "float"           
    except ValueError:
        return 'str'

def is_date(value):
    global dt_regex
    if value is None:
        return False
    value = value.strip('"')
    value = value.strip("'")
    v = str(value).strip()
    return bool(re.match(cfg.dt_regex, v))

def classify_content(value):
    if pd.isna(value):
        return "nulls"
    try: 
        value = value.strip("'")
        value = value.strip('"')
    except: 
        pass 
    if not is_numeric(value): 
        if (value is None or pd.isna(value) or value == "" or value.strip() == "" or value in ('""', "''") or 
            value.strip() in ('""', "''")):
            return "blanks"
        if is_date(value): 
            return "date"
        else: 
            return "str"
    else:  
        return(int_or_decimal(str(value),cfg.dec_sep)) 
    
def file_size_format(file_size_bytes:float):
    if file_size_bytes == 0:
        return "0.00 B"
    BASE = 1024
    UNITS = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    i = 0
    tamanho = float(file_size_bytes)
    while (tamanho >= BASE) and (i < len(UNITS) - 1):
        tamanho /= BASE 
        i += 1
    return f"{tamanho:.2f} {UNITS[i]}"
    


def file_stats(table:str,df_fields: DataFrame, file_path:Path, sep:str):       
    with open(file_path, "r", newline='', encoding=encode(file_path)) as f: 
        reader = csv.reader(f, delimiter=sep)
        data_columns = next(reader)
        data_columns = [coluna.lower() for coluna in data_columns]            
        # col count
        col_count = len(data_columns)    
        # col exists 
        expected_columns = (df_fields[df_fields['table'] == table]['field'].str.lower().unique()).tolist() 
        missing_columns = [x for x in expected_columns if x not in data_columns]
        if len(missing_columns) == 0: 
            col_exists = "yes"
        else: 
            col_exists = "no" 
        # col unique
        if len(data_columns) != len(set(data_columns)): 
            col_unique = "no"
        else: 
            col_unique = "yes" 
    # file_size 
    file_size = file_size_format(os.path.getsize(file_path)) 
    return file_size, col_count, col_exists, col_unique
