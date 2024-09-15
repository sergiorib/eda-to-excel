# ============================================================
#  File:        Utilities.py
#  Author:      Sergio Ribeiro
#  Description: Various tools
# ============================================================
import json
import os
import pandas as pd
from charset_normalizer import from_path 
from pandas import DataFrame, isna
from config import * 
from config import log_config
import numpy as np
import csv
import math
import re

logger = log_config()

# ------------------------------------------------
# table level functions
# ------------------------------------------------

def file_size(table:str,file_path:str):
    try: 
        file_size_bytes = os.path.getsize(file_path)
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
    except BaseException as e: 
        err_msg = f"file_size function error for ({table}): {str(e)}"
        logger.error(err_msg)
        return "err"

def lines(table:str,file_path:str):
    try: 
        encode_search = from_path(file_path).best() 
        encode = encode_search.encoding
        with open(file_path, "r", newline='', encoding=encode) as f:
            reader = csv.reader(f)
            row_count = sum(1 for _ in reader)
        return row_count -1 
    except BaseException as e: 
        err_msg = f"lines function error for ({table}): {str(e)}"
        logger.error(err_msg)
        return "err"    

def columns(table:str,file_path:str,sep:str):
    try: 
        encode_search = from_path(file_path).best() 
        encode = encode_search.encoding   
        with open(file_path, "r", newline='', encoding=encode) as f:
            reader = csv.reader(f, delimiter=sep)
            col_count = len(next(reader))
        return col_count     
    except BaseException as e: 
        err_msg = f"columns function error for ({table}): {str(e)}"
        logger.error(err_msg)
        return "err"

def column_exists(table:str,df_fields: DataFrame, file_path:str,sep:str):
    expected_columns = (df_fields[df_fields['table'] == table]['field'].str.lower().unique()).tolist()   
    encode_search = from_path(file_path).best() 
    encode = encode_search.encoding      
    with open(file_path, "r", newline='', encoding=encode) as f:
        reader = csv.reader(f, delimiter=sep)
        data_columns = next(reader)
    data_columns = [coluna.lower() for coluna in data_columns]
    missing_columns = [x for x in expected_columns if x not in data_columns]
    if len(missing_columns) == 0: 
        result = "yes"
    else: 
        result = "no" 
    return result

def column_unique(table:str,file_path:str,sep:str):
    encode_search = from_path(file_path).best() 
    encode = encode_search.encoding      
    with open(file_path, "r", newline='', encoding=encode) as f:
        reader = csv.reader(f, delimiter=sep)
        data_columns = next(reader)
    if len(data_columns) != len(set(data_columns)): 
        result = "no"
    else: 
        result = "yes" 
    return result

def pk_unique(table:str,df_fields: DataFrame, file_path:str,sep:str):
    pk_columns = df_fields[(df_fields['table'] == table) & (df_fields['pk'] == 'yes')]['field'].str.lower().unique().tolist()
    if len(pk_columns) > 0: 
        encode_search = from_path(file_path).best() 
        encode = encode_search.encoding       
        df_dados = pd.read_csv(file_path,encoding=encode,sep=sep,engine='python')
        df_dados.columns = df_dados.columns.str.lower() 
        reg_qtty = len(df_dados)
        reg_unique = df_dados.drop_duplicates(subset=pk_columns, keep='first')
        if reg_qtty == len(reg_unique): 
            result = 'yes'
        else: 
            result = 'no'
    return result

def field_values(table:str, field:str, regex_value:str, values_list: str, ranges_list: str, file_path:str, sep:str):
    # validations flags
    flg_format = (not pd.isna(regex_value)) and bool(str(regex_value).strip())
    flg_list = (pd.isna(values_list) is False) and bool(str(values_list).strip())
    flg_range = (pd.isna(ranges_list) is False) and bool(str(ranges_list).strip())
    # data read
    encode_search = from_path(file_path).best() 
    encode = encode_search.encoding       
    df_dados = pd.read_csv(file_path,encoding=encode,sep=sep,engine='python')
    df_dados.columns = df_dados.columns.str.lower() 
    field_series = df_dados[field]
    # values collect
    high = field_series.max() 
    low = field_series.min() 
    qty_emptys, qty_nulls, qty_zeroes, qty_negatives, qty_uniques, qty_formats, qty_list, qty_range = 0, 0, 0, 0, 0, 0, 0, 0   
    total_records = len(df_dados)
    for idx, vlr in field_series.items():
        if pd.isna(vlr):
            qty_nulls += 1
            continue
        vlr_str = str(vlr).strip().lower()
        if vlr_str == "":
            qty_emptys += 1
            continue
        try:
            vlr_num = float(vlr_str)         
            if vlr_num == 0:
                qty_zeroes += 1
            elif vlr_num < 0: 
                qty_negatives += 1       
        except ValueError:
            pass      
    qty_uniques = df_dados[field].nunique()
    if flg_format: 
        qty_formats = df_dados[field].astype(str).str.match(regex_value, na=False).sum()
    if flg_list:
        values_lst = values_list.split(';') 
        qty_list = df_dados[field].astype(str).isin(values_lst).sum()
    if flg_range: 
        range_lst = ranges_list.split(';') 
        min_limit, max_limit = range_lst[0], range_lst[1]
        qty_range = ((df_dados[field].astype(float) >= float(min_limit)) & (df_dados[field].astype(float) <= float(max_limit))).sum()

    print("------------------- Field ---------------")
    print(field)
    print(total_records, qty_emptys, qty_nulls, qty_zeroes, qty_negatives)
    print(flg_list)
    print(values_list)
    print(flg_range)
    print(ranges_list)
    # percent calc
    perc_emptys = round(((qty_emptys / total_records) * 100),2)
    perc_nulls = round(((qty_nulls / total_records) * 100),2)
    perc_zeroes = round(((qty_zeroes / total_records) * 100),2)
    perc_negatives = round(((qty_negatives / total_records) * 100),2)
    perc_uniques  = round(((qty_uniques / total_records) * 100),2)
    if  flg_format: 
        perc_formats  = round(((qty_formats / total_records) * 100),2)
    if  flg_list: 
        perc_list  = round(((qty_list / total_records) * 100),2)  
    if  flg_range: 
        perc_range  = round(((qty_range / total_records) * 100),2)          
    # return format
    if qty_emptys > 0: 
        ret_emptys = f"{qty_emptys} ({perc_emptys}%)"
    else: 
        ret_emptys = 0
    if qty_nulls > 0: 
        ret_nulls = f"{qty_nulls} ({perc_nulls}%)"
    else: 
         ret_nulls = 0
    if qty_zeroes:
        ret_zeroes = f"{qty_zeroes} ({perc_zeroes}%)"
    else: 
        ret_zeroes = 0
    if qty_negatives > 0: 
        ret_negatives = f"{qty_negatives} ({perc_negatives}%)"
    else: 
        ret_negatives = 0
    ret_uniques =  perc_uniques
    if flg_format: 
        ret_formats =  f"{qty_formats} ({perc_formats}%)"
    else: 
        ret_formats = "no format"
    if flg_list: 
        ret_list =  f"{qty_list} ({perc_list}%)"
    else: 
        ret_list = "no list"
    if flg_range: 
        ret_range =  f"{qty_range} ({perc_range}%)"
    else: 
        ret_range = "no range"
    # return
    return low, high, ret_emptys, ret_nulls, ret_zeroes, ret_negatives, ret_uniques, ret_formats, ret_list, ret_range


