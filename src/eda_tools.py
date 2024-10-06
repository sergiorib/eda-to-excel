import os 
import sys
from pathlib import Path
from xmlrpc.client import Boolean
from charset_normalizer import from_path
import pandas as pd
from pandas import DataFrame
import re
import config as cfg
import csv
import traceback

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

def check_pk_unique(table_name:str,df_data: DataFrame, df_fields: DataFrame):  
    pk_fields = df_fields[(df_fields['table'] == table_name) & (df_fields['pk'] == 'yes')]['field'].to_list() 
    count_pk_unique = df_data[pk_fields].drop_duplicates().shape[0]
    return count_pk_unique

def check_pk_not_null(table_name:str, df_data: DataFrame, df_fields: DataFrame):  
    pk_fields = df_fields[(df_fields['table'] == table_name) & (df_fields['pk'] == 'yes')]['field'].to_list() 
    df_pk = df_data[pk_fields]
    is_null = df_pk.isnull()
    has_null_pk = is_null.any(axis=1)
    count_null_pk = has_null_pk.sum()
    count_pk_not_null = len(df_data) - count_null_pk
    return count_pk_not_null

def check_ref_integrity(table_name:str, field_name:str, fk_table: str  , fk_field: str  , df_tables: DataFrame, df_data: DataFrame):   
    # fk field_values 
    sr_data = df_data[field_name]  
    sr_data = sr_data.astype(str).str.strip()
    # sr_data.sort_values(ascending=True,inplace=True)             
    fk_file_name = df_tables[df_tables['table'] == fk_table]['file'].item() 
    data_path_fk = Path(cfg.data_file_path / fk_file_name)
    try: 
        df_data_related = pd.read_csv(data_path_fk,encoding=encode(data_path_fk),quotechar=None,quoting=3,keep_default_na=True,sep=cfg.csv_sep,engine='python')
        df_data_related.columns = df_data_related.columns.str.strip().str.lower()
        sr_data_related = df_data_related[fk_field] 
        sr_data_related = sr_data_related.astype(str).str.strip()  
        # sr_data_related.sort_values(ascending=True,inplace=True)  
        exist_mask = sr_data.isin(sr_data_related)
        fk_found = exist_mask.sum() 
        # not_exist_mask = ~sr_data.isin(sr_data_related)
        sr_not_exist = sr_data[~exist_mask].unique() 
        print(sr_not_exist)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info() 
        print("🛑 Erro  check_ref!")
        print(f"📝 Tipo de Erro: {type(e).__name__}")
        linha_do_erro = exc_tb.tb_lineno            
        print(f"👉 Linha do Código que Gerou o Erro: {linha_do_erro}")
        print("\n--- Traceback Completo ---")
        return 'err'
    return fk_found