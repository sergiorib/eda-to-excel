from pathlib import Path
from xmlrpc.client import Boolean
from charset_normalizer import from_path
import pandas as pd
import re
import config as cfg

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

# def int_or_decimal(value):
#     global dec_sep     
#     v_dec_sep = dec_sep
#     v = str(value).strip()
#     if v_dec_sep != ".":
#         v = v.replace(v_dec_sep, ".")
#     num = float(v)
#     return "int" if num.is_integer() else "float"

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