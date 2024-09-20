import logging
import os
import json
from datetime import datetime
import pandas as pd 
from pathlib import Path
import sys 
from openpyxl import load_workbook 

# Variaveis globais
eda_sheet_full_path = None
config_file_full_path = Path(Path.cwd().parent /  r"config/config.json")
data_file_path = None
csv_sep, dec_sep, dt_frmt, dt_regex = "", "", "", "" 

def load_config():
    global eda_sheet_full_path, data_file_path, csv_sep, dec_sep, dt_frmt, dt_regex
    try:
        eda_config = pd.read_json(config_file_full_path, typ="series")
    except FileNotFoundError:
        print(f"Arquivo de configuração não encontrado ! {config_file_full_path}")
        raise SystemExit 
    except ValueError as e:
        print(f"Erro ao ler o JSON ({config_file_full_path}): {e}")
        raise SystemExit 
    except Exception as e:
        print(f"Erro inesperado ao carregar {config_file_full_path}: {e}")
        raise SystemExit 
    
    # eda sheet path
    eda_sheet_full_path =  Path(eda_config["eda_path"]) / eda_config["eda_file"]
    if not eda_sheet_full_path.exists():
        print(f"Planilha EDA não encontrada !\n{eda_sheet_full_path}")
        raise SystemExit
    
    # data file path
    data_file_path = Path(eda_config["data_path"]) 
    if not data_file_path.exists():
        print(f"Pasta de dados não encontrada !\n{data_file_path}")
        raise SystemExit
    
    csv_sep = eda_config["csv_separator"]
    dec_sep = eda_config["decimal_separator"]
    dt_frmt = eda_config["date_format"]
    dt_regex = eda_config["date_regex"]

    return None

def config_log(config_path: Path):
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    log_dir = config.get('log_path', './logs')
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d %H-%M-%S")
    log_file_name = f"{timestamp} eda.log"
    log_file_full_path = os.path.join(log_dir, log_file_name)

    logging.basicConfig(
        filename=log_file_full_path,
        filemode='w',
        format='%(asctime)s|%(levelname)s|%(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",   
        level=logging.INFO
    )

    logger = logging.getLogger('EDA')
    return logger