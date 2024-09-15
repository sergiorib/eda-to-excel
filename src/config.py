import logging
import os
import json
from datetime import datetime
import pandas as pd 
from pathlib import Path
import sys 

# Variaveis globais
eda_sheet_full_path = None
config_file_name = 'config.json'
data_file_path = None
separator = ""

def load_config():
    global eda_sheet_full_path, data_file_path, separator
    try:
        eda_config = pd.read_json(config_file_name, typ="series")
    except FileNotFoundError:
        print(f"Arquivo de configuração não encontrado ! {config_file_name}")
        raise SystemExit 
    except ValueError as e:
        print(f"Erro ao ler o JSON ({config_file_name}): {e}")
        raise SystemExit 
    except Exception as e:
        print(f"Erro inesperado ao carregar {config_file_name}: {e}")
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

    separator = eda_config["separator"]

    return None

def log_config(config_path='config.json'):
    """
    Configure a global logger for the project.
    Reads 'log_path' from eda_config.json and creates a timestamped log file.
    """

    # --- Load configuration file ---
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error loading log configuration file !\n{config_path}")
        print(f"Details: {e}")
        sys.exit(1)

    # --- Extract log directory from configuration ---
    log_dir = config.get('log_path', './logs')
    os.makedirs(log_dir, exist_ok=True)  # Create directory if it doesn't exist

    # --- Create timestamped log file name ---
    timestamp = datetime.now().strftime("%Y%m%d %H-%M-%S")
    log_file_name = f"{timestamp} eda.log"
    log_file_full_path = os.path.join(log_dir, log_file_name)

    # --- Configure base logging setup ---
    logging.basicConfig(
        filename=log_file_full_path,
        filemode='w',  # Create a new log file for each execution
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        level=logging.INFO
    )

    # --- Add console handler (optional) ---
    # console = logging.StreamHandler()
    # console.setLevel(logging.INFO)
    # console.setFormatter(logging.Formatter('%(levelname)-8s | %(message)s'))
    # logging.getLogger().addHandler(console)

    # --- Create logger instance ---
    logger = logging.getLogger('EDA')
    # logger.info(f"Log file created: {log_file_name}")

    return logger
