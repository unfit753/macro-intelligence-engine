import pandas as pd
import requests
from io import StringIO
import sqlite3
from config.config_fetch import DB_PATH, log

fred_url = (
    "https://fred.stlouisfed.org/graph/fredgraph.csv?"
    "id=GOLDAMGBD228NLBM"
)
resp = requests.get(fred_url)
resp.raise_for_status()
gold = pd.read_csv(StringIO(resp.text), parse_dates=['DATE'])
gold = gold.rename(columns={'DATE': 'ds', 'GOLDAMGBD228NLBM': 'y'})

print(f"Fetched {len(gold)} rows from {gold['ds'].min()} to {gold['ds'].max()}")
