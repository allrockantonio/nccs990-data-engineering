import functions as fnc
import build
import toml
import logging
import sys
import requests
import re
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from io import BytesIO
import pyarrow.parquet as pq
import sqlalchemy as sa
from process_timer import ProcessTimer

df_parquet = pd.read_parquet('parquets/SR-P06-T01-UNRLTD-ORGS-TAXABLE-PARTNERSHIP/sr-p06-t01-unrltd-orgs-taxable-partnership-2009.parquet')
for column in df_parquet.columns:
    print(column)