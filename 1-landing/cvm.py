# Databricks notebook source
# MAGIC %run ../util/add_workspace

# COMMAND ----------

# MAGIC %run ../util/create_workspace

# COMMAND ----------

import datetime
import pathlib
import os
from zipfile import ZipFile

from util import base_functions

# COMMAND ----------

environment = dbutils.widgets.get('environment')

# COMMAND ----------

file = dbutils.widgets.get('file')
year = dbutils.widgets.get('year')
url = f'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{file.upper()}/DADOS/{file.lower()}_cia_aberta_{year}.zip'

# COMMAND ----------

received_bytes = base_functions.download_file(url)

# COMMAND ----------

now = datetime.datetime.now()
prefix = now.strftime('%Y/%m/%d/%H/%M/%S')
directory = f'/Volumes/{environment}/landing/cvm/{file}/{prefix}'

# COMMAND ----------

pathlib.Path(directory).mkdir(parents=True, exist_ok=True)

# COMMAND ----------

myzip = ZipFile(received_bytes)
files = [i.filename for i in myzip.infolist()]
for file in files:
    csv_file = myzip.open(file)
    with open(f'{directory}/{file}', 'wb') as f:
        f.write(csv_file.read())
