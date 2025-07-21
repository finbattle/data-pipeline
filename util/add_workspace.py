# Databricks notebook source
import sys

abs_path = (
    dbutils
    .notebook
    .entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

splited = abs_path.split('/')

last_index = splited.index('cvm-data-pipeline')

folder_path = '/Workspace' + '/'.join(splited[:last_index + 1])

print(f'Adding new sources to sys.path: "{folder_path}"')

sys.path.append(folder_path)
