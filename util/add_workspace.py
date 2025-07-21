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

folder_path = '/Workspace' + '/'.join(splited[:-2])

print(f'Adding new sources to sys.path: "{folder_path}"')

sys.path.append(folder_path)
