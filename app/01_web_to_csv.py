import urllib.request
from html_table_parser.parser import HTMLTableParser
from datetime import datetime
import json
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

# Load local environmental variables
load_dotenv()
src_base_dir = os.environ.get('SRC_BASE_DIR')
tgt_base_dir = os.environ.get('TGT_BASE_DIR')

# Load user specified stocks
with open(f'{src_base_dir}/config.json', "r") as jsonfile:
    config = json.load(jsonfile)

def url_get_contents(url):
    req = urllib.request.Request(url=url)
    f = urllib.request.urlopen(req)
    return f.read()

# Creates key-pairs of web data, assigns id and converts to list to support dataframe merg
def tbl_to_list(data, id, my_list):
    item_dict = {item[0]: item[1:] for item in data}
    item_dict.update(id)
    my_list.append(item_dict)

# Rename columns for better readability to .csv
def clean_column_names(df):
    df = df.rename(columns={
        'Open': 'open',
        'Previous Close': 'previous_close',
        'Beta': 'beta',
        'High': 'high',
        'Low': 'low',
        'TTM EPS  See historical trend': 'ttm_eps_raw',
        '52 Week High': 'year_high',
        '52 Week Low': 'year_low',
        'TTM PE See historical trend': 'ttm_pe_raw',
        'Mkt. Cap ($ Billion)': 'market_capital',
        'Dividend Yield': 'dividend_yield'
    })
    return df

# Convert specific python objects to float
def convert_to_float(df, columns):
    for col in columns:
        df[col] = df[col].astype(str). \
            str.replace("['", '').str.replace("']", '').str.replace(",", ''). \
                astype(float)
    return df

# Set empty table lists
list1 = []
list2 = []
list3 = []
list4 = []

def main():

    for index in range(len(config)):
        for key in config[index]:
            
            xhtml = url_get_contents(config[index][key]).decode('utf-8')
            p = HTMLTableParser()
            p.feed(xhtml)

            id = {"stock": (config[index][key]).split('/')[-1]}

            data = p.tables[1]
            tbl_to_list(data, id, list1)
            tdf1 = pd.DataFrame(list1)

            data = p.tables[2]
            tbl_to_list(data, id, list2)
            tdf2 = pd.DataFrame(list2)

            data = p.tables[3]
            tbl_to_list(data, id, list3)
            tdf3 = pd.DataFrame(list3)

            data = p.tables[4]
            tbl_to_list(data, id, list4)
            tdf4 = pd.DataFrame(list4)

    # Merge stocks data and update column names
    tdf = pd.merge(tdf1, tdf2, on='stock').merge(tdf3, on='stock').merge(tdf4, on='stock')
    tdf = clean_column_names(tdf)

    # Specity float columns
    float_columns = ['open', 'previous_close', 'beta', 'high', 'low', 'year_high', 'year_low', 'market_capital']
    tdf = convert_to_float(tdf, float_columns)

    # Create stock_date column
    date = datetime.today().strftime('%Y-%m-%d')
    tdf['stock_date'] = f'{date}'

    # Export to .csv
    tdf.to_csv(f'{tgt_base_dir}/stocks_{date}.csv', sep=";")

if __name__== "__main__":
    main()