import os
from google.cloud import storage, bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv
from pathlib import Path
import glob

# Load environmental variables
load_dotenv()
tgt_base_dir = os.environ.get('TGT_BASE_DIR')
gbq_client = bigquery.Client()  
gcs_client = storage.Client()
project_id = os.environ.get('GCP_PROJECT_ID') 
bucket_name = os.environ.get('GCS_BUCKET')
location = os.environ.get('LOCATION')

# Get new stock data path
def get_stock_data_path():
    extension = 'csv'
    os.chdir(tgt_base_dir)
    files = glob.glob(f'*.{extension}')
    file_path = os.path.join(tgt_base_dir, files[0])  
    return file_path

# Load local cvs to GCS bucket
def upload_to_bucket(path_to_file, bucket_name):
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.blob(os.path.basename(path_to_file))
    blob.upload_from_filename(path_to_file)
    return blob.public_url

# Create data set if it doesn't exists in GBQ
def create_dataset(location):
    dataset_id = f"{project_id}.stock"
    try:
        gbq_client.get_dataset(dataset_id)  
        print("Dataset {} already exists".format(dataset_id))
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        dataset = gbq_client.create_dataset(dataset, timeout=30) 
        return print("Dataset {} is created".format(dataset_id))
    
# Identify latest csv in GCS
def find_latest_csv_in_gcs(bucket_name, prefix):
    bucket = gcs_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    csv_blobs = [blob for blob in blobs if blob.name.endswith('.csv')]
    csv_blobs.sort(key=lambda x: x.time_created, reverse=True)
    return f"gs://{bucket_name}/{csv_blobs[0].name}"

# Use to check is stock_stg already exists
def table_exists(client, table_id):
    try:
        client.get_table(table_id)
        return True
    except Exception as e:
        return False

# Create new stock_stg table each time
def create_or_overwrite_table(client, table_id, schema):
    table = bigquery.Table(table_id, schema=schema)
    if table_exists(client, table_id):
        client.delete_table(table_id)
    client.create_table(table)

# Load data into stock_stg
def load_data_into_stg_table(client, gcs_uri, table_id, job_config):
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # Waits for the job to complete

def main():
    csv_file_path = get_stock_data_path()
    upload_to_bucket(csv_file_path,bucket_name)
    create_dataset(location)

    # Set up stock_stg table
    stg_table_id = f"{project_id}.stock.stock_stg"
    gcs_uri = find_latest_csv_in_gcs(bucket_name, 'stocks_')
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = ";"
    job_config.skip_leading_rows = 1 
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = [
        bigquery.SchemaField("index", "INT64"),
        bigquery.SchemaField("open", "FLOAT64"),
        bigquery.SchemaField("previous_close", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("stock", "STRING"),
        bigquery.SchemaField("high", "FLOAT64"),
        bigquery.SchemaField("low", "FLOAT64"),
        bigquery.SchemaField("ttm_eps_raw", "STRING"),
        bigquery.SchemaField("year_high", "FLOAT64"),
        bigquery.SchemaField("year_low", "FLOAT64"),
        bigquery.SchemaField("ttm_pe_raw", "STRING"),
        bigquery.SchemaField("market_capital", "FLOAT64"),
        bigquery.SchemaField("dividend_yield", "STRING"),
        bigquery.SchemaField("stock_date", "DATE"),
    ]
    create_or_overwrite_table(gbq_client, stg_table_id, job_config.schema)
    load_data_into_stg_table(gbq_client, gcs_uri, stg_table_id, job_config)
    print(f"Data loaded from {gcs_uri} to BigQuery table {stg_table_id}.")

    # Create final tbl if not exists
    table_id = f"{project_id}.stock.stock_daily"
    schema = [
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("open_price", "FLOAT64"),
        bigquery.SchemaField("prev_close_price", "FLOAT64"),
        bigquery.SchemaField("beta", "FLOAT64"),
        bigquery.SchemaField("high", "FLOAT64"),
        bigquery.SchemaField("low", "FLOAT64"),
        bigquery.SchemaField("ttm_eps", "FLOAT64"),
        bigquery.SchemaField("ttm_eps_yoy", "STRING"),
        bigquery.SchemaField("year_high", "FLOAT64"),
        bigquery.SchemaField("year_low", "FLOAT64"),
        bigquery.SchemaField("ttm_pe", "FLOAT64"),
        bigquery.SchemaField("ttm_pe_range", "STRING"),
        bigquery.SchemaField("market_capital", "FLOAT64"),
        bigquery.SchemaField("dividend_yield", "STRING"),
        bigquery.SchemaField("stock_date", "DATE")  
    ]
    try:
        table = gbq_client.get_table(table_id)
    except NotFound:
        table = None
    if table is None:
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="stock_date"
        )
        table = gbq_client.create_table(table)
        print("Table {} created.".format(table.table_id))
    else:
        print("Table {} already exists.".format(table.table_id))

    # Insert data from stock_stg to stock_daily. Do not truncate stock_daily
    select_query = f"""
        INSERT INTO  `{table_id}` (ticker, open_price, prev_close_price, beta, high, low, ttm_eps, ttm_eps_yoy, year_high, year_low, ttm_pe, ttm_pe_range, market_capital, dividend_yield, stock_date)
        SELECT
            stock AS ticker,
            open AS open_price,
            previous_close AS prev_close_price,
            beta,
            high,
            low,
            CAST(REGEXP_EXTRACT(ttm_eps_raw, r'[0-9]+\.[0-9]+') AS FLOAT64) AS ttm_eps,
            REGEXP_EXTRACT(ttm_eps_raw, r'([-+]?\\d+\\.\\d+%)\s') AS ttm_eps_yoy,
            year_high,
            year_low,
            CAST(REGEXP_EXTRACT(ttm_pe_raw, r'[0-9]+\.[0-9]+') AS FLOAT64) AS ttm_pe,
            REGEXP_EXTRACT(ttm_pe_raw, r'\\(\\s*(.*?)\\s*\\)') AS ttm_pe_range,
            market_capital,
            REGEXP_EXTRACT(dividend_yield, r"\\['([^']+)'\\]") AS dividend_yield,
            stock_date
        FROM
            `{stg_table_id}`
    """
    job_config = bigquery.QueryJobConfig()
    query_job = gbq_client.query(select_query, job_config=job_config)
    query_job.result()
    if query_job.errors:
        print("Errors encountered during query execution:", query_job.errors)
    else:
        print("Query completed successfully. Data should be appended.")

    # Delete file in local after GBQ and GCS work
    try:
        os.remove(csv_file_path)
    except OSError:
        pass

if __name__== "__main__":
    main()