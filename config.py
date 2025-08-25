# Configuración del proyecto
PROJECT_ID = "testmanager-470115"
BUCKET = "bucket_acero"
DATASET = "dataset_acero"
TABLE = "acero_table"
DATAMART_TABLE = "datamartclean"

# Configuración de Dask
DASK_CHUNKSIZE = "100MB"
DASK_MEMORY_LIMIT = "8GB"

# Configuración de BigQuery
BQ_LOCATION = "US"
BQ_WRITE_DISPOSITION = "WRITE_TRUNCATE"

# Configuración de procesamiento
BATCH_SIZE = 1000000  # 1M registros por lote
MAX_WORKERS = 4

# URLs de datos
DATA_URL = "https://storage.googleapis.com/desafio-deacero-143d30a0-d8f8-4154-b7df-1773cf286d32/cdo_challenge.csv.gz"
GS_PATH = "gs://desafio-deacero-143d30a0-d8f8-4154-b7df-1773cf286d32/cdo_challenge.csv.gz"

# Configuración de columnas
COLUMNS = [
    "transaction_id", "date", "timestamp", "customer_id", "customer_segment",
    "product_id", "product_category", "product_lifecycle", "quantity", "unit_price",
    "total_amount", "currency", "region", "warehouse", "status", "payment_method",
    "discount_pct", "tax_amount", "notes", "created_by", "modified_date"
]

# Configuración de limpieza
SPANISH_PRIORITY = True
DEFAULT_CURRENCY = "MXN"
DEFAULT_REGION = "Mexico"
