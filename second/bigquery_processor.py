#!/usr/bin/env python3
"""
Procesador de datos usando BigQuery directamente
Solución alternativa sin descarga de archivos
"""

import logging
import time
from datetime import datetime
from typing import List, Dict, Any

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BigQueryDataProcessor:
    """Procesador de datos usando BigQuery directamente"""
    
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        
        # Configuración
        self.location = "US"
        self.max_retries = 3
        
    def create_external_table(self, gcs_uri: str, table_name: str = "raw_data") -> bool:
        """Crea una tabla externa en BigQuery apuntando a GCS"""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            # Esquema para datos CSV
            schema = [
                bigquery.SchemaField("transaction_id", "STRING"),
                bigquery.SchemaField("date", "STRING"),  # Como string primero
                bigquery.SchemaField("timestamp", "STRING"),
                bigquery.SchemaField("customer_id", "STRING"),
                bigquery.SchemaField("customer_segment", "STRING"),
                bigquery.SchemaField("product_id", "STRING"),
                bigquery.SchemaField("product_category", "STRING"),
                bigquery.SchemaField("product_lifecycle", "STRING"),
                bigquery.SchemaField("quantity", "STRING"),  # Como string primero
                bigquery.SchemaField("unit_price", "STRING"),
                bigquery.SchemaField("total_amount", "STRING"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("region", "STRING"),
                bigquery.SchemaField("warehouse", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("payment_method", "STRING"),
                bigquery.SchemaField("discount_pct", "STRING"),
                bigquery.SchemaField("tax_amount", "STRING"),
                bigquery.SchemaField("notes", "STRING"),
                bigquery.SchemaField("created_by", "STRING"),
                bigquery.SchemaField("modified_date", "STRING")
            ]
            
            # Configuración de tabla externa
            external_config = bigquery.ExternalConfig("CSV")
            external_config.source_uris = [gcs_uri]
            external_config.schema = schema
            external_config.options.skip_leading_rows = 1  # Saltar header
            external_config.options.allow_quoted_newlines = True
            external_config.options.allow_jagged_rows = True
            
            # Crear tabla
            table = bigquery.Table(table_id, schema=schema)
            table.external_data_configuration = external_config
            
            # Crear o reemplazar tabla
            self.bq_client.delete_table(table_id, not_found_ok=True)
            table = self.bq_client.create_table(table)
            
            logger.info(f"✅ Tabla externa creada: {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando tabla externa: {e}")
            return False
    
    def create_clean_table(self, source_table: str = "raw_data", target_table: str = "datamartclean") -> bool:
        """Crea la tabla limpia usando SQL de BigQuery"""
        try:
            target_table_id = f"{self.project_id}.{self.dataset_id}.{target_table}"
            source_table_id = f"{self.project_id}.{self.dataset_id}.{source_table}"
            
            # SQL para limpieza y transformación
            clean_sql = f"""
            CREATE OR REPLACE TABLE `{target_table_id}` AS
            WITH cleaned_data AS (
                SELECT 
                    -- Limpiar transaction_id
                    TRIM(REGEXP_REPLACE(transaction_id, r'[Ã±â]', '')) as transaction_id,
                    
                    -- Limpiar y convertir fechas
                    SAFE.PARSE_DATE('%Y-%m-%d', 
                        REGEXP_REPLACE(date, r'[Ã±â]', '')
                    ) as date,
                    
                    SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', 
                        REGEXP_REPLACE(timestamp, r'[Ã±â]', '')
                    ) as timestamp,
                    
                    -- Limpiar customer_id
                    TRIM(REGEXP_REPLACE(customer_id, r'[Ã±â]', '')) as customer_id,
                    
                    -- Normalizar customer_segment
                    CASE 
                        WHEN LOWER(customer_segment) LIKE '%empres%' OR LOWER(customer_segment) LIKE '%business%' THEN 'EMPRESARIAL'
                        WHEN LOWER(customer_segment) LIKE '%gob%' OR LOWER(customer_segment) LIKE '%gov%' THEN 'GUBERNAMENTAL'
                        WHEN LOWER(customer_segment) LIKE '%indiv%' OR LOWER(customer_segment) LIKE '%personal%' THEN 'INDIVIDUAL'
                        WHEN LOWER(customer_segment) LIKE '%mayor%' OR LOWER(customer_segment) LIKE '%wholesale%' THEN 'MAYORISTA'
                        ELSE UPPER(TRIM(REGEXP_REPLACE(customer_segment, r'[Ã±â]', '')))
                    END as customer_segment,
                    
                    -- Limpiar product_id
                    TRIM(REGEXP_REPLACE(product_id, r'[Ã±â]', '')) as product_id,
                    
                    -- Normalizar product_category
                    CASE 
                        WHEN LOWER(product_category) LIKE '%varill%' OR LOWER(product_category) LIKE '%rebar%' THEN 'VARILLA'
                        WHEN LOWER(product_category) LIKE '%alambr%' OR LOWER(product_category) LIKE '%wire%' THEN 'ALAMBRE'
                        WHEN LOWER(product_category) LIKE '%perfil%' OR LOWER(product_category) LIKE '%profile%' THEN 'PERFIL'
                        WHEN LOWER(product_category) LIKE '%plac%' OR LOWER(product_category) LIKE '%plate%' THEN 'PLACA'
                        WHEN LOWER(product_category) LIKE '%tub%' OR LOWER(product_category) LIKE '%tube%' THEN 'TUBO'
                        ELSE UPPER(TRIM(REGEXP_REPLACE(product_category, r'[Ã±â]', '')))
                    END as product_category,
                    
                    -- Normalizar product_lifecycle
                    CASE 
                        WHEN LOWER(product_lifecycle) LIKE '%nuev%' OR LOWER(product_lifecycle) LIKE '%new%' THEN 'NUEVO'
                        WHEN LOWER(product_lifecycle) LIKE '%madur%' OR LOWER(product_lifecycle) LIKE '%mature%' THEN 'MADURO'
                        WHEN LOWER(product_lifecycle) LIKE '%declin%' OR LOWER(product_lifecycle) LIKE '%decline%' THEN 'DECLINANTE'
                        ELSE UPPER(TRIM(REGEXP_REPLACE(product_lifecycle, r'[Ã±â]', '')))
                    END as product_lifecycle,
                    
                    -- Convertir cantidad
                    SAFE_CAST(quantity AS INT64) as quantity,
                    
                    -- Convertir precios
                    SAFE_CAST(unit_price AS FLOAT64) as unit_price,
                    SAFE_CAST(total_amount AS FLOAT64) as total_amount,
                    
                    -- Normalizar moneda
                    COALESCE(currency, 'MXN') as currency,
                    
                    -- Normalizar región
                    CASE 
                        WHEN LOWER(region) LIKE '%mex%' OR LOWER(region) LIKE '%mx%' THEN 'MEXICO'
                        WHEN LOWER(region) LIKE '%norte%' OR LOWER(region) LIKE '%north%' THEN 'NORTE'
                        WHEN LOWER(region) LIKE '%sur%' OR LOWER(region) LIKE '%south%' THEN 'SUR'
                        WHEN LOWER(region) LIKE '%este%' OR LOWER(region) LIKE '%east%' THEN 'ESTE'
                        WHEN LOWER(region) LIKE '%oeste%' OR LOWER(region) LIKE '%west%' THEN 'OESTE'
                        ELSE COALESCE(UPPER(TRIM(REGEXP_REPLACE(region, r'[Ã±â]', ''))), 'MEXICO')
                    END as region,
                    
                    -- Limpiar warehouse
                    TRIM(REGEXP_REPLACE(warehouse, r'[Ã±â]', '')) as warehouse,
                    
                    -- Normalizar status
                    CASE 
                        WHEN LOWER(status) LIKE '%complet%' OR LOWER(status) LIKE '%done%' THEN 'COMPLETADO'
                        WHEN LOWER(status) LIKE '%pend%' OR LOWER(status) LIKE '%pending%' THEN 'PENDIENTE'
                        WHEN LOWER(status) LIKE '%cancel%' THEN 'CANCELADO'
                        WHEN LOWER(status) LIKE '%devuel%' OR LOWER(status) LIKE '%return%' THEN 'DEVUELTO'
                        ELSE UPPER(TRIM(REGEXP_REPLACE(status, r'[Ã±â]', '')))
                    END as status,
                    
                    -- Normalizar payment_method
                    CASE 
                        WHEN LOWER(payment_method) LIKE '%efectiv%' OR LOWER(payment_method) LIKE '%cash%' THEN 'EFECTIVO'
                        WHEN LOWER(payment_method) LIKE '%tarjeta%' OR LOWER(payment_method) LIKE '%card%' THEN 'TARJETA'
                        WHEN LOWER(payment_method) LIKE '%transfer%' OR LOWER(payment_method) LIKE '%wire%' THEN 'TRANSFERENCIA'
                        WHEN LOWER(payment_method) LIKE '%cheque%' OR LOWER(payment_method) LIKE '%check%' THEN 'CHEQUE'
                        ELSE UPPER(TRIM(REGEXP_REPLACE(payment_method, r'[Ã±â]', '')))
                    END as payment_method,
                    
                    -- Convertir descuentos e impuestos
                    SAFE_CAST(discount_pct AS FLOAT64) as discount_pct,
                    SAFE_CAST(tax_amount AS FLOAT64) as tax_amount,
                    
                    -- Limpiar notes
                    TRIM(REGEXP_REPLACE(notes, r'[Ã±â]', '')) as notes,
                    
                    -- Limpiar created_by
                    TRIM(REGEXP_REPLACE(created_by, r'[Ã±â]', '')) as created_by,
                    
                    -- Limpiar modified_date
                    SAFE.PARSE_DATE('%Y-%m-%d', 
                        REGEXP_REPLACE(modified_date, r'[Ã±â]', '')
                    ) as modified_date
                    
                FROM `{source_table_id}`
                WHERE transaction_id IS NOT NULL  -- Filtrar filas válidas
            ),
            
            with_derived_fields AS (
                SELECT 
                    *,
                    -- Crear AnioMes
                    CAST(FORMAT_DATE('%Y%m', date) AS INT64) as AnioMes,
                    
                    -- Crear quantity_null
                    CASE WHEN quantity IS NULL THEN 1 ELSE 0 END as quantity_null
                    
                FROM cleaned_data
                WHERE date IS NOT NULL  -- Solo filas con fecha válida
            )
            
            SELECT * FROM with_derived_fields
            """
            
            # Ejecutar query
            query_job = self.bq_client.query(clean_sql)
            query_job.result()  # Esperar a que termine
            
            logger.info(f"✅ Tabla limpia creada: {target_table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando tabla limpia: {e}")
            return False
    
    def add_unit_price_imputation(self, table_name: str = "datamartclean") -> bool:
        """Agrega la columna unit_price_imput con imputación inteligente"""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            # SQL para imputación de precios
            imputation_sql = f"""
            ALTER TABLE `{table_id}` 
            ADD COLUMN unit_price_imput FLOAT64;
            
            UPDATE `{table_id}` 
            SET unit_price_imput = COALESCE(
                unit_price,
                -- Imputación por AnioMes, region, product_id
                (SELECT AVG(unit_price) 
                 FROM `{table_id}` t2 
                 WHERE t2.AnioMes = `{table_id}`.AnioMes 
                   AND t2.region = `{table_id}`.region 
                   AND t2.product_id = `{table_id}`.product_id
                   AND t2.unit_price IS NOT NULL),
                
                -- Imputación por AnioMes, region, product_category
                (SELECT AVG(unit_price) 
                 FROM `{table_id}` t2 
                 WHERE t2.AnioMes = `{table_id}`.AnioMes 
                   AND t2.region = `{table_id}`.region 
                   AND t2.product_category = `{table_id}`.product_category
                   AND t2.unit_price IS NOT NULL),
                
                -- Imputación por AnioMes, region
                (SELECT AVG(unit_price) 
                 FROM `{table_id}` t2 
                 WHERE t2.AnioMes = `{table_id}`.AnioMes 
                   AND t2.region = `{table_id}`.region
                   AND t2.unit_price IS NOT NULL),
                
                -- Promedio general
                (SELECT AVG(unit_price) 
                 FROM `{table_id}` t2 
                 WHERE t2.unit_price IS NOT NULL)
            )
            WHERE unit_price IS NULL;
            """
            
            # Ejecutar query
            query_job = self.bq_client.query(imputation_sql)
            query_job.result()  # Esperar a que termine
            
            logger.info(f"✅ Imputación de precios completada en {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en imputación de precios: {e}")
            return False
    
    def get_table_stats(self, table_name: str = "datamartclean") -> Dict[str, Any]:
        """Obtiene estadísticas de la tabla"""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            stats_sql = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(DISTINCT product_id) as unique_products,
                COUNT(DISTINCT region) as unique_regions,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                AVG(unit_price) as avg_unit_price,
                AVG(total_amount) as avg_total_amount,
                SUM(total_amount) as total_revenue,
                COUNT(CASE WHEN unit_price IS NULL THEN 1 END) as null_prices,
                COUNT(CASE WHEN quantity IS NULL THEN 1 END) as null_quantities
            FROM `{table_id}`
            """
            
            query_job = self.bq_client.query(stats_sql)
            results = query_job.result()
            
            stats = {}
            for row in results:
                stats = dict(row.items())
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo estadísticas: {e}")
            return {}
    
    def run_complete_pipeline(self, gcs_uri: str) -> bool:
        """Ejecuta el pipeline completo"""
        logger.info("🚀 Iniciando pipeline completo de BigQuery...")
        start_time = time.time()
        
        try:
            # 1. Crear tabla externa
            logger.info("📋 Paso 1: Creando tabla externa...")
            if not self.create_external_table(gcs_uri, "raw_data"):
                return False
            
            # 2. Crear tabla limpia
            logger.info("🧹 Paso 2: Creando tabla limpia...")
            if not self.create_clean_table("raw_data", "datamartclean"):
                return False
            
            # 3. Agregar imputación de precios
            logger.info("💰 Paso 3: Agregando imputación de precios...")
            if not self.add_unit_price_imputation("datamartclean"):
                return False
            
            # 4. Obtener estadísticas
            logger.info("📊 Paso 4: Obteniendo estadísticas...")
            stats = self.get_table_stats("datamartclean")
            
            elapsed_time = time.time() - start_time
            logger.info(f"✅ Pipeline completado en {elapsed_time/60:.2f} minutos")
            
            # Mostrar estadísticas
            if stats:
                logger.info("📈 Estadísticas de la tabla final:")
                for key, value in stats.items():
                    if isinstance(value, float):
                        logger.info(f"   {key}: {value:,.2f}")
                    else:
                        logger.info(f"   {key}: {value:,}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en pipeline completo: {e}")
            return False

def main():
    """Función principal"""
    # Configuración
    PROJECT_ID = "testmanager-470115"
    DATASET_ID = "dataset_acero"
    
    # URI de GCS (sin descargar)
    GCS_URI = "gs://desafio-deacero-143d30a0-d8f8-4154-b7df-1773cf286d32/cdo_challenge.csv.gz"
    
    # Crear procesador
    processor = BigQueryDataProcessor(PROJECT_ID, DATASET_ID)
    
    # Ejecutar pipeline
    success = processor.run_complete_pipeline(GCS_URI)
    
    if success:
        print("🎉 Pipeline de BigQuery completado exitosamente!")
        print("📊 Los datos están disponibles en BigQuery")
        print("🔗 Puedes consultar la tabla 'datamartclean' en BigQuery Console")
    else:
        print("❌ El pipeline falló. Revisar logs para más detalles.")

if __name__ == "__main__":
    main()
