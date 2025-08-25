#!/usr/bin/env python3
"""
Procesador de datos en streaming desde Google Cloud Storage
Solución alternativa para entornos con espacio limitado
"""

import logging
import time
from datetime import datetime
from typing import Iterator, Dict, Any

import pandas as pd
import numpy as np
from google.cloud import storage, bigquery
from google.cloud.exceptions import GoogleCloudError
import python_dateutil.parser as date_parser

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamDataProcessor:
    """Procesador de datos en streaming desde GCS"""
    
    def __init__(self, project_id: str, bucket_name: str, dataset_id: str):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.dataset_id = dataset_id
        
        # Inicializar clientes
        self.storage_client = storage.Client(project=project_id)
        self.bq_client = bigquery.Client(project=project_id)
        
        # Configuración de procesamiento
        self.chunk_size = 100000  # 100K registros por chunk
        self.max_retries = 3
        
        # Esquema de BigQuery
        self.schema = [
            bigquery.SchemaField("transaction_id", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("timestamp", "DATETIME"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("customer_segment", "STRING"),
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("product_category", "STRING"),
            bigquery.SchemaField("product_lifecycle", "STRING"),
            bigquery.SchemaField("quantity", "INTEGER"),
            bigquery.SchemaField("unit_price", "FLOAT64"),
            bigquery.SchemaField("total_amount", "FLOAT64"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("warehouse", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("payment_method", "STRING"),
            bigquery.SchemaField("discount_pct", "FLOAT64"),
            bigquery.SchemaField("tax_amount", "FLOAT64"),
            bigquery.SchemaField("notes", "STRING"),
            bigquery.SchemaField("created_by", "STRING"),
            bigquery.SchemaField("modified_date", "DATE"),
            bigquery.SchemaField("AnioMes", "INTEGER"),
            bigquery.SchemaField("quantity_null", "INTEGER"),
            bigquery.SchemaField("unit_price_imput", "FLOAT64")
        ]
    
    def clean_text_data(self, text_series: pd.Series) -> pd.Series:
        """Limpia caracteres corruptos en texto"""
        if text_series.dtype == 'object':
            # Reemplazar caracteres corruptos comunes
            text_series = text_series.astype(str).str.replace('Ã±', 'ñ', regex=False)
            text_series = text_series.astype(str).str.replace('Ã±Ã±', 'ññ', regex=False)
            text_series = text_series.astype(str).str.replace('STÃ±', 'STÑ', regex=False)
            text_series = text_series.astype(str).str.replace('UñER0ââ', 'USER0', regex=False)
            text_series = text_series.astype(str).str.replace('â', '', regex=False)
            text_series = text_series.astype(str).str.replace('Ã', '', regex=False)
            
            # Normalizar espacios
            text_series = text_series.str.strip()
            text_series = text_series.str.replace(r'\s+', ' ', regex=True)
            
            # Convertir a mayúsculas para consistencia
            text_series = text_series.str.upper()
        
        return text_series
    
    def clean_date_column(self, date_series: pd.Series) -> pd.Series:
        """Limpia y normaliza fechas"""
        def parse_date(date_str):
            if pd.isna(date_str) or str(date_str).strip() == '':
                return None
            
            date_str = str(date_str).strip()
            
            # Intentar diferentes formatos
            formats = [
                '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y%m%d',
                '%d/%m/%Y', '%m-%d-%Y', '%Y/%m/%d'
            ]
            
            for fmt in formats:
                try:
                    return pd.to_datetime(date_str, format=fmt).date()
                except:
                    continue
            
            # Si no funciona, intentar con dateutil
            try:
                return date_parser.parse(date_str).date()
            except:
                return None
        
        return date_series.apply(parse_date)
    
    def normalize_categories(self, series: pd.Series, category_type: str) -> pd.Series:
        """Normaliza categorías a valores estándar en español"""
        if category_type == 'customer_segment':
            mapping = {
                'EMPRESARIAL': ['EMPRESARIAL', 'BUSINESS', 'CORPORATE', 'EMPRESA'],
                'GUBERNAMENTAL': ['GUBERNAMENTAL', 'GOVERNMENT', 'GUBERNMENTAL'],
                'INDIVIDUAL': ['INDIVIDUAL', 'PERSONAL', 'INDIVIDUO', 'CLIENTE'],
                'MAYORISTA': ['MAYORISTA', 'WHOLESALE', 'MAYOREO']
            }
        elif category_type == 'product_category':
            mapping = {
                'VARILLA': ['VARILLA', 'REBAR', 'VARILLAS'],
                'ALAMBRE': ['ALAMBRE', 'WIRE', 'ALAMBRES'],
                'PERFIL': ['PERFIL', 'PROFILE', 'PERFILES'],
                'PLACA': ['PLACA', 'PLATE', 'PLACAS'],
                'TUBO': ['TUBO', 'TUBE', 'TUBOS']
            }
        elif category_type == 'status':
            mapping = {
                'COMPLETADO': ['COMPLETADO', 'COMPLETED', 'FINALIZADO', 'DONE'],
                'PENDIENTE': ['PENDIENTE', 'PENDING', 'EN_PROCESO'],
                'CANCELADO': ['CANCELADO', 'CANCELLED', 'CANCELED'],
                'DEVUELTO': ['DEVUELTO', 'RETURNED', 'REFUNDED']
            }
        else:
            return series
        
        def normalize_value(value):
            if pd.isna(value):
                return 'NO_ESPECIFICADO'
            
            value_str = str(value).upper().strip()
            
            for standard, variants in mapping.items():
                if any(variant in value_str for variant in variants):
                    return standard
            
            return value_str
        
        return series.apply(normalize_value)
    
    def process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Procesa un chunk de datos"""
        logger.info(f"Procesando chunk de {len(chunk)} registros")
        
        # Limpiar texto
        text_columns = ['transaction_id', 'customer_id', 'product_id', 'warehouse', 'notes', 'created_by']
        for col in text_columns:
            if col in chunk.columns:
                chunk[col] = self.clean_text_data(chunk[col])
        
        # Limpiar fechas
        date_columns = ['date', 'timestamp', 'modified_date']
        for col in date_columns:
            if col in chunk.columns:
                chunk[col] = self.clean_date_column(chunk[col])
        
        # Normalizar categorías
        chunk['customer_segment'] = self.normalize_categories(chunk['customer_segment'], 'customer_segment')
        chunk['product_category'] = self.normalize_categories(chunk['product_category'], 'product_category')
        chunk['status'] = self.normalize_categories(chunk['status'], 'status')
        
        # Valores por defecto
        chunk['currency'] = chunk['currency'].fillna('MXN')
        chunk['region'] = chunk['region'].fillna('MEXICO')
        
        # Crear columnas derivadas
        chunk['AnioMes'] = chunk['date'].apply(
            lambda x: int(f"{x.year}{x.month:02d}") if pd.notna(x) else None
        )
        chunk['quantity_null'] = chunk['quantity'].isna().astype(int)
        
        # Convertir tipos
        numeric_columns = ['quantity', 'unit_price', 'total_amount', 'discount_pct', 'tax_amount']
        for col in numeric_columns:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
        
        return chunk
    
    def stream_from_gcs(self, blob_name: str) -> Iterator[pd.DataFrame]:
        """Lee datos desde GCS en chunks sin descargar el archivo completo"""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            raise FileNotFoundError(f"Archivo {blob_name} no encontrado en bucket {self.bucket_name}")
        
        logger.info(f"Iniciando streaming desde {blob_name} ({blob.size / (1024**3):.2f} GB)")
        
        # Leer en chunks pequeños
        chunk_size = 1024 * 1024  # 1MB por chunk
        offset = 0
        
        buffer = ""
        line_count = 0
        
        while offset < blob.size:
            # Leer chunk
            end = min(offset + chunk_size, blob.size)
            chunk_data = blob.download_as_text(start=offset, end=end)
            
            # Agregar al buffer
            buffer += chunk_data
            
            # Procesar líneas completas
            lines = buffer.split('\n')
            
            # Mantener la última línea incompleta en el buffer
            buffer = lines[-1]
            lines = lines[:-1]
            
            if lines:
                # Convertir a DataFrame
                try:
                    # Crear CSV temporal
                    csv_content = '\n'.join(lines)
                    df_chunk = pd.read_csv(pd.StringIO(csv_content), low_memory=False)
                    
                    if not df_chunk.empty:
                        line_count += len(df_chunk)
                        logger.info(f"Procesadas {line_count} líneas, offset: {offset}")
                        
                        yield df_chunk
                        
                except Exception as e:
                    logger.warning(f"Error procesando chunk en offset {offset}: {e}")
                    continue
            
            offset = end
            
            # Control de memoria
            if line_count % (self.chunk_size * 10) == 0:
                logger.info(f"Procesadas {line_count} líneas, liberando memoria...")
        
        # Procesar el buffer final
        if buffer.strip():
            try:
                df_final = pd.read_csv(pd.StringIO(buffer), low_memory=False)
                if not df_final.empty:
                    yield df_final
            except Exception as e:
                logger.warning(f"Error procesando buffer final: {e}")
    
    def upload_to_bigquery(self, df: pd.DataFrame, table_name: str) -> bool:
        """Sube un DataFrame a BigQuery"""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            # Configurar job
            job_config = bigquery.LoadJobConfig(
                schema=self.schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
            )
            
            # Subir datos
            job = self.bq_client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Esperar a que termine
            
            logger.info(f"Subidos {len(df)} registros a {table_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error subiendo a BigQuery: {e}")
            return False
    
    def run_streaming_pipeline(self, blob_name: str, table_name: str = "datamartclean") -> bool:
        """Ejecuta el pipeline completo de streaming"""
        logger.info("🚀 Iniciando pipeline de streaming...")
        start_time = time.time()
        
        try:
            # Crear tabla si no existe
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            try:
                self.bq_client.get_table(table_id)
                logger.info(f"Tabla {table_id} ya existe")
            except:
                # Crear tabla vacía
                table = bigquery.Table(table_id, schema=self.schema)
                self.bq_client.create_table(table)
                logger.info(f"Tabla {table_id} creada")
            
            # Procesar datos en streaming
            total_processed = 0
            chunk_count = 0
            
            for chunk in self.stream_from_gcs(blob_name):
                # Procesar chunk
                processed_chunk = self.process_chunk(chunk)
                
                # Subir a BigQuery
                if self.upload_to_bigquery(processed_chunk, table_name):
                    total_processed += len(processed_chunk)
                    chunk_count += 1
                    
                    logger.info(f"Chunk {chunk_count} procesado: {len(processed_chunk)} registros")
                    logger.info(f"Total procesado: {total_processed:,} registros")
                
                # Control de memoria
                del processed_chunk
                del chunk
            
            elapsed_time = time.time() - start_time
            logger.info(f"✅ Pipeline completado en {elapsed_time/60:.2f} minutos")
            logger.info(f"Total de registros procesados: {total_processed:,}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en pipeline de streaming: {e}")
            return False

def main():
    """Función principal"""
    # Configuración
    PROJECT_ID = "testmanager-470115"
    BUCKET_NAME = "bucket_acero"
    DATASET_ID = "dataset_acero"
    BLOB_NAME = "desafio-deacero-143d30a0-d8f8-4154-b7df-1773cf286d32/cdo_challenge.csv.gz"
    
    # Crear procesador
    processor = StreamDataProcessor(PROJECT_ID, BUCKET_NAME, DATASET_ID)
    
    # Ejecutar pipeline
    success = processor.run_streaming_pipeline(BLOB_NAME)
    
    if success:
        print("🎉 Pipeline de streaming completado exitosamente!")
        print("📊 Los datos están disponibles en BigQuery")
    else:
        print("❌ El pipeline falló. Revisar logs para más detalles.")

if __name__ == "__main__":
    main()
