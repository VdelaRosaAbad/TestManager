#!/usr/bin/env python3
"""
Script principal para procesamiento de datos de acero usando Dask
Maneja archivos grandes de manera eficiente y los carga a BigQuery
"""

import dask.dataframe as dd
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import storage
import logging
import time
from datetime import datetime, timedelta
import re
from config import *

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AceroDataProcessor:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)
        self.storage_client = storage.Client(project=PROJECT_ID)
        self.processed_rows = 0
        self.start_time = time.time()
        
    def download_and_decompress(self):
        """Descarga y descomprime el archivo de datos"""
        logger.info("Iniciando descarga del archivo de datos...")
        
        try:
            # Usar gsutil para descargar directamente
            import subprocess
            cmd = f"gsutil cp {GS_PATH} ./cdo_challenge.csv.gz"
            subprocess.run(cmd, shell=True, check=True)
            
            # Descomprimir
            cmd = "gunzip -f ./cdo_challenge.csv.gz"
            subprocess.run(cmd, shell=True, check=True)
            
            logger.info("Archivo descargado y descomprimido exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error en descarga: {e}")
            return False
    
    def clean_text_data(self, text_series):
        """Limpia caracteres corruptos y normaliza texto"""
        if text_series.dtype == 'object':
            # Corregir caracteres corruptos comunes
            text_series = text_series.astype(str).str.replace('Ã±', 'ñ')
            text_series = text_series.str.replace('Ã±', 'ñ')
            text_series = text_series.str.replace('STÃ±', 'STÑ')
            text_series = text_series.str.replace('UÃ±ER0Ã¢Ã¢', 'UÑER0')
            text_series = text_series.str.replace('Ã¢', '')
            text_series = text_series.str.replace('Ã', '')
            
            # Normalizar espacios y caracteres especiales
            text_series = text_series.str.strip()
            text_series = text_series.str.replace(r'\s+', ' ', regex=True)
            
        return text_series
    
    def clean_date_column(self, date_series):
        """Limpia y convierte columnas de fecha"""
        def parse_date(date_str):
            if pd.isna(date_str) or date_str == '':
                return pd.NaT
            
            # Convertir a string si no lo es
            date_str = str(date_str)
            
            # Patrones de fecha comunes
            patterns = [
                r'(\d{4})-(\d{1,2})-(\d{1,2})',  # YYYY-MM-DD
                r'(\d{1,2})/(\d{1,2})/(\d{4})',  # MM/DD/YYYY
                r'(\d{1,2})-(\d{1,2})-(\d{4})',  # DD-MM-YYYY
                r'(\d{8})',  # YYYYMMDD
            ]
            
            for pattern in patterns:
                match = re.match(pattern, date_str)
                if match:
                    if len(match.groups()) == 3:
                        if len(match.group(1)) == 4:  # YYYY-MM-DD
                            return pd.to_datetime(f"{match.group(1)}-{match.group(2).zfill(2)}-{match.group(3).zfill(2)}")
                        elif len(match.group(3)) == 4:  # MM/DD/YYYY o DD-MM-YYYY
                            return pd.to_datetime(f"{match.group(3)}-{match.group(1).zfill(2)}-{match.group(2).zfill(2)}")
                    elif len(match.group(1)) == 8:  # YYYYMMDD
                        return pd.to_datetime(match.group(1), format='%Y%m%d')
            
            # Si no coincide con ningún patrón, intentar parseo automático
            try:
                return pd.to_datetime(date_str)
            except:
                return pd.NaT
        
        return date_series.apply(parse_date)
    
    def process_chunk(self, chunk):
        """Procesa un chunk de datos"""
        try:
            # Limpiar columnas de texto
            text_columns = ['customer_id', 'customer_segment', 'product_id', 'product_category', 
                           'product_lifecycle', 'currency', 'region', 'warehouse', 'status', 
                           'payment_method', 'notes', 'created_by']
            
            for col in text_columns:
                if col in chunk.columns:
                    chunk[col] = self.clean_text_data(chunk[col])
            
            # Limpiar columnas de fecha
            date_columns = ['date', 'timestamp', 'modified_date']
            for col in date_columns:
                if col in chunk.columns:
                    chunk[col] = self.clean_date_column(chunk[col])
            
            # Convertir tipos de datos
            if 'quantity' in chunk.columns:
                chunk['quantity'] = pd.to_numeric(chunk['quantity'], errors='coerce')
            
            if 'unit_price' in chunk.columns:
                chunk['unit_price'] = pd.to_numeric(chunk['unit_price'], errors='coerce')
            
            if 'total_amount' in chunk.columns:
                chunk['total_amount'] = pd.to_numeric(chunk['total_amount'], errors='coerce')
            
            if 'discount_pct' in chunk.columns:
                chunk['discount_pct'] = pd.to_numeric(chunk['discount_pct'], errors='coerce')
            
            if 'tax_amount' in chunk.columns:
                chunk['tax_amount'] = pd.to_numeric(chunk['tax_amount'], errors='coerce')
            
            # Crear columna AnioMes
            if 'date' in chunk.columns:
                chunk['AnioMes'] = chunk['date'].dt.to_period('M').astype(str).str.replace('-', '').astype(int)
            
            # Crear columna quantity_null
            chunk['quantity_null'] = chunk['quantity'].isna().astype(int)
            
            # Crear columna unit_price_imput (se calculará después)
            chunk['unit_price_imput'] = chunk['unit_price']
            
            return chunk
            
        except Exception as e:
            logger.error(f"Error procesando chunk: {e}")
            return chunk
    
    def process_data_in_chunks(self):
        """Procesa los datos en chunks para evitar problemas de memoria"""
        logger.info("Iniciando procesamiento de datos en chunks...")
        
        # Leer archivo con Dask
        ddf = dd.read_csv('./cdo_challenge.csv', 
                         blocksize=DASK_CHUNKSIZE,
                         dtype={
                             'transaction_id': 'object',
                             'date': 'object',
                             'timestamp': 'object',
                             'customer_id': 'object',
                             'customer_segment': 'object',
                             'product_id': 'object',
                             'product_category': 'object',
                             'product_lifecycle': 'object',
                             'quantity': 'float64',
                             'unit_price': 'float64',
                             'total_amount': 'float64',
                             'currency': 'object',
                             'region': 'object',
                             'warehouse': 'object',
                             'status': 'object',
                             'payment_method': 'object',
                             'discount_pct': 'float64',
                             'tax_amount': 'float64',
                             'notes': 'object',
                             'created_by': 'object',
                             'modified_date': 'object'
                         })
        
        logger.info(f"Archivo leído con Dask. Particiones: {ddf.npartitions}")
        
        # Procesar cada partición
        processed_chunks = []
        total_partitions = ddf.npartitions
        
        for i in range(total_partitions):
            logger.info(f"Procesando partición {i+1}/{total_partitions}")
            
            # Obtener partición
            partition = ddf.get_partition(i).compute()
            
            # Procesar chunk
            processed_chunk = self.process_chunk(partition)
            processed_chunks.append(processed_chunk)
            
            self.processed_rows += len(processed_chunk)
            
            # Log de progreso
            if (i + 1) % 10 == 0:
                elapsed = time.time() - self.start_time
                logger.info(f"Procesadas {self.processed_rows:,} filas en {elapsed:.2f} segundos")
        
        # Combinar chunks procesados
        logger.info("Combinando chunks procesados...")
        final_df = pd.concat(processed_chunks, ignore_index=True)
        
        return final_df
    
    def calculate_unit_price_imput(self, df):
        """Calcula unit_price_imput basado en promedios por AnioMes, region y product_id"""
        logger.info("Calculando unit_price_imput...")
        
        # Crear máscara para valores nulos
        null_mask = df['unit_price'].isna()
        
        if not null_mask.any():
            logger.info("No hay valores nulos en unit_price")
            return df
        
        # Calcular promedios por AnioMes, region y product_id
        avg_by_product = df.groupby(['AnioMes', 'region', 'product_id'])['unit_price'].mean()
        
        # Para casos donde no hay datos por product_id, usar product_category
        avg_by_category = df.groupby(['AnioMes', 'region', 'product_category'])['unit_price'].mean()
        
        # Aplicar imputación
        for idx in df[null_mask].index:
            row = df.loc[idx]
            anio_mes = row['AnioMes']
            region = row['region']
            product_id = row['product_id']
            product_category = row['product_category']
            
            # Intentar con product_id primero
            key = (anio_mes, region, product_id)
            if key in avg_by_product and not pd.isna(avg_by_product[key]):
                df.loc[idx, 'unit_price_imput'] = avg_by_product[key]
            else:
                # Usar product_category como fallback
                key = (anio_mes, region, product_category)
                if key in avg_by_category and not pd.isna(avg_by_category[key]):
                    df.loc[idx, 'unit_price_imput'] = avg_by_category[key]
                else:
                    # Usar promedio general por AnioMes y region
                    general_avg = df[(df['AnioMes'] == anio_mes) & (df['region'] == region)]['unit_price'].mean()
                    if not pd.isna(general_avg):
                        df.loc[idx, 'unit_price_imput'] = general_avg
                    else:
                        # Último recurso: promedio general
                        df.loc[idx, 'unit_price_imput'] = df['unit_price'].mean()
        
        logger.info("unit_price_imput calculado exitosamente")
        return df
    
    def upload_to_bigquery(self, df, table_name):
        """Sube los datos procesados a BigQuery"""
        logger.info(f"Subiendo datos a BigQuery tabla: {table_name}")
        
        try:
            # Configurar job
            job_config = bigquery.LoadJobConfig(
                write_disposition=BQ_WRITE_DISPOSITION,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ]
            )
            
            # Crear tabla si no existe
            table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
            table = bigquery.Table(table_id)
            
            # Definir esquema
            schema = [
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
            
            table.schema = schema
            
            # Crear tabla
            try:
                self.client.create_table(table, exists_ok=True)
                logger.info(f"Tabla {table_name} creada/verificada")
            except Exception as e:
                logger.info(f"Tabla {table_name} ya existe o error: {e}")
            
            # Subir datos en lotes
            total_rows = len(df)
            batch_size = BATCH_SIZE
            
            for i in range(0, total_rows, batch_size):
                end_idx = min(i + batch_size, total_rows)
                batch_df = df.iloc[i:end_idx]
                
                logger.info(f"Subiendo lote {i//batch_size + 1}: filas {i+1}-{end_idx}")
                
                # Convertir a formato compatible con BigQuery
                batch_df['date'] = pd.to_datetime(batch_df['date']).dt.date
                batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'])
                batch_df['modified_date'] = pd.to_datetime(batch_df['modified_date']).dt.date
                
                # Subir lote
                job = self.client.load_table_from_dataframe(
                    batch_df, table_id, job_config=job_config
                )
                job.result()  # Esperar a que termine
                
                logger.info(f"Lote {i//batch_size + 1} subido exitosamente")
            
            logger.info(f"Datos subidos exitosamente a {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error subiendo a BigQuery: {e}")
            return False
    
    def run_full_pipeline(self):
        """Ejecuta el pipeline completo de procesamiento"""
        logger.info("Iniciando pipeline completo de procesamiento...")
        
        # Paso 1: Descargar y descomprimir
        if not self.download_and_decompress():
            return False
        
        # Paso 2: Procesar datos en chunks
        df = self.process_data_in_chunks()
        
        # Paso 3: Calcular unit_price_imput
        df = self.calculate_unit_price_imput(df)
        
        # Paso 4: Subir a tabla principal
        if not self.upload_to_bigquery(df, TABLE):
            return False
        
        # Paso 5: Subir a datamart
        if not self.upload_to_bigquery(df, DATAMART_TABLE):
            return False
        
        # Estadísticas finales
        elapsed = time.time() - self.start_time
        logger.info(f"Pipeline completado en {elapsed:.2f} segundos")
        logger.info(f"Total de filas procesadas: {self.processed_rows:,}")
        
        return True

def main():
    """Función principal"""
    processor = AceroDataProcessor()
    
    try:
        success = processor.run_full_pipeline()
        if success:
            logger.info("Pipeline ejecutado exitosamente")
        else:
            logger.error("Pipeline falló")
            
    except Exception as e:
        logger.error(f"Error en ejecución principal: {e}")

if __name__ == "__main__":
    main()
