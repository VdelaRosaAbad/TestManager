# 📋 Pasos Técnicos - Pipeline de Procesamiento de Datos de Acero

## 🎯 Objetivo
Procesar un archivo de datos comerciales de 113.33 GB comprimido (~373 GB descomprimido) con 2,003,599,864 registros, limpiarlo y cargarlo a BigQuery para análisis EDA.

## 🏗️ Arquitectura de la Solución

### Componentes Principales:
1. **Dask** - Procesamiento paralelo de datos grandes
2. **Dataform** - Limpieza y transformación de datos
3. **BigQuery** - Almacenamiento y consulta de datos
4. **Streamlit** - Análisis EDA interactivo
5. **Cloud Shell** - Ejecución del pipeline

## 📁 Estructura del Proyecto

```
TestManager/
├── config.py                 # Configuración del proyecto
├── data_processor.py         # Script principal de procesamiento
├── dataform.yaml            # Configuración de Dataform
├── definitions/
│   └── clean_data.sql       # Transformaciones SQL de Dataform
├── eda_streamlit.py         # Aplicación Streamlit para EDA
├── requirements.txt          # Dependencias de Python
├── run_pipeline.sh          # Script de ejecución en Cloud Shell
└── PASOS_TECNICOS.md        # Este documento
```

## 🚀 Pasos Técnicos Detallados

### Paso 1: Preparación del Entorno
**Archivo:** `run_pipeline.sh` (líneas 1-50)

```bash
# Configurar proyecto de Google Cloud
gcloud config set project testmanager-470115

# Habilitar APIs necesarias
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable dataflow.googleapis.com

# Crear bucket y dataset si no existen
gsutil mb -p testmanager-470115 -c STANDARD -l US gs://bucket_acero
bq mk --project_id=testmanager-470115 --location=US dataset_acero
```

**¿Por qué?** Necesitamos habilitar las APIs de Google Cloud y crear la infraestructura básica antes de procesar datos.

### Paso 2: Descarga y Descompresión
**Archivo:** `data_processor.py` (líneas 60-80)

```python
def download_and_decompress(self):
    # Usar gsutil para descargar directamente
    cmd = f"gsutil cp {GS_PATH} ./cdo_challenge.csv.gz"
    subprocess.run(cmd, shell=True, check=True)
    
    # Descomprimir
    cmd = "gunzip -f ./cdo_challenge.csv.gz"
    subprocess.run(cmd, shell=True, check=True)
```

**¿Por qué?** El archivo está en Google Cloud Storage, por lo que usamos `gsutil` para descargarlo eficientemente y `gunzip` para descomprimirlo.

### Paso 3: Procesamiento con Dask
**Archivo:** `data_processor.py` (líneas 200-250)

```python
def process_data_in_chunks(self):
    # Leer archivo con Dask
    ddf = dd.read_csv('./cdo_challenge.csv', 
                      blocksize=DASK_CHUNKSIZE,  # 100MB por partición
                      dtype={
                          'transaction_id': 'object',
                          'date': 'object',
                          # ... otros tipos de datos
                      })
    
    # Procesar cada partición por separado
    for i in range(ddf.npartitions):
        partition = ddf.get_partition(i).compute()
        processed_chunk = self.process_chunk(partition)
        processed_chunks.append(processed_chunk)
```

**¿Por qué Dask?**
- **Memoria eficiente**: Divide archivos grandes en particiones manejables
- **Procesamiento paralelo**: Aprovecha múltiples núcleos de CPU
- **Escalabilidad**: Puede manejar archivos de cualquier tamaño
- **Compatibilidad**: API similar a pandas pero para big data

### Paso 4: Limpieza de Datos
**Archivo:** `data_processor.py` (líneas 100-150)

#### 4.1 Limpieza de Texto
```python
def clean_text_data(self, text_series):
    # Corregir caracteres corruptos comunes
    text_series = text_series.astype(str).str.replace('Ã±', 'ñ')
    text_series = text_series.str.replace('STÃ±', 'STÑ')
    text_series = text_series.str.replace('UÃ±ER0Ã¢Ã¢', 'UÑER0')
    
    # Normalizar espacios
    text_series = text_series.str.strip()
    text_series = text_series.str.replace(r'\s+', ' ', regex=True)
```

**¿Por qué?** Los datos contienen caracteres corruptos como `Ã±Ã±STÃ±` que necesitan ser corregidos para legibilidad.

#### 4.2 Limpieza de Fechas
```python
def clean_date_column(self, date_series):
    def parse_date(date_str):
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
                # Convertir según el patrón detectado
                return pd.to_datetime(...)
```

**¿Por qué?** Las fechas vienen en múltiples formatos y necesitan ser estandarizadas para análisis temporal.

#### 4.3 Conversión de Tipos
```python
# Convertir tipos de datos
if 'quantity' in chunk.columns:
    chunk['quantity'] = pd.to_numeric(chunk['quantity'], errors='coerce')

if 'unit_price' in chunk.columns:
    chunk['unit_price'] = pd.to_numeric(chunk['unit_price'], errors='coerce')
```

**¿Por qué?** Los datos numéricos pueden venir como strings y necesitan ser convertidos para cálculos matemáticos.

### Paso 5: Creación de Columnas Derivadas
**Archivo:** `data_processor.py` (líneas 180-190)

#### 5.1 Columna AnioMes
```python
# Crear columna AnioMes
if 'date' in chunk.columns:
    chunk['AnioMes'] = chunk['date'].dt.to_period('M').astype(str).str.replace('-', '').astype(int)
```

**¿Por qué?** Necesitamos agrupar datos por año-mes para análisis temporal y la imputación de precios.

#### 5.2 Columna quantity_null
```python
# Crear columna quantity_null
chunk['quantity_null'] = chunk['quantity'].isna().astype(int)
```

**¿Por qué?** Necesitamos identificar registros con cantidades nulas para análisis de calidad de datos.

### Paso 6: Imputación de Precios
**Archivo:** `data_processor.py` (líneas 300-350)

```python
def calculate_unit_price_imput(self, df):
    # Calcular promedios por AnioMes, region y product_id
    avg_by_product = df.groupby(['AnioMes', 'region', 'product_id'])['unit_price'].mean()
    
    # Para casos donde no hay datos por product_id, usar product_category
    avg_by_category = df.groupby(['AnioMes', 'region', 'product_category'])['unit_price'].mean()
    
    # Aplicar imputación jerárquica
    for idx in df[null_mask].index:
        # 1. Intentar con product_id
        # 2. Fallback a product_category
        # 3. Fallback a región y año-mes
        # 4. Último recurso: promedio general
```

**¿Por qué?** Los precios nulos necesitan ser imputados para análisis financiero completo, usando la jerarquía más específica disponible.

### Paso 7: Carga a BigQuery
**Archivo:** `data_processor.py` (líneas 400-500)

```python
def upload_to_bigquery(self, df, table_name):
    # Configurar job
    job_config = bigquery.LoadJobConfig(
        write_disposition=BQ_WRITE_DISPOSITION,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    )
    
    # Subir datos en lotes
    for i in range(0, total_rows, batch_size):
        batch_df = df.iloc[i:end_idx]
        
        job = self.client.load_table_from_dataframe(
            batch_df, table_id, job_config=job_config
        )
        job.result()  # Esperar a que termine
```

**¿Por qué?** BigQuery tiene límites de tamaño de carga, por lo que dividimos los datos en lotes manejables.

### Paso 8: Limpieza Adicional con Dataform
**Archivo:** `definitions/clean_data.sql`

```sql
WITH cleaned_data AS (
  SELECT
    -- Limpiar transaction_id
    COALESCE(TRIM(transaction_id), 'UNKNOWN') as transaction_id,
    
    -- Limpiar y validar fechas con múltiples formatos
    CASE 
      WHEN SAFE.PARSE_DATE('%Y-%m-%d', date) IS NOT NULL THEN SAFE.PARSE_DATE('%Y-%m-%d', date)
      WHEN SAFE.PARSE_DATE('%m/%d/%Y', date) IS NOT NULL THEN SAFE.PARSE_DATE('%m/%d/%Y', date)
      -- ... más formatos
    END as date,
    
    -- Normalizar segmentos de cliente (priorizar español)
    CASE 
      WHEN LOWER(customer_segment) IN ('empresarial', 'empresa', 'business', 'corporate') THEN 'Empresarial'
      WHEN LOWER(customer_segment) IN ('individual', 'persona', 'personal', 'retail') THEN 'Individual'
      -- ... más mapeos
    END as customer_segment
```

**¿Por qué Dataform?**
- **Consistencia**: Asegura que las transformaciones se apliquen uniformemente
- **Versionado**: Control de versiones de las transformaciones
- **Testing**: Permite validar la calidad de los datos
- **Documentación**: Auto-documenta las transformaciones

### Paso 9: Análisis EDA con Streamlit
**Archivo:** `eda_streamlit.py`

```python
@st.cache_data(ttl=3600)
def load_data_from_bigquery():
    # Query para obtener muestra representativa
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    ORDER BY RAND()
    LIMIT 100000
    """
    
    df = client.query(query).to_dataframe()
    return df
```

**¿Por qué Streamlit?**
- **Interactividad**: Permite explorar datos de manera dinámica
- **Visualización**: Gráficos interactivos con Plotly
- **Performance**: Cache de datos para consultas repetidas
- **Facilidad**: API simple para crear dashboards

## ⚡ Optimizaciones de Rendimiento

### 1. Procesamiento en Chunks
- **Tamaño de chunk**: 100MB para balancear memoria y velocidad
- **Paralelización**: Procesamiento simultáneo de múltiples chunks
- **Memoria**: Uso controlado de RAM para evitar crashes

### 2. Carga a BigQuery
- **Batch size**: 1M registros por lote
- **Paralelización**: Múltiples jobs simultáneos
- **Schema**: Definición explícita para evitar inferencia automática

### 3. Cache y Optimización
- **Streamlit cache**: TTL de 1 hora para datos
- **Dask partitions**: Optimización automática del tamaño de particiones
- **BigQuery**: Uso de clustering y particionamiento

## 🔍 Monitoreo y Logging

### Logging Estructurado
```python
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

### Métricas de Progreso
```python
if (i + 1) % 10 == 0:
    elapsed = time.time() - self.start_time
    logger.info(f"Procesadas {self.processed_rows:,} filas en {elapsed:.2f} segundos")
```

### Verificación de Resultados
```bash
# Verificar estadísticas finales
bq query --project_id=$PROJECT_ID --use_legacy_sql=false "
SELECT COUNT(*) as total_rows, AVG(unit_price) as avg_price
FROM \`$PROJECT_ID.$DATASET.$DATAMART_TABLE\`
"
```

## 🚨 Manejo de Errores

### 1. Errores de Descarga
```python
try:
    subprocess.run(cmd, shell=True, check=True)
except Exception as e:
    logger.error(f"Error en descarga: {e}")
    return False
```

### 2. Errores de Procesamiento
```python
try:
    processed_chunk = self.process_chunk(partition)
except Exception as e:
    logger.error(f"Error procesando chunk: {e}")
    return chunk  # Retornar chunk original en caso de error
```

### 3. Errores de BigQuery
```python
try:
    job.result()  # Esperar a que termine
except Exception as e:
    logger.error(f"Error en job de BigQuery: {e}")
    # Reintentar o continuar con siguiente lote
```

## 📊 Métricas de Rendimiento Esperadas

### Tiempo de Procesamiento
- **Archivo completo**: 45-60 minutos
- **Por chunk**: 2-5 minutos
- **Carga a BigQuery**: 10-15 minutos

### Uso de Recursos
- **Memoria**: 8GB máximo
- **CPU**: 4 workers paralelos
- **Storage**: 400GB temporal (archivo + procesamiento)

### Calidad de Datos
- **Datos válidos**: >95%
- **Fechas corregidas**: >99%
- **Precios imputados**: >98%

## 🔧 Comandos de Ejecución

### 1. Ejecutar Pipeline Completo
```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

### 2. Solo Procesamiento de Datos
```bash
python data_processor.py
```

### 3. Solo Limpieza con Dataform
```bash
dataform run --project-id=testmanager-470115 --location=US
```

### 4. Ejecutar Análisis EDA
```bash
streamlit run eda_streamlit.py
```

## 📈 Monitoreo en Tiempo Real

### 1. Logs de Progreso
```bash
tail -f /var/log/syslog | grep "data_processor"
```

### 2. Uso de Recursos
```bash
htop  # Monitorear CPU y memoria
df -h  # Monitorear espacio en disco
```

### 3. Estado de BigQuery
```bash
bq ls --project_id=testmanager-470115 dataset_acero
```

## 🎯 Resultados Esperados

### 1. Tablas Creadas
- `acero_table`: Datos procesados básicos
- `datamartclean`: Datos limpios y transformados

### 2. Calidad de Datos
- Caracteres corruptos corregidos
- Fechas estandarizadas
- Precios imputados
- Categorías normalizadas

### 3. Análisis EDA
- Dashboard interactivo completo
- Visualizaciones de tendencias
- Métricas de negocio clave
- Análisis de rentabilidad

## 🔄 Mantenimiento y Actualizaciones

### 1. Actualización de Datos
```bash
# Ejecutar pipeline completo nuevamente
./run_pipeline.sh
```

### 2. Modificación de Transformaciones
```sql
-- Editar definitions/clean_data.sql
-- Ejecutar: dataform run --project-id=testmanager-470115 --location=US
```

### 3. Actualización de Dependencias
```bash
pip install -r requirements.txt --upgrade
```

## 📚 Recursos Adicionales

- [Documentación de Dask](https://docs.dask.org/)
- [Dataform Documentation](https://docs.dataform.co/)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [Streamlit Documentation](https://docs.streamlit.io/)

---

**Nota**: Este pipeline está diseñado para ser robusto y eficiente, manejando archivos de datos muy grandes mientras mantiene la calidad y proporciona insights valiosos para la toma de decisiones empresariales.
