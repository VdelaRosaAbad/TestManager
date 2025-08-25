#!/bin/bash

# Script para ejecutar el pipeline completo de procesamiento de datos de acero
# Ejecutar en Cloud Shell

set -e

echo "🚀 Iniciando Pipeline de Procesamiento de Datos de Acero"
echo "=================================================="

# Configuración del proyecto
PROJECT_ID="testmanager-470115"
BUCKET="bucket_acero"
DATASET="dataset_acero"
TABLE="acero_table"
DATAMART_TABLE="datamartclean"

echo "📋 Configuración del Proyecto:"
echo "   Project ID: $PROJECT_ID"
echo "   Bucket: $BUCKET"
echo "   Dataset: $DATASET"
echo "   Tabla Principal: $TABLE"
echo "   Datamart: $DATAMART_TABLE"
echo ""

# Verificar que estamos en el proyecto correcto
echo "🔍 Verificando configuración del proyecto..."
gcloud config set project $PROJECT_ID

# Verificar que las APIs necesarias estén habilitadas
echo "🔧 Habilitando APIs necesarias..."
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable dataflow.googleapis.com

# Crear bucket si no existe
echo "🪣 Verificando bucket de almacenamiento..."
if ! gsutil ls -b gs://$BUCKET >/dev/null 2>&1; then
    echo "   Creando bucket $BUCKET..."
    gsutil mb -p $PROJECT_ID -c STANDARD -l US gs://$BUCKET
else
    echo "   Bucket $BUCKET ya existe"
fi

# Crear dataset de BigQuery si no existe
echo "🗄️ Verificando dataset de BigQuery..."
if ! bq show --project_id=$PROJECT_ID $DATASET >/dev/null 2>&1; then
    echo "   Creando dataset $DATASET..."
    bq mk --project_id=$PROJECT_ID --location=US $DATASET
else
    echo "   Dataset $DATASET ya existe"
fi

# Instalar dependencias de Python
echo "📦 Instalando dependencias de Python..."
pip install --upgrade pip
pip install -r requirements.txt

# Verificar instalación de Dask
echo "🔍 Verificando instalación de Dask..."
python -c "import dask; print(f'Dask version: {dask.__version__}')"

# Ejecutar pipeline de procesamiento
echo "⚙️ Ejecutando pipeline de procesamiento..."
echo "   Este proceso puede tomar hasta 1 hora para archivos grandes..."
echo "   Iniciando: $(date)"

# Ejecutar el procesador de datos
python data_processor.py

echo "✅ Pipeline de procesamiento completado: $(date)"
echo ""

# Ejecutar Dataform para limpieza adicional
echo "🧹 Ejecutando limpieza con Dataform..."
echo "   Instalando Dataform..."

# Instalar Dataform CLI
npm install -g @dataform/cli

# Ejecutar Dataform
echo "   Ejecutando transformaciones de Dataform..."
dataform run --project-id=$PROJECT_ID --location=US

echo "✅ Limpieza con Dataform completada"
echo ""

# Verificar resultados
echo "🔍 Verificando resultados..."
echo "   Tabla principal: $TABLE"
bq show --project_id=$PROJECT_ID $DATASET.$TABLE | head -20

echo ""
echo "   Datamart limpio: $DATAMART_TABLE"
bq show --project_id=$PROJECT_ID $DATASET.$DATAMART_TABLE | head -20

echo ""
echo "📊 Estadísticas de la tabla final:"
bq query --project_id=$PROJECT_ID --use_legacy_sql=false "
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT product_id) as unique_products,
    COUNT(DISTINCT region) as unique_regions,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    AVG(quantity) as avg_quantity,
    AVG(unit_price) as avg_unit_price,
    AVG(total_amount) as avg_total_amount,
    SUM(total_amount) as total_revenue
FROM \`$PROJECT_ID.$DATASET.$DATAMART_TABLE\`
"

echo ""
echo "🎉 Pipeline completado exitosamente!"
echo "=================================================="
echo ""
echo "📋 Próximos pasos:"
echo "   1. Ejecutar análisis EDA: streamlit run eda_streamlit.py"
echo "   2. Revisar datos en BigQuery Console"
echo "   3. Analizar métricas de rendimiento"
echo ""
echo "🔗 Enlaces útiles:"
echo "   - BigQuery Console: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
echo "   - Cloud Storage: https://console.cloud.google.com/storage/browser?project=$PROJECT_ID"
echo "   - Dataform: https://dataform.co/"
echo ""
echo "📅 Completado en: $(date)"
