#!/bin/bash

# Script para ejecutar la solución alternativa
# TestManager - Carpeta Second (Sin descarga de archivos)

set -e

echo "🚀 Ejecutando solución alternativa de TestManager..."
echo "=================================================="

# Función para mostrar mensajes
info_msg() {
    echo "ℹ️  $1"
}

success_msg() {
    echo "✅ $1"
}

error_exit() {
    echo "❌ ERROR: $1" >&2
    exit 1
}

# Verificar que estamos en Cloud Shell
if [[ -z "$CLOUDSHELL_ENVIRONMENT" ]]; then
    info_msg "No estás en Cloud Shell. Algunas funciones pueden no estar disponibles."
fi

# 1. Configurar proyecto de Google Cloud
info_msg "Configurando proyecto de Google Cloud..."
gcloud config set project testmanager-470115 || error_exit "No se pudo configurar el proyecto"

# 2. Habilitar APIs necesarias
info_msg "Habilitando APIs de Google Cloud..."
gcloud services enable bigquery.googleapis.com || error_exit "Error habilitando BigQuery API"
gcloud services enable storage.googleapis.com || error_exit "Error habilitando Storage API"

# 3. Crear dataset si no existe
info_msg "Creando dataset de BigQuery..."
bq mk --project_id=testmanager-470115 --dataset_id=dataset_acero --location=US 2>/dev/null || info_msg "Dataset ya existe"

# 4. Verificar versión de Python
info_msg "Verificando versión de Python..."
python_version=$(python3 --version 2>&1 | grep -oE 'Python [0-9]+\.[0-9]+' | cut -d' ' -f2)
echo "Versión de Python detectada: $python_version"

# 5. Crear y activar entorno virtual
info_msg "Creando entorno virtual ligero..."
python3 -m venv venv_light || error_exit "Error creando entorno virtual"
source venv_light/bin/activate || error_exit "Error activando entorno virtual"

success_msg "Entorno virtual activado: $VIRTUAL_ENV"

# 6. Instalar dependencias ligeras
info_msg "Instalando dependencias ligeras..."
pip install --upgrade pip || error_exit "Error actualizando pip"

# Instalar solo lo esencial
pip install "pandas==1.5.3" || error_exit "Error instalando pandas"
pip install "numpy==1.24.3" || error_exit "Error instalando numpy"
pip install "google-cloud-bigquery==3.11.4" || error_exit "Error instalando bigquery"
pip install "google-cloud-storage==2.8.0" || error_exit "Error instalando storage"
pip install "google-auth==2.17.3" || error_exit "Error instalando auth"
pip install "python-dateutil==2.8.2" || error_exit "Error instalando dateutil"
pip install "pytz==2023.3" || error_exit "Error instalando pytz"

success_msg "Dependencias ligeras instaladas correctamente"

# 7. Ejecutar solución BigQuery (recomendada)
info_msg "Ejecutando solución BigQuery directa (recomendada)..."
python bigquery_processor.py || error_exit "Error en procesador BigQuery"

success_msg "Procesamiento BigQuery completado"

# 8. Verificar resultados
info_msg "Verificando resultados..."
echo "📊 Información de la tabla final:"
bq show testmanager-470115:dataset_acero.datamartclean || error_exit "Error mostrando información de la tabla"

echo "📈 Consulta de resumen:"
bq query --use_legacy_sql=false "
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT customer_id) as clientes_unicos,
    COUNT(DISTINCT product_id) as productos_unicos,
    COUNT(DISTINCT region) as regiones,
    MIN(date) as fecha_inicio,
    MAX(date) as fecha_fin,
    AVG(total_amount) as monto_promedio,
    SUM(total_amount) as monto_total
FROM \`testmanager-470115.dataset_acero.datamartclean\`
" || error_exit "Error ejecutando consulta de resumen"

# 9. Mostrar información adicional
info_msg "Solución alternativa completada exitosamente"
echo ""
echo "🎯 Archivos disponibles en esta carpeta:"
echo "   - bigquery_processor.py: Procesador BigQuery directo (RECOMENDADO)"
echo "   - stream_processor.py: Procesador streaming desde GCS"
echo "   - requirements_light.txt: Dependencias mínimas"
echo ""
echo "📊 La tabla 'datamartclean' está lista en BigQuery"
echo "🔗 Puedes consultarla en BigQuery Console"
echo ""
echo "💡 Ventajas de esta solución:"
echo "   ✅ No requiere descargar archivos grandes"
echo "   ✅ Usa menos espacio en disco"
echo "   ✅ Procesamiento más rápido"
echo "   ✅ Menos dependencias"
echo ""
echo "🎉 ¡Solución alternativa completada exitosamente!"
