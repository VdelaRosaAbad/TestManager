#!/bin/bash

# Script de instalación ligera para la solución alternativa
# TestManager - Carpeta Second (Sin descarga de archivos)

echo "🚀 Instalando dependencias ligeras para solución alternativa..."
echo "=========================================================="

# Verificar versión de Python
echo "🔍 Verificando versión de Python..."
python_version=$(python3 --version 2>&1 | grep -oE 'Python [0-9]+\.[0-9]+' | cut -d' ' -f2)
echo "Versión detectada: $python_version"

# Crear entorno virtual
echo "🏗️  Creando entorno virtual..."
python3 -m venv venv_light

# Activar entorno virtual
echo "🔧 Activando entorno virtual..."
source venv_light/bin/activate

# Actualizar pip
echo "⬆️  Actualizando pip..."
pip install --upgrade pip

# Instalar dependencias ligeras
echo "📥 Instalando dependencias ligeras..."

echo "   - Pandas..."
pip install "pandas==1.5.3"

echo "   - Numpy..."
pip install "numpy==1.24.3"

echo "   - Google Cloud..."
pip install "google-cloud-bigquery==3.11.4"
pip install "google-cloud-storage==2.8.0"
pip install "google-auth==2.17.3"

echo "   - Utilidades..."
pip install "python-dateutil==2.8.2"
pip install "pytz==2023.3"

echo "   - Visualización básica..."
pip install "matplotlib==3.7.1"
pip install "seaborn==0.12.2"

echo ""
echo "✅ Dependencias ligeras instaladas correctamente!"
echo ""
echo "🎯 Para activar el entorno virtual:"
echo "   source venv_light/bin/activate"
echo ""
echo "🚀 Para ejecutar la solución alternativa:"
echo "   # Opción 1: Procesamiento directo desde GCS"
echo "   python stream_processor.py"
echo ""
echo "   # Opción 2: BigQuery directo (recomendada)"
echo "   python bigquery_processor.py"
echo ""
echo "📊 Ventajas de esta solución:"
echo "   ✅ No requiere descargar archivos grandes"
echo "   ✅ Usa menos espacio en disco"
echo "   ✅ Procesamiento más rápido"
echo "   ✅ Menos dependencias"
echo ""
echo "⚠️  Nota: Esta solución requiere acceso directo a GCS"
echo "   desde BigQuery (tablas externas)"
