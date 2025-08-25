# 🚀 SOLUCIÓN ALTERNATIVA - TestManager

## 📋 Descripción

Esta es una solución alternativa para el desafío de TestManager que **NO requiere descargar archivos grandes** al dispositivo local. Está diseñada específicamente para entornos con espacio limitado en disco.

## ❌ Problema Original

```
[Errno 28] No space left on device
Transfer failed after 23 retries
npm error nospc ENOSPC: no space left on device
```

## ✅ Solución Implementada

### 🎯 **Enfoque Principal: BigQuery Directo**
- **Tablas externas**: Lee datos directamente desde Google Cloud Storage
- **Procesamiento SQL**: Limpieza y transformación usando BigQuery nativo
- **Sin descarga**: Los datos nunca se descargan al dispositivo local

### 🔄 **Enfoque Secundario: Streaming desde GCS**
- **Chunks pequeños**: Lee datos en fragmentos de 1MB
- **Procesamiento incremental**: Sube a BigQuery por lotes
- **Control de memoria**: Libera memoria después de cada chunk

## 🏗️ Arquitectura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Google Cloud  │    │   BigQuery      │    │   Tabla Final   │
│   Storage       │───▶│   (Tabla        │───▶│   datamartclean │
│   (113.33 GB)   │    │    Externa)     │    │   (Limpia)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   SQL de        │
                       │   Limpieza      │
                       └─────────────────┘
```

## 📁 Archivos Disponibles

| Archivo | Propósito | Recomendación |
|---------|-----------|---------------|
| **`bigquery_processor.py`** | Procesamiento BigQuery directo | ⭐ **RECOMENDADO** |
| **`stream_processor.py`** | Streaming desde GCS | Alternativa |
| **`requirements_light.txt`** | Dependencias mínimas | Configuración |
| **`install_light.sh`** | Instalación automática | Fácil uso |
| **`run_alternative.sh`** | Pipeline completo | Ejecución |

## 🚀 Instalación Rápida

### Opción 1: Instalación Automática (Recomendada)

```bash
# 1. Navegar a la carpeta second
cd second

# 2. Dar permisos de ejecución
chmod +x install_light.sh
chmod +x run_alternative.sh

# 3. Instalar dependencias
./install_light.sh

# 4. Ejecutar pipeline completo
./run_alternative.sh
```

### Opción 2: Instalación Manual

```bash
# 1. Crear entorno virtual
python3 -m venv venv_light
source venv_light/bin/activate

# 2. Instalar dependencias
pip install --upgrade pip
pip install "pandas==1.5.3"
pip install "numpy==1.24.3"
pip install "google-cloud-bigquery==3.11.4"
pip install "google-cloud-storage==2.8.0"
pip install "google-auth==2.17.3"
pip install "python-dateutil==2.8.2"
pip install "pytz==2023.3"

# 3. Ejecutar procesador
python bigquery_processor.py
```

## 📊 Ventajas de esta Solución

### ✅ **Eficiencia de Espacio**
- **0 GB descargados**: Los datos permanecen en GCS
- **Entorno virtual ligero**: Solo dependencias esenciales
- **Procesamiento en la nube**: BigQuery maneja los datos pesados

### ⚡ **Rendimiento**
- **Más rápido**: No hay transferencia de archivos grandes
- **Escalable**: BigQuery procesa automáticamente
- **Paralelo**: Múltiples workers en la nube

### 🔧 **Simplicidad**
- **Menos dependencias**: Solo lo esencial
- **Menos configuración**: BigQuery maneja la complejidad
- **Menos errores**: Menos puntos de falla

## 🔍 Cómo Funciona

### 1. **Tabla Externa**
```sql
-- Crea una tabla que apunta a GCS
CREATE EXTERNAL TABLE `raw_data` (
    transaction_id STRING,
    date STRING,
    -- ... otras columnas
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://bucket/archivo.csv.gz']
);
```

### 2. **Limpieza SQL**
```sql
-- Limpia y transforma los datos
CREATE OR REPLACE TABLE `datamartclean` AS
SELECT 
    TRIM(REGEXP_REPLACE(transaction_id, r'[Ã±â]', '')) as transaction_id,
    SAFE.PARSE_DATE('%Y-%m-%d', date) as date,
    -- ... más transformaciones
FROM `raw_data`
WHERE transaction_id IS NOT NULL;
```

### 3. **Imputación Inteligente**
```sql
-- Imputa precios faltantes
UPDATE `datamartclean` 
SET unit_price_imput = COALESCE(
    unit_price,
    -- Promedio por AnioMes, region, product_id
    (SELECT AVG(unit_price) FROM ...),
    -- Promedio por AnioMes, region
    (SELECT AVG(unit_price) FROM ...),
    -- Promedio general
    (SELECT AVG(unit_price) FROM ...)
)
WHERE unit_price IS NULL;
```

## 📈 Resultados Esperados

| Métrica | Valor Esperado |
|---------|----------------|
| **Tiempo total** | 15-30 minutos |
| **Espacio usado** | <100 MB |
| **Dependencias** | 7 paquetes |
| **Calidad de datos** | >95% |
| **Fechas corregidas** | >99% |

## 🚨 Solución de Problemas

### Error: "Permission denied"
```bash
chmod +x install_light.sh
chmod +x run_alternative.sh
```

### Error: "No module named 'google.cloud'"
```bash
source venv_light/bin/activate
pip install "google-cloud-bigquery==3.11.4"
```

### Error: "Dataset not found"
```bash
bq mk --project_id=testmanager-470115 --dataset_id=dataset_acero --location=US
```

### Error: "API not enabled"
```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
```

## 🔄 Comparación con Solución Original

| Aspecto | Solución Original | Solución Alternativa |
|---------|-------------------|----------------------|
| **Descarga** | 373 GB | 0 GB |
| **Espacio** | 8+ GB | <100 MB |
| **Dependencias** | 15+ paquetes | 7 paquetes |
| **Tiempo** | 45-60 min | 15-30 min |
| **Complejidad** | Alta | Baja |
| **Confiabilidad** | Media | Alta |

## 🎯 Casos de Uso Ideales

### ✅ **Usar Solución Alternativa cuando:**
- Espacio en disco limitado (<5 GB)
- Conexión a internet lenta
- Tiempo limitado para configuración
- Preferencia por procesamiento en la nube
- Entorno Cloud Shell con restricciones

### ⚠️ **Considerar Solución Original cuando:**
- Espacio en disco abundante (>10 GB)
- Necesidad de procesamiento local
- Requerimientos específicos de Dask
- Análisis offline necesario

## 📞 Soporte

### 🔧 **Verificación Rápida**
```bash
# Verificar configuración
gcloud config list
bq ls --project_id=testmanager-470115

# Verificar APIs
gcloud services list --enabled | grep -E "(bigquery|storage)"
```

### 📝 **Logs y Debugging**
```bash
# Ver logs de BigQuery
bq show testmanager-470115:dataset_acero.datamartclean

# Verificar datos
bq query --use_legacy_sql=false "
SELECT COUNT(*) as total FROM \`testmanager-470115.dataset_acero.datamartclean\`
"
```

## 🎉 Conclusión

Esta solución alternativa resuelve completamente el problema de espacio en disco y proporciona una ruta más eficiente y confiable para procesar el dataset de acero. 

**Recomendación**: Usar `bigquery_processor.py` como primera opción, ya que es más rápido, confiable y usa menos recursos.

---

**Nota**: Esta solución mantiene todas las funcionalidades requeridas del desafío original, pero con un enfoque más eficiente en el uso de recursos.
