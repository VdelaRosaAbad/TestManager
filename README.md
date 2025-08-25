# 🏭 TestManager - Pipeline de Procesamiento de Datos de Acero

## 📋 Descripción del Proyecto

Este proyecto implementa una solución completa para procesar, limpiar y analizar datos comerciales de una empresa de aceros largos. El objetivo es transformar un dataset de 2 mil millones de registros (~373 GB) en insights accionables para la toma de decisiones empresariales.

## 🎯 Objetivos del Desafío

- **Análisis EDA**: Exploración exhaustiva de datos comerciales de 5 años
- **Limpieza de Datos**: Corrección de caracteres corruptos, fechas y normalización
- **Estrategia de Rentabilidad**: Propuesta basada en análisis de datos para los próximos 12 meses
- **Plan Green Field**: Implementación desde cero sin capacidades de datos preexistentes

## 🏗️ Arquitectura de la Solución

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Google Cloud  │    │   Dask +        │    │   BigQuery +    │
│   Storage       │───▶│   Python        │───▶│   Dataform      │
│   (113.33 GB)   │    │   (Procesamiento)│   │   (Limpieza)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   BigQuery      │    │   Streamlit     │
                       │   (Almacenamiento)│  │   (Análisis EDA)│
                       └─────────────────┘    └─────────────────┘
```

## 🚀 Características Principales

### ✨ Procesamiento Eficiente
- **Dask**: Procesamiento paralelo de archivos grandes
- **Chunking**: División en lotes manejables (100MB)
- **Memoria optimizada**: Uso controlado de RAM (8GB máximo)

### 🧹 Limpieza Avanzada
- **Corrección de caracteres**: `Ã±Ã±STÃ±` → `STÑ`
- **Normalización de fechas**: Múltiples formatos → Estándar ISO
- **Priorización del español**: Categorías en español por defecto
- **Imputación inteligente**: Precios basados en contexto temporal y geográfico

### 📊 Análisis EDA Completo
- **Dashboard interactivo**: 5 tipos de análisis diferentes
- **Visualizaciones**: Gráficos interactivos con Plotly
- **Métricas de negocio**: KPIs clave para toma de decisiones
- **Cache inteligente**: Optimización de consultas repetidas

## 📁 Estructura del Proyecto

```
TestManager/
├── 📄 config.py                 # Configuración centralizada
├── 🐍 data_processor.py         # Procesador principal con Dask
├── ⚙️ dataform.yaml            # Configuración de Dataform
├── 📁 definitions/
│   └── 🧹 clean_data.sql       # Transformaciones SQL
├── 📊 eda_streamlit.py         # Dashboard EDA interactivo
├── 📦 requirements.txt          # Dependencias de Python
├── 🚀 run_pipeline.sh          # Script de ejecución automática
├── 📋 PASOS_TECNICOS.md        # Documentación técnica detallada
└── 📖 README.md                 # Este archivo
```

## 🛠️ Tecnologías Utilizadas

| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| **Dask** | 2023.11.0 | Procesamiento paralelo de big data |
| **Python** | 3.8+ | Lenguaje principal |
| **Dataform** | 1.0.0 | Transformaciones y limpieza de datos |
| **BigQuery** | - | Almacenamiento y consulta de datos |
| **Streamlit** | 1.28.2 | Dashboard interactivo |
| **Plotly** | 5.17.0 | Visualizaciones avanzadas |
| **Google Cloud** | - | Infraestructura en la nube |

## 🚀 Instalación y Configuración

### Prerrequisitos

- **Google Cloud Project** con APIs habilitadas
- **Cloud Shell** o entorno con acceso a gcloud
- **Python 3.8+** con pip
- **Node.js** para Dataform CLI

### 1. Clonar el Repositorio

```bash
git clone https://github.com/VdelaRosaAbad/TestManager.git
cd TestManager
```

### 2. Configurar Google Cloud

```bash
# Configurar proyecto
gcloud config set project testmanager-470115

# Habilitar APIs necesarias
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable dataflow.googleapis.com
```

### 3. Instalar Dependencias

```bash
# Python
pip install -r requirements.txt

# Dataform CLI
npm install -g @dataform/cli
```

### 4. Ejecutar Pipeline Completo

```bash
# Dar permisos de ejecución
chmod +x run_pipeline.sh

# Ejecutar pipeline completo
./run_pipeline.sh
```

## 📊 Uso del Sistema

### 🚀 Ejecución del Pipeline

```bash
# Pipeline completo (recomendado)
./run_pipeline.sh

# Solo procesamiento de datos
python data_processor.py

# Solo limpieza con Dataform
dataform run --project-id=testmanager-470115 --location=US
```

### 📈 Análisis EDA

```bash
# Ejecutar dashboard interactivo
streamlit run eda_streamlit.py
```

### 🔍 Verificación de Resultados

```bash
# Verificar tablas en BigQuery
bq ls --project_id=testmanager-470115 dataset_acero

# Consultar estadísticas
bq query --project_id=testmanager-470115 --use_legacy_sql=false "
SELECT COUNT(*) as total_rows, AVG(unit_price) as avg_price
FROM \`testmanager-470115.dataset_acero.datamartclean\`
"
```

## 📋 Columnas del Dataset Final

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `transaction_id` | STRING | Identificador único de transacción |
| `date` | DATE | Fecha de la transacción (limpia) |
| `timestamp` | DATETIME | Marca de tiempo exacta |
| `customer_id` | STRING | ID del cliente (limpio) |
| `customer_segment` | STRING | Segmento del cliente (español) |
| `product_id` | STRING | ID del producto (limpio) |
| `product_category` | STRING | Categoría del producto (español) |
| `product_lifecycle` | STRING | Estado del producto (español) |
| `quantity` | INTEGER | Cantidad vendida |
| `unit_price` | FLOAT64 | Precio unitario original |
| `total_amount` | FLOAT64 | Monto total de la transacción |
| `currency` | STRING | Moneda (MXN por defecto) |
| `region` | STRING | Región de venta (español) |
| `warehouse` | STRING | Almacén de origen |
| `status` | STRING | Estado de la transacción (español) |
| `payment_method` | STRING | Método de pago (español) |
| `discount_pct` | FLOAT64 | Porcentaje de descuento |
| `tax_amount` | FLOAT64 | Monto de impuestos |
| `notes` | STRING | Notas adicionales |
| `created_by` | STRING | Usuario que creó el registro |
| `modified_date` | DATE | Fecha de última modificación |
| `AnioMes` | INTEGER | Año-mes en formato YYYYMM |
| `quantity_null` | INTEGER | Indicador de cantidad nula (0/1) |
| `unit_price_imput` | FLOAT64 | Precio imputado para valores nulos |

## ⚡ Optimizaciones de Rendimiento

### 🚀 Procesamiento con Dask
- **Particionamiento automático**: 100MB por partición
- **Procesamiento paralelo**: 4 workers simultáneos
- **Gestión de memoria**: Control automático de RAM

### 📊 BigQuery
- **Carga en lotes**: 1M registros por lote
- **Schema explícito**: Evita inferencia automática
- **Clustering**: Optimización de consultas por región y fecha

### 🎯 Streamlit
- **Cache inteligente**: TTL de 1 hora para datos
- **Muestreo representativo**: 100K registros para análisis
- **Lazy loading**: Carga de datos bajo demanda

## 📈 Métricas de Rendimiento

| Métrica | Valor Esperado |
|---------|----------------|
| **Tiempo total** | 45-60 minutos |
| **Velocidad de procesamiento** | ~40M registros/minuto |
| **Uso de memoria** | <8GB |
| **Calidad de datos** | >95% |
| **Fechas corregidas** | >99% |
| **Precios imputados** | >98% |

## 🔍 Análisis EDA Disponibles

### 📊 Resumen General
- Métricas clave del negocio
- Calidad de datos
- Distribución por regiones

### 📅 Análisis Temporal
- Evolución de ingresos
- Tendencias de transacciones
- Estacionalidad del negocio

### 📦 Análisis de Productos
- Rentabilidad por categoría
- Top productos
- Ciclo de vida de productos

### 👥 Análisis de Clientes
- Segmentación de clientes
- Top clientes por ingresos
- Métodos de pago preferidos

### 💰 Análisis Financiero
- Análisis de precios
- Estrategias de descuento
- Rentabilidad por región

## 🚨 Manejo de Errores

### 🔄 Reintentos Automáticos
- **Descarga**: Reintento en caso de fallo de red
- **Procesamiento**: Continuación con chunks siguientes
- **BigQuery**: Reintento de lotes fallidos

### 📝 Logging Detallado
- **Nivel INFO**: Progreso del pipeline
- **Nivel ERROR**: Errores y excepciones
- **Métricas**: Tiempo y registros procesados

### 🛡️ Validación de Datos
- **Integridad**: Verificación de esquemas
- **Calidad**: Detección de anomalías
- **Consistencia**: Validación de relaciones

## 🔧 Configuración Avanzada

### ⚙️ Variables de Entorno

```bash
# Configuración del proyecto
export PROJECT_ID="testmanager-470115"
export BUCKET="bucket_acero"
export DATASET="dataset_acero"

# Configuración de Dask
export DASK_CHUNKSIZE="100MB"
export DASK_MEMORY_LIMIT="8GB"
```

### 🔧 Personalización de Chunks

```python
# En config.py
BATCH_SIZE = 1000000  # 1M registros por lote
MAX_WORKERS = 4       # Número de workers paralelos
```

## 📚 Documentación Adicional

- **[Pasos Técnicos Detallados](PASOS_TECNICOS.md)**: Explicación paso a paso del pipeline
- **[Configuración de Dataform](dataform.yaml)**: Parámetros de transformación
- **[Transformaciones SQL](definitions/clean_data.sql)**: Lógica de limpieza

## 🤝 Contribución

### 🐛 Reportar Bugs
1. Crear issue en GitHub
2. Incluir logs de error
3. Describir pasos para reproducir

### 💡 Sugerencias
1. Crear issue con etiqueta "enhancement"
2. Describir la funcionalidad deseada
3. Proporcionar casos de uso

### 🔧 Desarrollo Local
1. Fork del repositorio
2. Crear rama feature
3. Implementar cambios
4. Crear Pull Request

## 📄 Licencia

Este proyecto está bajo la licencia MIT. Ver [LICENSE](LICENSE) para más detalles.

## 📞 Soporte

- **Issues**: [GitHub Issues](https://github.com/VdelaRosaAbad/TestManager/issues)
- **Documentación**: [PASOS_TECNICOS.md](PASOS_TECNICOS.md)
- **Contacto**: Crear issue en el repositorio

## 🎯 Roadmap

### 🚀 Versión 1.1
- [ ] Dashboard de monitoreo en tiempo real
- [ ] Alertas automáticas de calidad de datos
- [ ] API REST para consultas programáticas

### 🚀 Versión 1.2
- [ ] Machine Learning para predicción de precios
- [ ] Análisis de sentimiento en notas
- [ ] Integración con sistemas externos

### 🚀 Versión 2.0
- [ ] Pipeline de streaming en tiempo real
- [ ] Análisis de geolocalización
- [ ] Dashboard ejecutivo con KPIs

---

## 🎉 ¡Gracias por usar TestManager!

Este proyecto demuestra cómo procesar eficientemente datasets masivos y transformarlos en insights accionables para la toma de decisiones empresariales.

**⭐ Si te gusta el proyecto, ¡dale una estrella en GitHub!**
