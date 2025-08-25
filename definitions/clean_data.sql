-- Limpieza y transformación de datos de acero
-- Tabla final: datamartclean

config {
  type: "table",
  schema: "dataset_acero",
  description: "Tabla de datos limpios y transformados de acero"
}

WITH cleaned_data AS (
  SELECT
    -- Limpiar transaction_id
    COALESCE(TRIM(transaction_id), 'UNKNOWN') as transaction_id,
    
    -- Limpiar y validar fechas
    CASE 
      WHEN SAFE.PARSE_DATE('%Y-%m-%d', date) IS NOT NULL THEN SAFE.PARSE_DATE('%Y-%m-%d', date)
      WHEN SAFE.PARSE_DATE('%m/%d/%Y', date) IS NOT NULL THEN SAFE.PARSE_DATE('%m/%d/%Y', date)
      WHEN SAFE.PARSE_DATE('%d-%m-%Y', date) IS NOT NULL THEN SAFE.PARSE_DATE('%d-%m-%Y', date)
      WHEN REGEXP_MATCH(date, r'^\d{8}$') THEN SAFE.PARSE_DATE('%Y%m%d', date)
      ELSE NULL
    END as date,
    
    -- Limpiar timestamp
    CASE 
      WHEN SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', timestamp) IS NOT NULL THEN SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', timestamp)
      WHEN SAFE.PARSE_DATETIME('%Y-%m-%dT%H:%M:%S', timestamp) IS NOT NULL THEN SAFE.PARSE_DATETIME('%Y-%m-%dT%H:%M:%S', timestamp)
      ELSE NULL
    END as timestamp,
    
    -- Limpiar customer_id
    COALESCE(TRIM(REGEXP_REPLACE(customer_id, r'[Ã±Ã¢Ã]', '')), 'UNKNOWN') as customer_id,
    
    -- Limpiar customer_segment (priorizar español)
    CASE 
      WHEN LOWER(customer_segment) IN ('empresarial', 'empresa', 'business', 'corporate') THEN 'Empresarial'
      WHEN LOWER(customer_segment) IN ('individual', 'persona', 'personal', 'retail') THEN 'Individual'
      WHEN LOWER(customer_segment) IN ('gobierno', 'government', 'publico', 'public') THEN 'Gobierno'
      ELSE COALESCE(TRIM(REGEXP_REPLACE(customer_segment, r'[Ã±Ã¢Ã]', '')), 'Otros')
    END as customer_segment,
    
    -- Limpiar product_id
    COALESCE(TRIM(REGEXP_REPLACE(product_id, r'[Ã±Ã¢Ã]', '')), 'UNKNOWN') as product_id,
    
    -- Limpiar product_category (priorizar español)
    CASE 
      WHEN LOWER(product_category) IN ('varilla', 'reinforcement', 'refuerzo') THEN 'Varilla'
      WHEN LOWER(product_category) IN ('alambre', 'wire', 'cable') THEN 'Alambre'
      WHEN LOWER(product_category) IN ('perfil', 'profile', 'structural') THEN 'Perfil'
      WHEN LOWER(product_category) IN ('tubo', 'tube', 'pipe') THEN 'Tubo'
      ELSE COALESCE(TRIM(REGEXP_REPLACE(product_category, r'[Ã±Ã¢Ã]', '')), 'Otros')
    END as product_category,
    
    -- Limpiar product_lifecycle
    CASE 
      WHEN LOWER(product_lifecycle) IN ('nuevo', 'new', 'activo', 'active') THEN 'Activo'
      WHEN LOWER(product_lifecycle) IN ('descontinuado', 'discontinued', 'obsoleto', 'obsolete') THEN 'Descontinuado'
      WHEN LOWER(product_lifecycle) IN ('faseout', 'phaseout', 'transicion', 'transition') THEN 'Transición'
      ELSE COALESCE(TRIM(REGEXP_REPLACE(product_lifecycle, r'[Ã±Ã¢Ã]', '')), 'Desconocido')
    END as product_lifecycle,
    
    -- Limpiar quantity
    SAFE_CAST(quantity AS INT64) as quantity,
    
    -- Limpiar unit_price
    SAFE_CAST(unit_price AS FLOAT64) as unit_price,
    
    -- Limpiar total_amount
    SAFE_CAST(total_amount AS FLOAT64) as total_amount,
    
    -- Limpiar currency (priorizar MXN)
    CASE 
      WHEN LOWER(currency) IN ('mxn', 'peso', 'pesos', 'mexico') THEN 'MXN'
      WHEN LOWER(currency) IN ('usd', 'dolar', 'dollars', 'usa') THEN 'USD'
      WHEN LOWER(currency) IN ('eur', 'euro', 'europe') THEN 'EUR'
      ELSE COALESCE(UPPER(TRIM(REGEXP_REPLACE(currency, r'[Ã±Ã¢Ã]', ''))), 'MXN')
    END as currency,
    
    -- Limpiar region (priorizar español)
    CASE 
      WHEN LOWER(region) IN ('norte', 'north', 'norte de mexico') THEN 'Norte'
      WHEN LOWER(region) IN ('centro', 'center', 'centro de mexico') THEN 'Centro'
      WHEN LOWER(region) IN ('sur', 'south', 'sur de mexico') THEN 'Sur'
      WHEN LOWER(region) IN ('occidente', 'west', 'occidente de mexico') THEN 'Occidente'
      WHEN LOWER(region) IN ('oriente', 'east', 'oriente de mexico') THEN 'Oriente'
      ELSE COALESCE(TRIM(REGEXP_REPLACE(region, r'[Ã±Ã¢Ã]', '')), 'Centro')
    END as region,
    
    -- Limpiar warehouse
    COALESCE(TRIM(REGEXP_REPLACE(warehouse, r'[Ã±Ã¢Ã]', '')), 'UNKNOWN') as warehouse,
    
    -- Limpiar status
    CASE 
      WHEN LOWER(status) IN ('completado', 'completed', 'finalizado', 'finished') THEN 'Completado'
      WHEN LOWER(status) IN ('pendiente', 'pending', 'en proceso', 'in process') THEN 'Pendiente'
      WHEN LOWER(status) IN ('cancelado', 'cancelled', 'cancelado', 'cancelled') THEN 'Cancelado'
      WHEN LOWER(status) IN ('devuelto', 'returned', 'retorno', 'return') THEN 'Devuelto'
      ELSE COALESCE(TRIM(REGEXP_REPLACE(status, r'[Ã±Ã¢Ã]', '')), 'Pendiente')
    END as status,
    
    -- Limpiar payment_method
    CASE 
      WHEN LOWER(payment_method) IN ('efectivo', 'cash', 'contado') THEN 'Efectivo'
      WHEN LOWER(payment_method) IN ('tarjeta', 'card', 'credito', 'credit') THEN 'Tarjeta'
      WHEN LOWER(payment_method) IN ('transferencia', 'transfer', 'banco', 'bank') THEN 'Transferencia'
      WHEN LOWER(payment_method) IN ('cheque', 'check') THEN 'Cheque'
      ELSE COALESCE(TRIM(REGEXP_REPLACE(payment_method, r'[Ã±Ã¢Ã]', '')), 'Otros')
    END as payment_method,
    
    -- Limpiar discount_pct
    SAFE_CAST(discount_pct AS FLOAT64) as discount_pct,
    
    -- Limpiar tax_amount
    SAFE_CAST(tax_amount AS FLOAT64) as tax_amount,
    
    -- Limpiar notes
    COALESCE(TRIM(REGEXP_REPLACE(notes, r'[Ã±Ã¢Ã]', '')), '') as notes,
    
    -- Limpiar created_by
    COALESCE(TRIM(REGEXP_REPLACE(created_by, r'[Ã±Ã¢Ã]', '')), 'SYSTEM') as created_by,
    
    -- Limpiar modified_date
    CASE 
      WHEN SAFE.PARSE_DATE('%Y-%m-%d', modified_date) IS NOT NULL THEN SAFE.PARSE_DATE('%Y-%m-%d', modified_date)
      WHEN SAFE.PARSE_DATE('%m/%d/%Y', modified_date) IS NOT NULL THEN SAFE.PARSE_DATE('%m/%d/%Y', modified_date)
      WHEN SAFE.PARSE_DATE('%d-%m-%Y', modified_date) IS NOT NULL THEN SAFE.PARSE_DATE('%d-%m-%Y', modified_date)
      ELSE CURRENT_DATE()
    END as modified_date
    
  FROM `{{ var("project_id") }}.{{ var("dataset") }}.{{ var("source_table") }}`
  WHERE transaction_id IS NOT NULL  -- Filtrar filas válidas
),

enriched_data AS (
  SELECT
    *,
    -- Crear columna AnioMes
    CAST(FORMAT_DATE('%Y%m', date) AS INT64) as AnioMes,
    
    -- Crear columna quantity_null
    CASE WHEN quantity IS NULL THEN 1 ELSE 0 END as quantity_null
    
  FROM cleaned_data
  WHERE date IS NOT NULL  -- Solo filas con fecha válida
),

final_data AS (
  SELECT
    *,
    -- Calcular unit_price_imput
    COALESCE(
      unit_price,
      -- Promedio por AnioMes, region y product_id
      AVG(unit_price) OVER (
        PARTITION BY AnioMes, region, product_id 
        ORDER BY AnioMes, region, product_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ),
      -- Promedio por AnioMes, region y product_category
      AVG(unit_price) OVER (
        PARTITION BY AnioMes, region, product_category 
        ORDER BY AnioMes, region, product_category
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ),
      -- Promedio general por AnioMes y region
      AVG(unit_price) OVER (
        PARTITION BY AnioMes, region 
        ORDER BY AnioMes, region
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ),
      -- Promedio general
      AVG(unit_price) OVER ()
    ) as unit_price_imput
    
  FROM enriched_data
)

SELECT
  transaction_id,
  date,
  timestamp,
  customer_id,
  customer_segment,
  product_id,
  product_category,
  product_lifecycle,
  quantity,
  unit_price,
  total_amount,
  currency,
  region,
  warehouse,
  status,
  payment_method,
  discount_pct,
  tax_amount,
  notes,
  created_by,
  modified_date,
  AnioMes,
  quantity_null,
  unit_price_imput
FROM final_data
WHERE transaction_id != 'UNKNOWN'  -- Filtrar transacciones válidas
