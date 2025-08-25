#!/usr/bin/env python3
"""
Configuración de Google Cloud Dataflow para procesamiento de datos de acero
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms import ParDo, Map, GroupByKey, Combine
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterCount, AfterWatermark
from apache_beam.transforms.trigger import AccumulationMode
import json
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AceroDataflowOptions(PipelineOptions):
    """Opciones de configuración para Dataflow"""
    
    @classmethod
    def _add_argparse_args(cls, parser):
        # Opciones de entrada/salida
        parser.add_argument(
            '--input',
            default='gs://desafio-deacero-143d30a0-d8f8-4154-b7df-1773cf286d32/cdo_challenge.csv.gz',
            help='Ruta del archivo de entrada'
        )
        parser.add_argument(
            '--output',
            default='gs://bucket_acero/dataflow_output/',
            help='Ruta de salida para datos procesados'
        )
        parser.add_argument(
            '--temp_location',
            default='gs://bucket_acero/temp/',
            help='Ubicación temporal para archivos intermedios'
        )
        parser.add_argument(
            '--staging_location',
            default='gs://bucket_acero/staging/',
            help='Ubicación de staging para archivos'
        )
        
        # Opciones de Dataflow
        parser.add_argument(
            '--runner',
            default='DataflowRunner',
            help='Runner a usar (DataflowRunner, DirectRunner)'
        )
        parser.add_argument(
            '--project',
            default='testmanager-470115',
            help='ID del proyecto de Google Cloud'
        )
        parser.add_argument(
            '--region',
            default='us-central1',
            help='Región para ejecutar el pipeline'
        )
        parser.add_argument(
            '--job_name',
            default='acero-data-processing',
            help='Nombre del job de Dataflow'
        )
        
        # Opciones de workers
        parser.add_argument(
            '--max_num_workers',
            default=20,
            type=int,
            help='Número máximo de workers'
        )
        parser.add_argument(
            '--worker_machine_type',
            default='n1-standard-4',
            help='Tipo de máquina para workers'
        )
        parser.add_argument(
            '--disk_size_gb',
            default=50,
            type=int,
            help='Tamaño del disco en GB por worker'
        )
        parser.add_argument(
            '--num_workers',
            default=5,
            type=int,
            help='Número inicial de workers'
        )

class ParseCSVLine(beam.DoFn):
    """Parsea líneas CSV y las convierte en diccionarios"""
    
    def process(self, element):
        """Procesa cada línea CSV"""
        try:
            # Dividir línea en campos
            fields = element.split(',')
            
            if len(fields) >= 20:
                # Crear diccionario con campos
                record = {
                    'transaction_id': fields[0].strip(),
                    'date': fields[1].strip(),
                    'timestamp': fields[2].strip(),
                    'customer_id': fields[3].strip(),
                    'customer_segment': fields[4].strip(),
                    'product_id': fields[5].strip(),
                    'product_category': fields[6].strip(),
                    'product_lifecycle': fields[7].strip(),
                    'quantity': fields[8].strip(),
                    'unit_price': fields[9].strip(),
                    'total_amount': fields[10].strip(),
                    'currency': fields[11].strip(),
                    'region': fields[12].strip(),
                    'warehouse': fields[13].strip(),
                    'status': fields[14].strip(),
                    'payment_method': fields[15].strip(),
                    'discount_pct': fields[16].strip(),
                    'tax_amount': fields[17].strip(),
                    'notes': fields[18].strip(),
                    'created_by': fields[19].strip(),
                    'modified_date': fields[20].strip() if len(fields) > 20 else ''
                }
                
                yield record
            else:
                logger.warning(f"Línea con campos insuficientes: {len(fields)} campos")
                
        except Exception as e:
            logger.error(f"Error parseando línea CSV: {e}")
            pass

class CleanTextData(beam.DoFn):
    """Limpia caracteres corruptos en campos de texto"""
    
    def process(self, element):
        """Limpia cada registro"""
        try:
            # Limpiar caracteres corruptos en campos de texto
            text_fields = [
                'customer_id', 'customer_segment', 'product_id', 'product_category',
                'product_lifecycle', 'currency', 'region', 'warehouse', 'status',
                'payment_method', 'notes', 'created_by'
            ]
            
            for field in text_fields:
                if field in element and element[field]:
                    # Corregir caracteres corruptos
                    cleaned_value = element[field]
                    cleaned_value = cleaned_value.replace('Ã±', 'ñ')
                    cleaned_value = cleaned_value.replace('Ã¢', '')
                    cleaned_value = cleaned_value.replace('Ã', '')
                    cleaned_value = cleaned_value.strip()
                    element[field] = cleaned_value
            
            yield element
            
        except Exception as e:
            logger.error(f"Error limpiando texto: {e}")
            yield element

class NormalizeDates(beam.DoFn):
    """Normaliza campos de fecha"""
    
    def process(self, element):
        """Normaliza fechas en cada registro"""
        try:
            date_fields = ['date', 'modified_date']
            
            for field in date_fields:
                if field in element and element[field]:
                    date_str = element[field].strip()
                    if date_str:
                        # Intentar diferentes formatos de fecha
                        normalized_date = self.parse_date(date_str)
                        if normalized_date:
                            element[field] = normalized_date
            
            yield element
            
        except Exception as e:
            logger.error(f"Error normalizando fechas: {e}")
            yield element
    
    def parse_date(self, date_str):
        """Parsea fecha en diferentes formatos"""
        try:
            # Implementar lógica de parsing de fechas
            # Por ahora retornar el string original
            return date_str
        except:
            return None

class ConvertNumericFields(beam.DoFn):
    """Convierte campos numéricos"""
    
    def process(self, element):
        """Convierte campos numéricos en cada registro"""
        try:
            numeric_fields = ['quantity', 'unit_price', 'total_amount', 'discount_pct', 'tax_amount']
            
            for field in numeric_fields:
                if field in element and element[field]:
                    try:
                        value = float(element[field])
                        element[field] = value
                    except (ValueError, TypeError):
                        element[field] = None
            
            yield element
            
        except Exception as e:
            logger.error(f"Error convirtiendo campos numéricos: {e}")
            yield element

class AddDerivedFields(beam.DoFn):
    """Agrega campos derivados"""
    
    def process(self, element):
        """Agrega campos derivados a cada registro"""
        try:
            # Agregar AnioMes
            if 'date' in element and element['date']:
                try:
                    date_str = str(element['date'])
                    if len(date_str) >= 4:
                        year = date_str[:4]
                        month = date_str[5:7] if len(date_str) >= 7 else "01"
                        element['AnioMes'] = int(f"{year}{month}")
                    else:
                        element['AnioMes'] = None
                except:
                    element['AnioMes'] = None
            else:
                element['AnioMes'] = None
            
            # Agregar quantity_null
            if 'quantity' in element:
                element['quantity_null'] = 1 if element['quantity'] is None else 0
            else:
                element['quantity_null'] = 1
            
            # Agregar unit_price_imput (placeholder)
            element['unit_price_imput'] = element.get('unit_price')
            
            yield element
            
        except Exception as e:
            logger.error(f"Error agregando campos derivados: {e}")
            yield element

class CalculatePriceAverages(beam.DoFn):
    """Calcula promedios de precios para imputación"""
    
    def process(self, element):
        """Procesa cada registro para calcular promedios"""
        try:
            if element.get('AnioMes') and element.get('region') and element.get('product_id'):
                key = (element['AnioMes'], element['region'], element['product_id'])
                if element.get('unit_price'):
                    yield (key, (element['unit_price'], 1))  # (precio, contador)
                else:
                    yield (key, (0, 0))  # Sin precio
            else:
                yield (('unknown', 'unknown', 'unknown'), (0, 0))
                
        except Exception as e:
            logger.error(f"Error calculando promedios: {e}")
            yield (('error', 'error', 'error'), (0, 0))

class ImputePrices(beam.DoFn):
    """Imputa precios faltantes usando promedios calculados"""
    
    def __init__(self, price_averages):
        super().__init__()
        self.price_averages = price_averages
    
    def process(self, element):
        """Imputa precios en cada registro"""
        try:
            if element.get('unit_price') is None:
                # Intentar imputar precio
                anio_mes = element.get('AnioMes')
                region = element.get('region')
                product_id = element.get('product_id')
                
                if anio_mes and region and product_id:
                    key = (anio_mes, region, product_id)
                    if key in self.price_averages:
                        avg_price, count = self.price_averages[key]
                        if count > 0:
                            element['unit_price_imput'] = avg_price
                        else:
                            element['unit_price_imput'] = None
                    else:
                        element['unit_price_imput'] = None
                else:
                    element['unit_price_imput'] = None
            else:
                element['unit_price_imput'] = element['unit_price']
            
            yield element
            
        except Exception as e:
            logger.error(f"Error imputando precios: {e}")
            yield element

def create_dataflow_pipeline(options):
    """Crea el pipeline de Dataflow"""
    
    with beam.Pipeline(options=options) as pipeline:
        
        # Leer datos de entrada
        raw_data = (pipeline 
                   | 'ReadFromGCS' >> ReadFromText(options.input)
                   | 'ParseCSV' >> ParDo(ParseCSVLine())
                   | 'CleanText' >> ParDo(CleanTextData())
                   | 'NormalizeDates' >> ParDo(NormalizeDates())
                   | 'ConvertNumeric' >> ParDo(ConvertNumericFields())
                   | 'AddDerivedFields' >> ParDo(AddDerivedFields())
                   )
        
        # Calcular promedios de precios para imputación
        price_data = (raw_data 
                     | 'FilterForPrices' >> beam.Filter(lambda x: x.get('unit_price') is not None)
                     | 'CalculateAverages' >> ParDo(CalculatePriceAverages())
                     | 'GroupByKey' >> GroupByKey()
                     | 'CombineAverages' >> beam.CombinePerKey(
                         beam.combiners.MeanCombineFn()
                     ))
        
        # Convertir a diccionario para lookup
        price_averages = (price_data 
                         | 'ToDict' >> beam.Map(lambda x: (x[0], x[1])))
        
        # Imputar precios
        final_data = (raw_data 
                     | 'ImputePrices' >> ParDo(ImputePrices(price_averages.asDict())))
        
        # Escribir resultados
        (final_data 
         | 'ConvertToCSV' >> beam.Map(lambda x: ','.join(str(v) for v in x.values()))
         | 'WriteToGCS' >> WriteToText(
             options.output,
             file_name_suffix='.csv',
             num_shards=1
         ))
    
    return pipeline

def run_dataflow_pipeline():
    """Ejecuta el pipeline de Dataflow"""
    
    # Configurar opciones
    options = AceroDataflowOptions()
    
    # Configurar opciones de Google Cloud
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = options.project
    google_cloud_options.region = options.region
    google_cloud_options.temp_location = options.temp_location
    google_cloud_options.staging_location = options.staging_location
    
    # Configurar opciones de workers
    worker_options = options.view_as(WorkerOptions)
    worker_options.max_num_workers = options.max_num_workers
    worker_options.worker_machine_type = options.worker_machine_type
    worker_options.disk_size_gb = options.disk_size_gb
    worker_options.num_workers = options.num_workers
    
    # Configurar runner
    options.runner = options.runner
    
    # Crear y ejecutar pipeline
    pipeline = create_dataflow_pipeline(options)
    
    logger.info("Pipeline de Dataflow configurado correctamente")
    logger.info(f"Archivo de entrada: {options.input}")
    logger.info(f"Directorio de salida: {options.output}")
    logger.info(f"Proyecto: {options.project}")
    logger.info(f"Región: {options.region}")
    logger.info(f"Runner: {options.runner}")
    logger.info(f"Workers máximos: {options.max_num_workers}")
    
    return pipeline

if __name__ == "__main__":
    # Solo mostrar configuración, no ejecutar
    print("Configuración de Google Cloud Dataflow para procesamiento de datos de acero")
    print("Este archivo contiene la configuración para futuras implementaciones")
    print("Para usar Dataflow, ejecuta: python dataflow_config.py --help")
    
    # Mostrar opciones disponibles
    options = AceroDataflowOptions()
    print(f"\nOpciones de configuración:")
    print(f"  --input: {options.input}")
    print(f"  --output: {options.output}")
    print(f"  --project: {options.project}")
    print(f"  --region: {options.region}")
    print(f"  --runner: {options.runner}")
    print(f"  --max_num_workers: {options.max_num_workers}")
    print(f"  --worker_machine_type: {options.worker_machine_type}")
    print(f"  --disk_size_gb: {options.disk_size_gb}")
