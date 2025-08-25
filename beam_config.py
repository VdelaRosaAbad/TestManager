#!/usr/bin/env python3
"""
Configuración de Apache Beam para procesamiento de datos
(Configuración de respaldo para futuras implementaciones)
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms import ParDo, Map
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterCount, AfterWatermark
from apache_beam.transforms.trigger import AccumulationMode

class AceroDataOptions(PipelineOptions):
    """Opciones de configuración para el pipeline de datos de acero"""
    
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            default='gs://desafio-deacero-143d30a0-d8f8-4154-b7df-1773cf286d32/cdo_challenge.csv.gz',
            help='Ruta del archivo de entrada'
        )
        parser.add_argument(
            '--output',
            default='gs://bucket_acero/processed/',
            help='Ruta de salida para datos procesados'
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
            '--temp_location',
            default='gs://bucket_acero/temp/',
            help='Ubicación temporal para archivos intermedios'
        )
        parser.add_argument(
            '--staging_location',
            default='gs://bucket_acero/staging/',
            help='Ubicación de staging para archivos'
        )
        parser.add_argument(
            '--max_num_workers',
            default=10,
            type=int,
            help='Número máximo de workers'
        )
        parser.add_argument(
            '--worker_machine_type',
            default='n1-standard-4',
            help='Tipo de máquina para workers'
        )

class CleanTextData(beam.DoFn):
    """Transformación para limpiar datos de texto"""
    
    def process(self, element):
        """Procesa cada línea de texto"""
        try:
            # Dividir la línea en campos
            fields = element.split(',')
            
            if len(fields) >= 20:  # Verificar que tenga suficientes campos
                # Limpiar caracteres corruptos
                cleaned_fields = []
                for field in fields:
                    cleaned_field = field.strip()
                    # Corregir caracteres corruptos
                    cleaned_field = cleaned_field.replace('Ã±', 'ñ')
                    cleaned_field = cleaned_field.replace('Ã¢', '')
                    cleaned_field = cleaned_field.replace('Ã', '')
                    cleaned_fields.append(cleaned_field)
                
                # Retornar línea limpia
                yield ','.join(cleaned_fields)
            else:
                # Línea inválida, omitir
                pass
                
        except Exception as e:
            # Log del error pero continuar procesando
            print(f"Error procesando línea: {e}")
            pass

class ParseDateFields(beam.DoFn):
    """Transformación para parsear campos de fecha"""
    
    def process(self, element):
        """Procesa cada línea para parsear fechas"""
        try:
            fields = element.split(',')
            
            if len(fields) >= 20:
                # Índices de campos de fecha (ajustar según esquema real)
                date_indices = [1, 2, 19]  # date, timestamp, modified_date
                
                for idx in date_indices:
                    if idx < len(fields):
                        # Intentar parsear fecha
                        date_str = fields[idx].strip()
                        if date_str:
                            # Aquí iría la lógica de parsing de fechas
                            # Por ahora solo limpiar
                            fields[idx] = date_str
                
                yield ','.join(fields)
            else:
                yield element
                
        except Exception as e:
            print(f"Error parseando fechas: {e}")
            yield element

class AddDerivedFields(beam.DoFn):
    """Transformación para agregar campos derivados"""
    
    def process(self, element):
        """Agrega campos derivados a cada línea"""
        try:
            fields = element.split(',')
            
            if len(fields) >= 20:
                # Agregar AnioMes (campo 21)
                try:
                    date_str = fields[1]  # Campo date
                    if date_str and len(date_str) >= 4:
                        year = date_str[:4]
                        month = date_str[5:7] if len(date_str) >= 7 else "01"
                        anio_mes = f"{year}{month}"
                        fields.append(anio_mes)
                    else:
                        fields.append("")
                except:
                    fields.append("")
                
                # Agregar quantity_null (campo 22)
                try:
                    quantity = fields[8]  # Campo quantity
                    if quantity and quantity.strip():
                        fields.append("0")  # No es nulo
                    else:
                        fields.append("1")  # Es nulo
                except:
                    fields.append("1")
                
                # Agregar unit_price_imput (campo 23)
                try:
                    unit_price = fields[9]  # Campo unit_price
                    if unit_price and unit_price.strip():
                        fields.append(unit_price)  # Usar valor original
                    else:
                        fields.append("")  # Se calculará después
                except:
                    fields.append("")
                
                yield ','.join(fields)
            else:
                yield element
                
        except Exception as e:
            print(f"Error agregando campos derivados: {e}")
            yield element

def create_pipeline(options):
    """Crea el pipeline de Apache Beam"""
    
    with beam.Pipeline(options=options) as pipeline:
        
        # Leer datos de entrada
        lines = (pipeline 
                | 'ReadFromGCS' >> ReadFromText(options.input)
                | 'CleanText' >> ParDo(CleanTextData())
                | 'ParseDates' >> ParDo(ParseDateFields())
                | 'AddDerivedFields' >> ParDo(AddDerivedFields())
                )
        
        # Escribir resultados
        (lines 
         | 'WriteToGCS' >> WriteToText(
             options.output,
             file_name_suffix='.csv',
             num_shards=1
         ))
    
    return pipeline

def run_pipeline():
    """Ejecuta el pipeline de Apache Beam"""
    
    # Configurar opciones
    options = AceroDataOptions()
    
    # Configurar opciones de ejecución
    options.view_as(beam.options.pipeline_options.GoogleCloudOptions).project = options.project
    options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = options.region
    options.view_as(beam.options.pipeline_options.GoogleCloudOptions).temp_location = options.temp_location
    options.view_as(beam.options.pipeline_options.GoogleCloudOptions).staging_location = options.staging_location
    
    # Configurar opciones de worker
    options.view_as(beam.options.pipeline_options.WorkerOptions).max_num_workers = options.max_num_workers
    options.view_as(beam.options.pipeline_options.WorkerOptions).worker_machine_type = options.worker_machine_type
    
    # Crear y ejecutar pipeline
    pipeline = create_pipeline(options)
    
    print("Pipeline de Apache Beam configurado correctamente")
    print(f"Archivo de entrada: {options.input}")
    print(f"Directorio de salida: {options.output}")
    print(f"Proyecto: {options.project}")
    print(f"Región: {options.region}")
    
    return pipeline

if __name__ == "__main__":
    # Solo mostrar configuración, no ejecutar
    print("Configuración de Apache Beam para procesamiento de datos de acero")
    print("Este archivo contiene la configuración para futuras implementaciones")
    print("Para usar Apache Beam, ejecuta: python beam_config.py --help")
    
    # Mostrar opciones disponibles
    options = AceroDataOptions()
    print(f"\nOpciones de configuración:")
    print(f"  --input: {options.input}")
    print(f"  --output: {options.output}")
    print(f"  --project: {options.project}")
    print(f"  --region: {options.region}")
    print(f"  --max_num_workers: {options.max_num_workers}")
    print(f"  --worker_machine_type: {options.worker_machine_type}")
