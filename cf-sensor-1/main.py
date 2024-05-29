# Ingesta de CSV a BigQuery

import base64
import pandas as pd
import json
import pyarrow
from datetime import datetime
from io import StringIO

from google.cloud import storage
from google.cloud import bigquery

bucket_name='bucket-ppack'
project_name='carbide-calling-393420'
dataset_name='ar_ppack_lake_raw'
table_name='tablon'
file_name='PROVIN PACK S.A. - Reporting - R1.csv'

def activar_proceso():

   # Acceder al CSV del bucket
   storage_client = storage.Client()
   bucket = storage_client.get_bucket(bucket_name)
   blob = bucket.blob(file_name)
   csv_content = blob.download_as_text()

   # Cargar el contenido CSV en un DataFrame de pandas
   raw = pd.read_csv(StringIO(csv_content))   
   print("Dataframe cargado")

   #Tratamiento de valores nulos --------------------------------------------------------
   #raw['cuit'] = raw['cuit'].fillna("")           # CUIT
   #raw['Domicilio'] = raw['Domicilio'].fillna("") # Domicilio
   raw['UXB'] = raw['UXB'].fillna("0")            # UXB
   raw['BULTOS'] = raw['BULTOS'].fillna("0")      # Bultos
   #raw['Marca'] = raw['Marca'].fillna("")         # Marca
   #raw['Sub-Línea'] = raw['Sub-Línea'].fillna("") # Sub-Linea

   ## Tratamiento de tipo de datos 
   raw['Fecha'] = raw['Fecha'].astype('datetime64[ns]')                  # Cambiar a fecha
   raw['UXB'] = raw['UXB'].astype('int64')                               # Cambiar a int
   raw['BULTOS'] = raw['BULTOS'].str.replace(',','.').astype('float64')  # Cambiar a float

   # Eliminar $ de Precio y PxU (está validado que es necesario)
   raw['Precio'] = raw['Precio'].str.replace('$','')
   raw['PxU'] = raw['PxU'].str.replace('$','')

   # Eliminar punto de miles de Precio y PxU
   raw['Precio'] = raw['Precio'].str.replace('.','')
   raw['PxU'] = raw['PxU'].str.replace('.','')

   # Reemplazar punto por coma decimal de Precio y PxU
   raw['Precio'] = raw['Precio'].str.replace(',','.')
   raw['PxU'] = raw['PxU'].str.replace(',','.')

   # Cambiar a float
   raw['Precio'] = raw['Precio'].astype('float64')
   raw['PxU'] = raw['PxU'].astype('float64')

   print("Transformaciones realizadas")
   #------------------------------------------------------------------------------------
   
   # Cambiamos nombre de columnas
   nuevos_nombres = {'Fecha':'fecha',
               'Comprobante':'comprobante',
               'Nombre Cliente':'cliente',
               'cuit':'cuit',
               'Domicilio':'domicilio',
               'Localidad':'localidad',
               'Ciudad':'ciudad',
               'Provincia':'provincia',
               'VENDEDOR':'vendedor',
               'Estado':'estado',
               'Codigo':'codigo',
               'Descripcion':'descripcion',
               'Proveedor':'proveedor',
               'Cantidad':'cantidad',
               'UXB':'unidades_por_bulto',
               'BULTOS':'bultos',
               'Marca':'marca',
               'Sub-Línea':'sublinea',
               'Precio':'precio_total',
               'PxU':'precio_unitario'
   }

   # Utilizamos el método 'rename' con el diccionario de columnas
   raw.rename(columns=nuevos_nombres, inplace=True)

   columnas_object = raw.select_dtypes(include=['object']).columns
   raw[columnas_object] = raw[columnas_object].astype(str)

   # Dataframe to Bigquery
   client = bigquery.Client()
   table_id = f'{project_name}.{dataset_name}.{table_name}'
   table_property = client.get_table(table_id)
     
   job_config = bigquery.LoadJobConfig()
   job_config.schema = table_property.schema
   job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

   job = client.load_table_from_dataframe( raw, table_id, job_config=job_config)
   job.result()


def evento_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    activar_proceso()

if __name__ == "__main__":
    evento_gcs('data', 'context')