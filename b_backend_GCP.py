# 1. Cargar la bbdd con langchain
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

from google.cloud import bigquery
import os
from dotenv import load_dotenv
load_dotenv()

# 1. crear un objeto cliente de BigQuery usando la clave JSON como parametro
client = bigquery.Client.from_service_account_json('service.json')

# 1. Cargar la bbdd con langchain
from langchain.sql_database import SQLDatabase

dataset_id = 'ar_ppack_lake_raw'
project_id = 'carbide-calling-393420'
engine = create_engine(f"bigquery://{project_id}/{dataset_id}", credentials_path='service.json')

db = SQLDatabase(engine=engine, metadata=MetaData(bind=engine), include_tables=['tablon'])

# 2. Importar las APIs
import a_env_vars
os.environ["OPENAI_API_KEY"] = a_env_vars.OPENAI_API_KEY

# 3. Crear el LLM
from langchain.chat_models import ChatOpenAI
llm = ChatOpenAI(temperature=0, model_name='gpt-3.5-turbo')

# 4. Crear la cadena
from langchain import SQLDatabaseChain
cadena = SQLDatabaseChain(llm= llm, database = db, verbose=False)


# 5. Formato personalizado de respuesta
formato = """
Dada una pregunta del usuario:
1. crea una consulta Standard SQL
2. revisa los resultados
3. devuelve el resultado
4. si tienes que hacer alguna aclaración o devolver cualquier texto que sea siempre en español
#{question}
"""

# 6. Función para hacer la consulta

def consulta(input_usuario):
    consulta = formato.format(question = input_usuario)
    resultado = cadena.run(consulta)
    return(resultado)
