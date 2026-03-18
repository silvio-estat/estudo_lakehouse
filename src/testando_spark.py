import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Define o caminho raiz do projeto
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# 1. Garante o uso do Java 17 para evitar o erro de ambiente
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# 2. Configura o Spark com suporte ao Delta Lake e limpa a raiz do projeto
builder = SparkSession.builder.appName("LakehouseC2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", os.path.join(PROJECT_ROOT, ".spark_warehouse"))

# Inicia a sessão
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define caminhos absolutos para a query SQL
delta_table_path = os.path.join(PROJECT_ROOT, 'lakehouse', 'bronze', 'events_delta')
source_parquet_path = os.path.join(PROJECT_ROOT, 'lakehouse', 'bronze', 'events_raw.parquet')

# 3. Comando SQL adaptado para a nova estrutura 'lakehouse'
comando_sql_do_livro = f"""
CREATE TABLE IF NOT EXISTS bronze_events_delta 
USING delta LOCATION '{delta_table_path}' 
AS SELECT * FROM parquet.`{source_parquet_path}`
"""

# 4. Executa o comando
spark.sql(comando_sql_do_livro)

print("Tabela Delta criada com sucesso na nova camada Bronze!")