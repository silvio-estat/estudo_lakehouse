import os
from pyspark.sql import SparkSession

# 1. Força o encerramento da sessão atual para limpar o cache do Jupyter
try:
    spark.stop()
    print("Sessão antiga do Spark encerrada.")
except:
    pass

# 2. Configura as variáveis de caminho de forma dinâmica
try:
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
except NameError:
    current_dir = os.getcwd()
    PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '..')) if current_dir.endswith('src') else current_dir

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# Define o warehouse do ICEBERG (onde seus dados reais vão morar)
warehouse_path = os.path.join(PROJECT_ROOT, "lakehouse")

# Define o warehouse PADRÃO do Spark (trava as pastas genéricas na pasta metadata)
spark_default_warehouse = os.path.join(PROJECT_ROOT, "metadata", "spark_warehouse")

# 3. Cria a sessão blindada com TODAS as configurações
spark = SparkSession.builder.appName("Lakehouse_Iceberg_Spark") \
    .config("spark.sql.warehouse.dir", spark_default_warehouse) \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", warehouse_path) \
    .getOrCreate()

print("Nova sessão iniciada. Catálogo padrão travado em: metadata/spark_warehouse")

# 4. Garante o namespace e define o caminho do Parquet bruto
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")
caminho_parquet = os.path.join(PROJECT_ROOT, "lakehouse", "bronze", "events_raw", "events_raw.parquet")

# 5. Executa a criação da tabela
comando_sql = f"""
CREATE OR REPLACE TABLE iceberg.bronze.events_iceberg 
USING iceberg 
AS SELECT * FROM parquet.`{caminho_parquet}`;
"""

spark.sql(comando_sql)

print("\nTabela criada com sucesso!")
spark.sql("SELECT * FROM iceberg.bronze.events_iceberg").show(5)