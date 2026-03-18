import duckdb
import os
from pyiceberg.catalog.sql import SqlCatalog

# Define o caminho raiz do projeto para que os scripts funcionem de qualquer lugar
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Caminhos absolutos para o warehouse e o catálogo
warehouse_path = os.path.join(PROJECT_ROOT, "lakehouse", "iceberg")
os.makedirs(warehouse_path, exist_ok=True)
catalog_db_path = os.path.join(PROJECT_ROOT, "catalogo_iceberg.db")

# 1. Configura um Catálogo Local (usando um arquivo SQLite para rastrear a tabela)
catalog = SqlCatalog(
    "meu_catalogo",
    **{
        "uri": f"sqlite:///{catalog_db_path}",
        "warehouse": f"file://{warehouse_path}"
    }
)

# 2. Cria o namespace (o equivalente a um schema ou pasta lógica) chamado 'bronze'
try:
    catalog.create_namespace("bronze")
except Exception:
    pass # Ignora se o namespace já existir

# 3. Lê o arquivo bruto com o DuckDB (igual ao seu comando original)
conn = duckdb.connect()
source_parquet_path = os.path.join(PROJECT_ROOT, 'lakehouse', 'bronze', 'events_raw.parquet')
df_arrow = conn.execute("""
    SELECT * FROM read_parquet(?)
""", [source_parquet_path]).to_arrow_table()

# 4. Cria a tabela no formato Iceberg e escreve os dados
nome_tabela = "bronze.events"

try:
    # Remove a tabela se ela já existir para facilitar seus testes
    catalog.drop_table(nome_tabela) 
except Exception:
    pass

# Registra a tabela no catálogo
tabela = catalog.create_table(
    nome_tabela,
    schema=df_arrow.schema
)

# Salva os arquivos físicos particionados/otimizados na pasta
tabela.append(df_arrow)

if __name__ == "__main__":
    print(f"Tabela Iceberg '{nome_tabela}' criada com sucesso!")
