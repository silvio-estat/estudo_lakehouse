import duckdb
from deltalake import write_deltalake
import os

# Define o caminho raiz do projeto para que os caminhos funcionem corretamente
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# 1. Cria a conexão do DuckDB e lê o arquivo Parquet
conn = duckdb.connect()
source_parquet_path = os.path.join(PROJECT_ROOT, 'lakehouse', 'bronze', 'events_raw.parquet')
df_arrow = conn.execute("""
    SELECT * FROM read_parquet(?)
""", [source_parquet_path]).arrow()

# 2. Escreve os dados no formato Delta Lake real
delta_table_path = os.path.join(PROJECT_ROOT, 'lakehouse', 'bronze', 'events_delta_py')
write_deltalake(delta_table_path, df_arrow, mode='overwrite')

if __name__ == "__main__":
    print(f"Tabela Delta criada com sucesso em: {delta_table_path}")