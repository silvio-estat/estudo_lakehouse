# Arquitetura Lakehouse

## 1. Principais ferramentas

1. **S3 - MinIO**: Será o repositório e conterá a estrutura medalhão para guardar os objetos (Bronze, Prata e Ouro);
2. **Redpanda**: Será utilizado para a injestão de dados de diferentes plataformas;
3. **Airflow**: Será utilizado para orquestração de pipelines de transformação de dados;
4. **DuckDB**: Utilizado para transformacao de dados;
5. **Polars**: Utilizado para transformacao de dados;
6. **Spark**: Utilizado para transformação de dados;
7. **Apache Iceberg**: Utilizado como formato de tabela lakehouse;
8. **OpenMetadata**: Utilizado para guardar os metadados (ajudar achar as informações)
9. **Qdrant**: Utilizado para guardar os vetores de informacoes para RAG;
10. **Grafana**: Utilizado para construir relatórios de BI;

## 2. Politicas de seguranca