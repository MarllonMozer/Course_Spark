# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .appName("parquet") \
        .getOrCreate()
        
print("Spark Session criada com sucesso!")  

# %%
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
caminho = "../download/despachantes.csv"
despachantes = spark.read.csv(caminho, header= False, schema= arqschema)
# %%
despachantes.show()
# %%
# COMO NA MINHA MAQUINA SEMPRE DA ERRO DE NATIVE IO QUANDO TENTO CONVERTER QUALQUER ARQUIVO DE UM 
# TIPO PARA OUTRO EX: CSV PARA PARQUET, PARQUET PARA JSON. ISSO USANDO O SPARK, DESCOBRIR QUE É UM ERRO
# DO HADOOP QUE NAO FUNCIONA DIREITO NO WINDONS SE NAO TIVER UMA INSTALAÇAO COMPLETA. ACREDITO QUE 
# NUM AMBIENTE DOCKER OU DATABRICKS ESSE ERRO NAO ACONTECERIA. A MELHOR SOLUÇAO QUE ACHEI FOI USAR 
# A BIBLIOTECA DO PANDAS E CONVERTER OS ARQUIVOS USANDO ELA. E SIM EU POSSO FAZER TODO O TRATAMENTO 
# DOS DADOS USANDO O SPARK E SO NO FINAL SE EU PRECISAR CONVERTER EU USO O PANDAS.
type(despachantes)
# %%
despachantes_pandas = despachantes.toPandas()
despachantes_pandas.head()
# %%
type(despachantes_pandas)
# %%
despachantes_pandas.to_parquet("despachantes_pd.parquet")
# %%
despachantes_pandas.to_json("despachantes_pd.json", orient="records")
# %%
despachantes_json = spark.read.json("despachantes_pd.json")
despachantes_json.show()
# %%
despachantes_parquet = spark.read.parquet("despachantes_pd.parquet")
despachantes_parquet.show()
# %%
type(despachantes_parquet)

# %%
