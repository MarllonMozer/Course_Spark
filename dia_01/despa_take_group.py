#  %%
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .appName("Despachantes") \
        .getOrCreate()
        
print("Spark Session criada com sucesso")        
# %%
# Criar o schema
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
caminho = "../download/despachantes.csv"

# %%
despachantes = spark.read.csv("../download/despachantes.csv",
                             header= False,
                             schema= arqschema)
despachantes.show()
# %%
# take retorna uma lista
despachantes.take(1)
# %%
# retorna todos os dados em forma de lista
despachantes.collect()
# %%
# retorna o numero de linhas
despachantes.count()
# %%
# orderBy ordena os dados em ordem crescente
despachantes.orderBy("vendas").show()
# %%
# orderBy ordena os dados em ordem decrescente com ascending=False
despachantes.orderBy("vendas", ascending=False).show()
# %%
# orderBy ordena os dados em ordem decrescente com desc()
despachantes.orderBy(col("vendas").desc()).show()
# %%
despachantes.orderBy(col("cidade").desc(), col("vendas").desc()).show()
# %%
# agrupar por cidade e somar as vendas e ordenar pela cidade com mais vendas
despachantes.groupBy("cidade").agg(sum("vendas")
                              .alias("total_vendas")).orderBy(col("total_vendas").desc()).show()
# %%
despachantes.filter("nome == 'Deolinda Vilela'").show()
# %%
despachantes.write.parquet("despachantes.parquet")
# %%
despachantes_parquet = spark.read.parquet("/despachantes.parquet")
despachantes_parquet.show()
# %%
despachantes.coalesce(1).write.csv("despachantes.csv", header=True)
# %%
