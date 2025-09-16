# %%
from ast import alias
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName("Sql")\
        .getOrCreate()
        
print("DEU CERTO")
# %%
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
caminho = "../download/despachantes.csv"
despachantes = spark.read.csv(caminho, header= False, schema= arqschema)
# %%
despachantes.show()
# %%
# mesmo caso do erro no NATIVEIO
despachantes.createOrReplaceTempView("Despachantes_View")
spark.sql("select * from Despachantes_View").show()
# so estou coparando comandos spark com comando sql entao select * from despachantes é a mesma coisa de
# despachantes.show() 
# %%
spark.sql("select nome, vendas from Despachantes_View").show()
# %%
despachantes.select("nome", "vendas").show()
# %%
spark.sql("select nome, vendas from Despachantes_View where vendas > 20").show()
# %%
despachantes.select("nome", "vendas").where(col("vendas") > 20).show()
# %%
# aqui eu peguei as cidades e agrupei elas e somei a quantiade de vendas de cada cidade agrupada
# eu ordenei pelo index como coloquei 2 eu ordenei pela segunda coluna que no caso é a coluna vendas
# e coloquei de forma decrecente
spark.sql("select cidade, sum(vendas) as total_vendas from Despachantes_View group by cidade order by 2 desc").show()
# %%
despachantes.groupBy("cidade").agg(sum("vendas").alias("total_vendas")).orderBy(col("total_vendas").desc()).show()
# %%
