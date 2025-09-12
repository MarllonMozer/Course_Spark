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
despachantes.schema
# %%
despachantes.select("id","nome","vendas").where(col("vendas") > 20).show()
# %%
despachantes.select("id","nome","vendas").filter("vendas > 20").show()
# %%
despachantes.select("id","nome","vendas").filter("vendas > 20 and vendas < 40").show()
# %%
novodf = despachantes.withColumnRenamed("nome","nomes")
novodf.show()
# %%
despachantes_date = despachantes.withColumn("data_Date", to_date(col("data"), "yyyy-MM-dd"))
despachantes_date.printSchema()
# %%
# diferenca entre date e timestamp é que o timestamp tem horas, minutos e segundos
despachantes_timestamp = despachantes.withColumn("data_TimeStamp", to_timestamp(col("data"), "yyyy-MM-dd HH:mm:ss"))
despachantes_timestamp.printSchema()
# %%
# mesmo a data em string, consigo extrair o ano ou mes ou dia
despachantes.select(year("data").alias("ano")).show()
# %%
despachantes.select(year("data").alias("ano")).distinct().show()
# %%
despachantes.select("nome", year("data").alias("ano")).orderBy("nome").show()
# %%
despachantes.select(year("data").alias("ano")).groupBy("ano").count().show()
# %%
# somar as vendas SUM
despachantes.select(sum("vendas").alias("total_vendas")).show()
# %%
