# %%
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .appName("Clientes") \
        .getOrCreate()
        
print("Spark Session criada com sucesso")
# %%
caminho = "../../download/atividades/clientes.parquet"
clientes = spark.read.parquet(caminho, header=True, inferSchema=True)
clientes.show()
# %%
# Crie uma consulta oque mostre, nesta ordem,nome,estados,status
clientes = clientes.withColumnRenamed("Cliente", "Nome")
clientes.select("Nome", "Estado", "Status").show()
# %%
# Crie uma consulta que mostre o apenas Clientes do status "Platinum" e "Gold"
clientes_gold_platinum = clientes.filter(("Status == 'Platinum' OR Status == 'Gold'"))
clientes_gold_platinum.show()
# %%
# Demonstre quanto cada Status de Clientes representa em vendas? 
# quero saber o total de vendas do status Platinum, Gold, Silver
spark2 = SparkSession.builder\
          .appName("Vendas")\
          .getOrCreate()

print("Spark2 Session criada com sucesso")
# %%
caminho2 = "../../download/atividades/Vendas.parquet"
vendas = spark.read.parquet(caminho2, header=True, inferSchema=True)
vendas.show()
# %%
resultado = clientes.join(vendas, "ClienteID", "left")
resultado.groupBy("Status").agg(sum("Total").alias("Total_Vendas")).orderBy(col("Total_Vendas").desc()).show()
# %%
