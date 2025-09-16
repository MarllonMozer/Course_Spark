# %%
import select
from numpy import inner
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName("Joins")\
        .getOrCreate()
        
print("DEU CERTO")
# %%
caminho = "../download/despachantes.csv"
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv(caminho, header=False, schema=arqschema)
despachantes.show()
# %%
caminho2 = "../download/reclamacoes.csv"
recschema = "idrec INT, datarec STRING, iddesp INT"
reclamaçoes = spark.read.csv(caminho2,header=False, schema=recschema)
reclamaçoes.show()
# %%
despachantes.createOrReplaceTempView("despachantes_view")
reclamaçoes.createOrReplaceTempView("reclamacoes_view")
# %%
resultado_sql = spark.sql("""
    SELECT reclamacoes_view.*, despachantes_view.nome 
    FROM despachantes_view 
    RIGHT JOIN reclamacoes_view ON (despachantes_view.id = reclamacoes_view.iddesp)
""")
resultado_sql.show()
# %%
resultado_spark = despachantes.join(reclamaçoes , despachantes.id == reclamaçoes.iddesp, "inner"
                    ).select("idrec", "datarec", "iddesp","nome")
resultado_spark.show() 
# %%
teste = reclamaçoes.join(
    despachantes.select("id", "nome"),
    reclamaçoes["iddesp"] == despachantes["id"],
    "right"
).select(
    reclamaçoes["*"],
    despachantes["nome"]
).dropna()  # Remove linhas com qualquer valor NULL
teste.show()