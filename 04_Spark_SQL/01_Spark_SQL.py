# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .appName("SQL") \
        .getOrCreate()
        
print("Spark Session criada com sucesso!")        
# %%
spark.sql("show databases").show()
# %%
spark.sql("create database desp")
# %%
spark.sql("show databases").show()
# %%
# retorna uma saida vazia
spark.sql("use desp").show()
# %%
# criando uma tabela gerenciada 
# vamos criar a tabela a partir de um dataframe
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
caminho = "../download/despachantes.csv"
despachantes = spark.read.csv(caminho, header= False, schema= arqschema)
# %%
despachantes.show()
# %%
# como a gente criou o banco de dados desp e apontamos pra ele usando "use desp"
# essa tabela vai ser criada dentro do banco de dados desp caso contrario ela 
# seria criada no banco default

# sempre da erro de NativeIO por causa das bibliotecas nativas do hadoop no windows
# eu devo fazer esse mesmo comando usando pandas.


# o copilot deu uma sugestao de usar esse createOrReplaceTempView e deu certo entao nem precisei
# usar o pandas 
despachantes.write.saveAsTable("Despachantes_View")
# %%
despachantes.createOrReplaceTempView("Despachantes_View")
spark.sql("select * from Despachantes_View").show()
# %%
spark.sql("show tables").show()
# %%
# Quando criamos uma tabela e exibimos ela retorna um dataframe 
despachantes2 = spark.sql("select * from Despachantes_View")
despachantes2.show()
# %%
spark.catalog.listTables()
# %%
# para consultar a view global tem quie colocar global_temp.nome da view
despachantes.createOrReplaceGlobalTempView("Despachantes_View_2")
spark.sql("select * from global_temp.Despachantes_view_2").show()
# %%
spark.sql("CREATE OR REPLACE TEMP VIEW DESP_VIEW AS select * from despachantes")
# %%
spark.sql("select * from DESP_VIEW").show()
# %%
spark.sql("CREATE OR REPLACE GLOBAL TEMP VIEW DESP_VIEW2 AS select * from despachantes")
spark.sql("select * from globa_temp.DESP_VIEW2").show()
# %%
