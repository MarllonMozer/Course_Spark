#Crie um banco de dados no DW do Spark
#chamado VendasVarejo, e persista todas as 
#tabelas neste banco de dados.
#2. Crie uma consulta que mostre de cada item 
#vendido: Nome do Cliente, Data da Venda, 
#Produto, Vendedor e Valor Total do item.
#  %%
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .appName("VendasVarejo") \
        .getOrCreate()
        
print("Spark Session criada com sucesso")     

# %%
clientes = spark.read.parquet("../../download/atividades/clientes.parquet", header=True, inferSchema=True)
clientes.show()
# %%
vendas = spark.read.parquet("../../download/atividades/vendas.parquet", header=True, inferSchema=True)
vendas.show()
# %%
itensVendas = spark.read.parquet("../../download/atividades/itensVendas.parquet", header=True, inferSchema=True)
itensVendas.show()
# %%
produtos = spark.read.parquet("../../download/atividades/produtos.parquet", header=True, inferSchema=True)
produtos.show()
# %%
vendedores = spark.read.parquet("../../download/atividades/vendedores.parquet", header=True, inferSchema=True)
vendedores.show()
# %%
#Crie um banco de dados no DW do Spark chamado VendasVarejo, e persista todas as 
#tabelas neste banco de dados.
# Crie uma consulta que mostre de cada item vendido: Nome do Cliente, Data da Venda,
# Produto, Vendedor e Valor Total do item.
vendas_varejo = vendas\
    .join(clientes, "ClienteID", "inner") \
    .join(vendedores, "VendedorID", "inner") \
    .join(itensVendas, "VendasID", "inner") \
    .join(produtos, "ProdutoID", "inner")

vendas_varejo.show()
# %%
vendas_varejo.createOrReplaceTempView("vendas_varejo")
spark.sql("select * from vendas_varejo").show(10)
# %%
# Mesmo codigo de cima so que usando a consulta SQL

clientes.createOrReplaceTempView("clientes")
vendas.createOrReplaceTempView("vendas")
itensVendas.createOrReplaceTempView("itensVendas")
produtos.createOrReplaceTempView("produtos")
vendedores.createOrReplaceTempView("vendedores")
# %%
spark.sql("SELECT * FROM vendas "+
          "INNER JOIN clientes ON vendas.ClienteID = clientes.ClienteID "+
          "INNER JOIN vendedores ON vendas.VendedorID = vendedores.VendedorID "+
          "INNER JOIN itensVendas ON vendas.VendasID = itensVendas.VendasID "+
          "INNER JOIN produtos ON itensVendas.ProdutoID = produtos.ProdutoID").show()
# %%
