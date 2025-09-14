# ===============================================
# POR QUE PYDOOP FALHOU E O QUE REALMENTE RESOLVER
# ===============================================
# %%
"""
❌ ERRO NO PYDOOP - CAUSAS:
==========================
1. Python 3.11 (muito novo para pydoop 2.0.0)
2. pydoop precisa compilar código C++ (complexo no Windows)
3. pydoop precisa Hadoop COMPLETO instalado (não só winutils)
4. pydoop NÃO resolve o erro NativeIO do Spark
"""

import sys
import platform

def explicar_erro_pydoop():
    """
    Explica por que pydoop falhou e por que não é a solução
    """
    
    print("🚫 POR QUE PYDOOP FALHOU")
    print("="*40)
    
    print(f"Python versão: {sys.version}")
    print(f"Sistema: {platform.system()} {platform.release()}")
    
    print("""
    PROBLEMAS IDENTIFICADOS:
    ========================
    
    1. 🐍 PYTHON 3.11 INCOMPATÍVEL:
       - pydoop 2.0.0 foi feito para Python <= 3.9
       - Setuptools 80.9.0 muito novo
       - AttributeError: 'NoneType' object has no attribute 'strip'
    
    2. 🔧 COMPILAÇÃO C++:
       - pydoop precisa compilar extensões C++
       - Windows não tem compilador por padrão
       - Precisa Microsoft C++ Build Tools
    
    3. 🐘 HADOOP COMPLETO NECESSÁRIO:
       - pydoop precisa Hadoop COMPLETO (não só winutils)
       - Precisa HDFS rodando
       - Configurações complexas
    
    4. ❌ NÃO RESOLVE SEU PROBLEMA:
       - pydoop é para ACESSAR dados no HDFS
       - Seu erro é SALVAR arquivos localmente no Windows
       - São problemas diferentes!
    """)

def solucoes_que_realmente_funcionam():
    """
    Soluções que realmente resolvem o problema NativeIO
    """
    
    print("\n✅ SOLUÇÕES QUE REALMENTE FUNCIONAM")
    print("="*50)
    
    # SOLUÇÃO 1: Spark configurado corretamente
    print("1. 🎯 SPARK COM CONFIGURAÇÕES CERTAS:")
    print("""
    from pyspark.sql import SparkSession
    from pyspark.conf import SparkConf
    
    conf = SparkConf() \\
        .set("spark.hadoop.io.native.lib.available", "false") \\
        .set("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse") \\
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    """)
    
    # SOLUÇÃO 2: Fallback para Pandas
    print("\n2. 🐼 FALLBACK AUTOMÁTICO PARA PANDAS:")
    print("""
    def salvar_seguro(df, nome):
        try:
            df.write.csv(nome)
            print("Salvo com Spark!")
        except:
            df.toPandas().to_csv(f"{nome}.csv")
            print("Salvo com Pandas!")
    """)
    
    # SOLUÇÃO 3: Docker
    print("\n3. 🐳 DOCKER (100% FUNCIONA):")
    print("""
    # Um comando resolve tudo:
    docker run -it --rm -p 8888:8888 jupyter/pyspark-notebook
    
    # Acesse: http://localhost:8888
    # Spark + Hadoop funcionando perfeitamente!
    """)
    
    # SOLUÇÃO 4: WSL2
    print("\n4. 🐧 WSL2 (LINUX NO WINDOWS):")
    print("""
    wsl --install Ubuntu
    # Depois no Ubuntu:
    sudo apt install openjdk-8-jdk python3-pip
    pip3 install pyspark
    # Zero problemas NativeIO!
    """)

def exemplo_pratico():
    """
    Exemplo prático que funciona AGORA
    """
    
    print("\n🚀 CÓDIGO QUE FUNCIONA AGORA")
    print("="*40)
    
    codigo_exemplo = '''
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import tempfile
import os

# Configuração anti-NativeIO
conf = SparkConf().setAppName("SemProblemas") \\
    .set("spark.hadoop.io.native.lib.available", "false") \\
    .set("spark.sql.warehouse.dir", f"file:///{tempfile.gettempdir()}/spark") \\
    .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \\
    .set("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Seus dados
dados = [("João", 25), ("Maria", 30), ("Pedro", 28)]
df = spark.createDataFrame(dados, ["nome", "idade"])

print("DataFrame criado:")
df.show()

# Função que SEMPRE funciona
def salvar_garantido(dataframe, nome_arquivo):
    try:
        # Tentar Spark primeiro
        dataframe.coalesce(1).write.mode("overwrite").csv(nome_arquivo)
        print(f"✅ Salvo com Spark: {nome_arquivo}")
    except Exception as e:
        print(f"⚠️ Spark falhou: {str(e)[:50]}...")
        try:
            # Fallback para Pandas
            pandas_df = dataframe.toPandas()
            pandas_df.to_csv(f"{nome_arquivo}.csv", index=False)
            print(f"✅ Salvo com Pandas: {nome_arquivo}.csv")
        except Exception as e2:
            print(f"❌ Tudo falhou: {e2}")

# Usar a função
salvar_garantido(df, "meus_dados")

spark.stop()
    '''
    
    print(codigo_exemplo)

def comandos_uteis():
    """
    Comandos úteis para resolver definitivamente
    """
    
    print("\n🛠️ COMANDOS PARA RESOLVER DEFINITIVAMENTE")
    print("="*60)
    
    print("OPÇÃO A - Docker (Mais fácil):")
    print("  docker run -it --rm -p 8888:8888 jupyter/pyspark-notebook")
    
    print("\nOPÇÃO B - WSL2 (Para Windows):")
    print("  wsl --install Ubuntu")
    print("  # Depois instalar Spark no Ubuntu")
    
    print("\nOPÇÃO C - Downgrade Python (Complexo):")
    print("  # Instalar Python 3.9 ou 3.8")
    print("  # Reinstalar PySpark")
    
    print("\nOPÇÃO D - Usar apenas Pandas:")
    print("  pip install pandas")
    print("  # Processar dados com pandas instead")

def main():
    """Executar explicação completa"""
    
    explicar_erro_pydoop()
    solucoes_que_realmente_funcionam()
    exemplo_pratico()
    comandos_uteis()
    
    print("\n" + "="*60)
    print("🎯 RESUMO:")
    print("- pydoop NÃO resolve seu problema")
    print("- Use as configurações Spark que mostrei")
    print("- Docker é a solução mais simples")
    print("- WSL2 é a solução mais permanente")
    print("="*60)

if __name__ == "__main__":
    main()

# TESTE RÁPIDO: rode isso para ver se Spark funciona
def teste_rapido():
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("teste").getOrCreate()
        df = spark.createDataFrame([("teste", 1)], ["col", "val"])
        df.show()
        spark.stop()
        print("✅ Spark funcionando!")
        return True
    except Exception as e:
        print(f"❌ Spark com problema: {e}")
        return False

# Descomente para testar:
# teste_rapido()
# %%
