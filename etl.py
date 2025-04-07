from pyspark.sql import SparkSession


# Criar SparkSession com o JAR do PostgreSQL
spark = SparkSession.builder \
    .appName("PostgreSQL Import") \
    .config("spark.jars", "file:///C:/Users/anaxi/.m2/repository/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar") \
    .getOrCreate()


# Configurações de conexão
jdbc_url = "jdbc:postgresql://localhost:5432/db_comex_oltp"
tabela = "public.produtos"
usuario = "postgres"
senha = "root"

# Lê os dados da tabela
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", tabela) \
    .option("user", usuario) \
    .option("password", senha) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Mostra os dados
df.show()
