# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "818dd992-40eb-4e75-b1e5-fe97da5dceab",
# META       "default_lakehouse_name": "LH_FabricGit",
# META       "default_lakehouse_workspace_id": "993dc7c6-b108-4c01-8424-17f9e122f9cc",
# META       "known_lakehouses": [
# META         {
# META           "id": "818dd992-40eb-4e75-b1e5-fe97da5dceab"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Library session
from pyspark.sql.functions import col, year, month, dayofmonth
from pyspark.sql.functions import current_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Lendo todos os arquivos de uma pasta e armazendo em um dataframe e inferindo o schema.
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Files/bronze/hr/*.csv")

#fitlrando os funcionários ativos.
df_active = df.filter(df['status']=='A')

# Calculando a idade
df_active = df.withColumn("age",(year(current_date()) - year(col("birthdate")) .cast("int")))

from pyspark.sql.functions import col, when
# Classificando a idade em categorias
df_active= df_active.withColumn("age_category",
                   when(col("age").between(0, 25), "menor que 25 anos")
                   .when(col("age").between(26, 35), "entre 26 e 35 anos")
                   .when(col("age").between(36, 45), "entre 36 e 45 anos")
                   .otherwise("Maior que 45"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Gravando como parquet na silver
df_active.write.format('parquet').mode('overwrite').save('Files/silver/hr')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
