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
# META       "default_lakehouse_workspace_id": "993dc7c6-b108-4c01-8424-17f9e122f9cc"
# META     }
# META   }
# META }

# CELL ********************

#library session
import os
from pyspark.sql.types import DateType
import pandas as pd
from datetime import timedelta
from pyspark.sql.functions import col, month, year, expr

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Definindo o caminho da pasta
folder_path = "Files/silver/hr/"
folder_name = os.path.basename(os.path.normpath(folder_path))

# Lendo os arquivos Parquet, somente colunas relevantes
df = spark.read.format("parquet").load(f"{folder_path}*.parquet").select("name", "birthdate", "hiredate","age_category")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Identificar a data mínima e máxima da coluna hiredate
min_date = df.selectExpr("min(hiredate)").collect()[0][0]
max_date = df.selectExpr("max(hiredate)").collect()[0][0]
# Gerar um DataFrame com todas as datas entre min_date e max_date
date_range = pd.date_range(start=min_date, end=max_date).to_pydatetime()
# Converter para DataFrame Spark
date_df = spark.createDataFrame([(date,) for date in date_range], ["hiredate"])

# Adicionar colunas hiremonth e hireyear
date_df = date_df.withColumn("hiremonth", month(col("hiredate"))) \
                 .withColumn("hireyear", year(col("hiredate")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Criando Tabelas gerenciadas
df.write.format("delta").mode("overwrite").saveAsTable("employee")
date_df.write.format("delta").mode("overwrite").saveAsTable("dim_hiredate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Agora vamos escrever um codigo para salvar como managed table and external table.

#managed
df.write.format("delta").mode("overwrite").saveAsTable("dim_employee")
date_df.write.format("delta").mode("overwrite").saveAsTable("dim_hiredate")

#External. Lembre-se que external tables não aparecem na camada semântica.
df.write.format("delta").mode("overwrite").saveAsTable("ex_dim_employee", path=("Files/gold/ex_employee"))
date_df.write.format("delta").mode("overwrite").saveAsTable("ex_dim_hiredate", path=("Files/gold/ex_dim_hiredate"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
