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

#Lendo todos os arquivos de uma pasta e armazendo em um dataframe e inferindo o schema.
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Files/bronze/hr/*.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#fitlrando os funcionários ativos.
df_active = df.filter(df['status']=='A')
display(df_active)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#contando os funcionários ativos.
df_active.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
