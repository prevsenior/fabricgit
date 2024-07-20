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

#Lendo todos os arquivos de uma pasta e armazendo em um dataframe.
df = spark.read.format("csv").option("header","true").load("Files/bronze/hr/*.csv")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
