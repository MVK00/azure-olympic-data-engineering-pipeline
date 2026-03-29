# Databricks notebook source

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", "service credential")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/")


# COMMAND ----------

athletes = spark.read.format('csv') \
    .option('header', True) \
    .option('inferSchema', True) \
    .load('abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/raw-data/athletes.csv')

coaches = spark.read.format('csv') \
    .option('header', True) \
    .option('inferSchema', True) \
    .load('abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/raw-data/coaches.csv')

entriesgender = spark.read.format('csv') \
    .option('header', True) \
    .option('inferSchema', True) \
    .load('abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/raw-data/entriesgender.csv')

medals = spark.read.format('csv') \
    .option('header', True) \
    .option('inferSchema', True) \
    .load('abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/raw-data/medals.csv')

teams = spark.read.format('csv') \
    .option('header', True) \
    .option('inferSchema', True) \
    .load('abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/raw-data/teams.csv')

# COMMAND ----------


athletes.show()

# COMMAND ----------


athletes.printSchema()

# COMMAND ----------


coaches.show()

# COMMAND ----------


coaches.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------


entriesgender.printSchema()

# COMMAND ----------


medals.show()
     

# COMMAND ----------


medals.printSchema()

# COMMAND ----------


teams.show()

# COMMAND ----------


teams.printSchema()

# COMMAND ----------


# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("TeamCountry","Gold").show()

# COMMAND ----------


# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

athletes.write.format("csv") \
    .option("header", True) \
    .mode("overwrite") \
    .save("abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/transformed-data/athletes")

coaches.write.format("csv") \
    .option("header", True) \
    .mode("overwrite") \
    .save("abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/transformed-data/coaches")

entriesgender.write.format("csv") \
    .option("header", True) \
    .mode("overwrite") \
    .save("abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/transformed-data/entriesgender")

medals.write.format("csv") \
    .option("header", True) \
    .mode("overwrite") \
    .save("abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/transformed-data/medals")

teams.write.format("csv") \
    .option("header", True) \
    .mode("overwrite") \
    .save("abfss://tokyo-olympic-data@tokyoolympicdata1.dfs.core.windows.net/transformed-data/teams")