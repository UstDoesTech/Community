# Databricks notebook source
product_save_path = '/mnt/lake/raw/product'
product_path = 'dbfs:/FileStore/Autoloader/'
checkpoint = f"{product_save_path}/_checkpoint"

# COMMAND ----------

dbutils.fs.ls(product_path)

# COMMAND ----------

schema = 'ProductID INT, Name STRING, ProductNumber STRING, Color STRING, StandardCost DOUBLE, ListPrice DOUBLE, Size STRING, Weight DOUBLE, ProductCategoryID INT, ProductModelID STRING, SellStartDate TIMESTAMP, SellEndDate TIMESTAMP, DiscontinuedDate TIMESTAMP, ThumbNailPhoto STRING, ThumbnailPhotoFileName STRING, rowguid STRING, ModifiedDate TIMESTAMP'

# COMMAND ----------

normal_autoloader_config = {
    "cloudFiles.schemaLocation": product_save_path,
    "cloudFiles.format": "json"
}

# COMMAND ----------

product = (spark
           .readStream
           .format("cloudFiles")
           .options(**normal_autoloader_config)
           .schema(schema)
           .load(product_path)
)

# COMMAND ----------

stream = (product.writeStream
          .option("checkpointLocation", checkpoint)
          .outputMode("append")
          .trigger(once=True)
          .format("delta")
          .start(product_save_path)
          )

# COMMAND ----------

dbutils.fs.ls('/mnt/lake/raw/product')

# COMMAND ----------

total_rows = stream.recentProgress[0]['numInputRows']
print(total_rows)

# COMMAND ----------

# Upload Product_noneNewColumn.json

# COMMAND ----------

dbutils.fs.ls(product_path)

# COMMAND ----------

df = spark.read.format("json").load("dbfs:/FileStore/Autoloader/Product_noneNewColumn.json")
display(df)

# COMMAND ----------

ignore_column_autoloader_config = {
    "cloudFiles.schemaLocation": product_save_path,
    "cloudFiles.format": "json",
    "cloudFiles.schemaEvolutionMode": "none"
}

# COMMAND ----------

product = (spark
           .readStream
           .format("cloudFiles")
           .options(**ignore_column_autoloader_config)
           .schema(schema)
           .load(product_path)
)

# COMMAND ----------

stream = (product.writeStream
          .option("checkpointLocation", checkpoint)
          .outputMode("append")
          .trigger(once=True)
          .format("delta")
          .start(product_save_path)
          )

# COMMAND ----------

total_rows = stream.recentProgress[0]['numInputRows']
print(total_rows)

# COMMAND ----------

df = spark.read.format("delta").load(product_save_path)
display(df)

# COMMAND ----------

# Upload Product_failNewColumn.json

# COMMAND ----------

dbutils.fs.ls(product_path)

# COMMAND ----------

df = spark.read.format("json").load("dbfs:/FileStore/Autoloader/Product_failNewColumn.json")
display(df)

# COMMAND ----------

fail_column_autoloader_config = {
    "cloudFiles.schemaLocation": product_save_path,
    "cloudFiles.format": "json",
    "cloudFiles.schemaEvolutionMode": "failOnNewColumns"
}

# COMMAND ----------

product = (spark
           .readStream
           .format("cloudFiles")
           .options(**fail_column_autoloader_config)
           .schema(schema)
           .load(product_path)
)

# COMMAND ----------

stream = (product.writeStream
          .option("checkpointLocation", checkpoint)
          .outputMode("append")
          .trigger(once=True)
          .format("delta")
          .start(product_save_path)
          )

# COMMAND ----------

total_rows = stream.recentProgress[0]['numInputRows']
print(total_rows)

# COMMAND ----------

df = spark.read.format("delta").load(product_save_path)
display(df)

# COMMAND ----------

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

# COMMAND ----------

rescue_column_autoloader_config = {
    "cloudFiles.schemaLocation": product_save_path,
    "cloudFiles.format": "json",
    "cloudFiles.schemaEvolutionMode": "rescue"
}

# COMMAND ----------

product = (spark
           .readStream
           .format("cloudFiles")
           .options(**rescue_column_autoloader_config)
           .schema(schema)
           .load(product_path)
)

# COMMAND ----------

stream = (product.writeStream
          .option("checkpointLocation", checkpoint)
          .option("mergeSchema", "true")
          .outputMode("append")
          .trigger(once=True)
          .format("delta")
          .start(product_save_path)
          )

# COMMAND ----------

total_rows = stream.recentProgress[0]['numInputRows']
print(total_rows)

# COMMAND ----------

df = spark.read.format("delta").load(product_save_path)
display(df)

# COMMAND ----------

#Upload Product_addNewColumn.json

# COMMAND ----------

dbutils.fs.ls(product_path)

# COMMAND ----------

df = spark.read.format("json").load("dbfs:/FileStore/Autoloader/Product_rescueNewColumn.json")
display(df)

# COMMAND ----------

rescue_column_autoloader_config = {
    "cloudFiles.schemaLocation": product_save_path,
    "cloudFiles.format": "json",
    "cloudFiles.schemaEvolutionMode": "addNewColumns",
    "cloudFiles.schemaHints": schema
}

# COMMAND ----------

product = (spark
           .readStream
           .format("cloudFiles")
           .options(**rescue_column_autoloader_config)
        #    .schema(schema)
           .load(product_path)
)

# COMMAND ----------

stream = (product.writeStream
          .option("checkpointLocation", checkpoint)
          .option("mergeSchema", "true")
          .outputMode("append")
          .trigger(once=True)
          .format("delta")
          .start(product_save_path)
          )

# COMMAND ----------

total_rows = stream.recentProgress[0]['numInputRows']
print(total_rows)

# COMMAND ----------

df = spark.read.format("delta").load(product_save_path)
display(df)
