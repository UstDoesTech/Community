# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.originalTable

# COMMAND ----------

regName = 'default.originalTable'

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {regName}_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.originalTable (
# MAGIC   Id INT NOT NULL,
# MAGIC   Name STRING
# MAGIC )

# COMMAND ----------

struct = StructType([
  StructField('Id', IntegerType(), False, {'comment': 'The ID Columns of an object'}),
  StructField('Name', StringType(), False, {'comment': 'The name of an object'}),
  StructField('Group', LongType(), False, {'comment': 'The Grouping for an object'})
])

# COMMAND ----------

def createColumns(struct):
  columns = ""
  for column in struct:
    comment = ""
    nullableColumn = ""
    if column.jsonValue()["nullable"] == False:
      nullableColumn = "NOT NULL"
    if column.metadata != {}:
      metadataComment = column.metadata["comment"]
      comment = f"COMMENT '{metadataComment}'"
    columns += f"""{column.jsonValue()["name"]} {column.jsonValue()["type"]} {nullableColumn} {comment}, """
  
  return columns

# COMMAND ----------

def createEmptyCuratedTable(struct, regName):
  columns = createColumns(struct)
  createTableStatement = f"CREATE TABLE IF NOT EXISTS {regName} \
  ( {columns[:-2]} )"

  
  return createTableStatement

# COMMAND ----------

newStruct = StructType([
  StructField('ProductId', StringType(), False, {'comment': 'The Business key for a Product dimension object'}),
  StructField('ProductName', StringType(), False, {'comment': 'The name of a Product object'}),
  StructField('ProductGroup', StringType(), False, {'comment': 'The Grouping for a Product', 'columnType': 'type 3'}),
  StructField('ProductColour', LongType(), False, {'comment': 'The Colour of a Product', 'columnType': 'type 2'})
])

# COMMAND ----------

createtablestatement = createEmptyCuratedTable(struct,regName)

# COMMAND ----------

if spark.catalog.tableExists(regName) == True:
  comparisonTable = regName + '_temp'
  createtablestatement = createEmptyCuratedTable(struct,comparisonTable)
  spark.sql(createtablestatement)
else:
  createtablestatement = createEmptyCuratedTable(struct,regName)
  spark.sql(createtablestatement)

# COMMAND ----------

originalSchema = spark.sql(f"DESCRIBE {regName}").toPandas()
newSchema = spark.sql(f"DESCRIBE {regName}_temp").toPandas()

# COMMAND ----------

display(newSchema)

# COMMAND ----------

def schemaToDictionary(schema):
  schemaDict = {}
  for index, row in schema.iterrows():
    schemaDict[row['col_name']] = {"dataType":row['data_type'], "comment":row['comment'], "position":index}
  return schemaDict

# COMMAND ----------

def schemaDifference(originalSchema, newSchema):
  originalSchemaDict = schemaToDictionary(originalSchema)
  newSchemaDict = schemaToDictionary(newSchema)

  # Column present in Original but not in New
  columnsForRenameOrRemoval = dict.fromkeys((set(originalSchemaDict) - set(newSchemaDict)), '')
  for column in columnsForRenameOrRemoval:
    columnsForRenameOrRemoval[column] = originalSchemaDict[column]


  # Column present in New but not in Original
  newColumns = dict.fromkeys((set(newSchemaDict) - set(originalSchemaDict)), '')
  for column in newColumns:
    newColumns[column] = newSchemaDict[column]

  # Find data type changed in Original as compared to New
  updatedSchema = {k:v for k,v in originalSchemaDict.items() if k not in columnsForRenameOrRemoval}
  updatedSchema = {**updatedSchema, **newColumns}
  changedDataTypes = {}
  for column in updatedSchema:
    if updatedSchema[column]["dataType"] != newSchemaDict[column]["dataType"]:
      changedDataTypes[column] = newSchemaDict[column]

  # Find new comments changed in Original as compared to New
  changedComments = {}
  for column in updatedSchema:
    if updatedSchema[column]["comment"] != newSchemaDict[column]["comment"]:
      changedComments[column] = newSchemaDict[column]

  return columnsForRenameOrRemoval,newColumns, changedDataTypes, changedComments

# COMMAND ----------

columnsForRenameOrRemoval, newColumns, changedDataTypes, changedComments = schemaDifference(originalSchema, newSchema)
print(columnsForRenameOrRemoval, newColumns, changedDataTypes, changedComments)

# COMMAND ----------

def alterColumn(columns):
  for column in columns:
    alterExistingColumn = f"ALTER TABLE {regName} ALTER COLUMN {column} COMMENT '{columns[column]['comment']}' "
    print(alterExistingColumn)
    spark.sql(alterExistingColumn)

def addColumn(columns):
  for column in columns:
    addNewColumn = f"ALTER TABLE {regName} ADD COLUMN {column} {columns[column]['dataType']} COMMENT '{columns[column]['comment']}'"
    print(addNewColumn)
    spark.sql(addNewColumn)

# COMMAND ----------

if changedComments:
  alterColumn(changedComments)
if newColumns:
  addColumn(newColumns)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE default.originalTable

# COMMAND ----------

spark.sql(f"DROP TABLE {regName}_temp")

# COMMAND ----------

if spark.catalog.tableExists(regName) == True:
  comparisonTable = regName + '_temp'
  createtablestatement = createEmptyCuratedTable(newStruct,comparisonTable)
  spark.sql(createtablestatement)
else:
  createtablestatement = createEmptyCuratedTable(newStruct,regName)
  spark.sql(createtablestatement)

# COMMAND ----------

originalSchema = spark.sql(f"DESCRIBE {regName}").toPandas()
newSchema = spark.sql(f"DESCRIBE {regName}_temp").toPandas()

# COMMAND ----------

display(originalSchema)

# COMMAND ----------

display(newSchema)

# COMMAND ----------

def schemaDifference(originalSchema, newSchema):
  originalSchemaDict = schemaToDictionary(originalSchema)
  newSchemaDict = schemaToDictionary(newSchema)

  # Column present in Original but not in New
  columnsForRenameOrRemoval = dict.fromkeys((set(originalSchemaDict) - set(newSchemaDict)), '')
  for column in columnsForRenameOrRemoval:
    columnsForRenameOrRemoval[column] = originalSchemaDict[column]


  # Column present in New but not in Original
  newColumns = dict.fromkeys((set(newSchemaDict) - set(originalSchemaDict)), '')
  for column in newColumns:
    newColumns[column] = newSchemaDict[column]

  # Find data type changed in Original as compared to New
  updatedSchema = {k:v for k,v in originalSchemaDict.items() if k not in columnsForRenameOrRemoval}
  updatedSchema = {**updatedSchema, **newColumns}
  changedDataTypes = {}
  for column in updatedSchema:
    if updatedSchema[column]["dataType"] != newSchemaDict[column]["dataType"]:
      changedDataTypes[column] = newSchemaDict[column]

  # Find new comments changed in Original as compared to New
  changedComments = {}
  for column in updatedSchema:
    if updatedSchema[column]["comment"] != newSchemaDict[column]["comment"]:
      changedComments[column] = newSchemaDict[column]

  return columnsForRenameOrRemoval,newColumns, changedDataTypes, changedComments

# COMMAND ----------

columnsForRenameOrRemoval, newColumns, changedDataTypes, changedComments = schemaDifference(originalSchema, newSchema)

# COMMAND ----------

print(columnsForRenameOrRemoval)

# COMMAND ----------

print(newColumns)

# COMMAND ----------

import re
originalSchemaList = []
for key in columnsForRenameOrRemoval:
  originalSchemaList.append(key.lower())

matchedColumns = {}
for k,v in newColumns.items():
  for element in originalSchemaList:
    if re.search(element,k.lower()):
      v["originalColumn"] = element
      matchedColumns[k] = v

print(matchedColumns)

# COMMAND ----------

def dictComparison(dictionaryOne, dictionaryTwo):
  dictionary = dict.fromkeys((set(dictionaryOne) - set(dictionaryTwo)), '')
  for key in dictionary:
    dictionary[key] = dictionaryOne[key]
  
  return dictionary

# COMMAND ----------

def dictValueCamparison(dictionaryOne, dictionaryTwo, value):
  dictionary = {}
  for key in dictionaryTwo:
    if dictionaryTwo[key][f"{value}"] != dictionaryOne[key][f"{value}"]:
      dictionary[key] = dictionaryOne[key]

  return dictionary

# COMMAND ----------

import re
def renameColumns (dictionaryOne, dictionaryTwo):
  schemaList = []
  for key in dictionaryOne:
    schemaList.append(key.lower())

  matchedColumns = {}
  for k,v in dictionaryTwo.items():
    for element in schemaList:
      if re.search(element,k.lower()):
        v["originalColumn"] = element
        matchedColumns[k] = v

  return matchedColumns

# COMMAND ----------

def schemaDifference(originalSchema, newSchema):
  originalSchemaDict = schemaToDictionary(originalSchema)
  newSchemaDict = schemaToDictionary(newSchema)

  # Column present in Original but not in New
  columnsForRenameOrRemoval = dictComparison(originalSchemaDict,newSchemaDict)

  # Column present in New but not in Original
  newColumns = dictComparison(newSchemaDict,originalSchemaDict)
  
  updatedSchema = {k:v for k,v in originalSchemaDict.items() if k not in columnsForRenameOrRemoval}
  updatedSchema = {**updatedSchema, **newColumns}

  # Find data type changed in Original as compared to New
  # Data Type changing not supported in current Delta functionality. If data type has changed, column needs to be created, data migrated and old column dropped
  changedDataTypes = dictValueCamparison(newSchemaDict, updatedSchema, "dataType")
  # Find new comments changed in Original as compared to New
  changedComments = dictValueCamparison(newSchemaDict, updatedSchema, "comment")

  # Find columns that have changed name
  renamedColumns = renameColumns(columnsForRenameOrRemoval, newColumns)

  # Recalculate new columns, subtracting renamed columns
  newColumns = dictComparison(newColumns,renamedColumns)

  # Find columns that need to be dropped because they no longer exist
  # dropColumns = dictComparison(columnsForRenameOrRemoval, renamedColumns)

  return newColumns, changedDataTypes, changedComments, renamedColumns

# COMMAND ----------

 newColumns, changedDataTypes, changedComments, renamedColumns = schemaDifference(originalSchema, newSchema)
print(newColumns, changedDataTypes, changedComments, renamedColumns)

# COMMAND ----------

def alterColumn(columns, tableName):
  for column in columns:
    alterExistingColumn = f"ALTER TABLE {tableName} ALTER COLUMN {column} COMMENT '{columns[column]['comment']}' "
    print(alterExistingColumn)
    spark.sql(alterExistingColumn)

def addColumn(columns, tableName):
  for column in columns:
    addNewColumn = f"ALTER TABLE {tableName} ADD COLUMN {column} {columns[column]['dataType']} COMMENT '{columns[column]['comment']}'"
    print(addNewColumn)
    spark.sql(addNewColumn)

def renameColumn(columns, tableName):
  for column in columns:
    spark.sql(f"ALTER TABLE {tableName} SET TBLPROPERTIES ('delta.minReaderVersion' = '2','delta.minWriterVersion' = '5','delta.columnMapping.mode' = 'name')")
    renameExistingColumn = f"ALTER TABLE {tableName} RENAME COLUMN {columns[column]['originalColumn']} TO {column}"
    print(renameExistingColumn)
    spark.sql(renameExistingColumn)

# COMMAND ----------

if changedComments:
  alterColumn(changedComments, regName)
if newColumns:
  addColumn(newColumns, regName)
if renamedColumns:
  renameColumn(renamedColumns, regName)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE default.originalTable

# COMMAND ----------


