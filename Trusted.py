#!/usr/bin/env python
# coding: utf-8

# ## Trusted
# 
# 
# 

# In[85]:


Container = ""
Zone = ""
highway = ""
System = ""
SchemaName = ""
TableName = ""
PrimaryKey = ""
Partition = ""
UpdatedDate = ""
FlagDelete = ""
PartitionColumn =""
linkedService = ""


# In[86]:


#Container = "rodovias"
#Zone = "trusted"
#highway = "autoban"
#System = "suat"
#SchemaName = "avinew"
#TableName = "receita_trafego"
#PrimaryKey = "IDPRESTACAO/CDPAIS/CDCONCESS/NOPRACA/DTRECEITA/CDCATEGORIA/TPRECEITA/NOPISTA"
#Partition = "1"
#UpdatedDate = "2022-06-08"
#FlagDelete = "1"
#v_error = 0


# In[87]:


import json
import pyspark.sql.functions as F
from delta.tables import *
from datetime import *


# In[88]:


rawPath = 'abfss://{}{}/raw/{}/{}/{}/{}/'.format(Container,linkedService,highway,System,SchemaName,TableName)
#bkpPath = 'abfss://{}@saedwdev.dfs.core.windows.net/raw_processados/{}/{}/{}/{}/'.format(Container,highway,System,SchemaName,TableName)
trustedPath = 'abfss://{}{}/{}/{}/{}/{}/{}/'.format(Container,linkedService,Zone,highway,System,SchemaName,TableName)
failedPath = 'abfss://{}{}/failed/{}/{}/{}/{}/'.format(Container,linkedService,highway,System,SchemaName,TableName)
v_error = 0


# In[89]:


def RemoveFile(filePath):
  """
     Removes a specific file
     
       Parameters:
          - sink (String): The processing zone file path
  """
    
  mssparkutils.fs.rm(filePath)


# In[90]:


def RemoveFiles(filePath, recursive):
  """
     Removes all files in a specific directory
     
       Parameters:
          - filePath (String): Thae processing zone file path
          - recurse (Boolean): Use the True or False parameter to delete files recursively or not 
  """
  
  mssparkutils.fs.rm(filePath,recurse=recursive)


# In[91]:


def CopyFiles(source, sink):
  """
     Move a specific file into zones

       Parameters:
          - source (String): The origin file path
          - sink (String): The destiny file path
  """
  mssparkutils.fs.cp(source, sink)


# In[92]:


def MoveFiles(source, sink, overwrite):
  """
     Move a specific file into zones

       Parameters:
          - source (String): The origin file path
          - sink (String): The destiny file path
  """
  mssparkutils.fs.mv(source, sink,True,overwrite = overwrite)


# In[93]:


def CheckingPathExists(path):
  """
    Verify if the path is valid, if is valid, return True
    If is not valid, return False
    And if have another error, return the error
    
      Parameters:
        - path (String): The file path
  """
    
  try:
    mssparkutils.fs.ls(path)
    return True
  except Exception as e:
    if 'The specified path does not exist' in str(e):
      return False
    else:
      raise


# In[72]:


def CheckingFoldesAsCreated(path):
  """
    Call the function CheckingPathExists to verify if the path is valid
    If the path is not valid, recreate the path
    
      Parameters:
        - path (String): The file path
  """
  
  if (CheckingPathExists(path) == False):
    mssparkutils.fs.mkdirs(path)
    return True
  else:
    return True


# In[73]:


def ConvertParquetTODelta_OLD(rawPath):
  """
     Load the processing files to generate Data Frame and the Schema of the Data Frame
     Replace the empty fields to None
     Write the delta table into the right location
     
        Parameters:
          - rawPath (String): The processing zone file path
  """
  
  dataFrame = spark.read.format("parquet").load(rawPath)
  dataFrame = dataFrame.replace('',None)
  dataFrame.write.format("delta").save(rawPath + "delta/")


# In[74]:


def PartitionParquet(v_error,Partition):

  if v_error == 1 and Partition == "1":
      #print('entrou')
      df = spark.read.format('delta').load(trustedPath)
      df = df.withColumn('PARTITION_DT', F.date_format(PartitionColumn,'yyyy-MM'))
  #display(df)
      df.write.format("delta") \
                 .partitionBy("PARTITION_DT") \
                 .option("mergeSchema","true") \
                 .option("overwriteSchema", "true") \
                 .mode("overwrite") \
                 .save(trustedPath)


# In[75]:


def DeleteDelta(trustedPath,UpdatedDate,FlagDelete):
    
    if FlagDelete == "1":

        try:
            trustedTable = spark.read.format('delta').load(trustedPath)
            trustedTable.createOrReplaceTempView('trustedTableView')
            sql = "delete from {} old where {} >= date_add('{}',-35) ".format('trustedTableView',PartitionColumn,UpdatedDate)
            spark.sql(sql)
            #trustedTableView.unpersist()
        
        except Exception as e:
            if 'is not a Delta Table' in str(e):
                pass


# In[76]:


def ConvertParquetTODelta(rawPath, file):
  """
     Load the processing files to generate Data Frame and the Schema of the Data Frame
     Replace the empty fields to None
     Write the delta table into the right location
     
        Parameters:
          - rawPath (String): The processing zone file path
  """
  
  dataFrame = spark.read.format("parquet").load(rawPath+file)
  dataFrame = dataFrame.replace('',None)
  dataFrame.write.format("delta").save(rawPath + "delta/")


# In[77]:


def MergeParquetToTrusted(rawPath, trustedPath, PrimaryKey, Partition):
  """
      Import delta and json libs
      Split the keys into a list
      Dynamically create join keys to use for merging
      Load the Spark and the Delta DataFrames
      Generate the list of columns form the source file (raw zone)
      Declare local variables to use within upsert commands
      Dynamically generate the update and insert script to update or not the InsertedDate and UpdatedDate
      Generate a dictionary whit the json lib for local variables sqlStringUpdate and sqlStringInsert to use into the merge command
      If the merge fails because there is no Delta Table location, the Delta files from Processing are moved to Bronze

        Parameters:
          - trustedPath (String): The Trusted zone file path
          - rawPath (String): The Raw zone file path
          - key (string): The entity primary key (PK) concatenated with slashes
          - Partition (String): Validates if delta file creation will be partitioned
  """  

  try:
    global v_error
    KeyList = PrimaryKey.split('/')
    sqlStringKey = ''
    keyDict = list(PrimaryKey.split('/'))
    upper_dict = {}     

    dataFrame = spark.read.format("delta").load(rawPath)
    #dataFrame.createOrReplaceTempView('dataFrameView')
    trustedTable = DeltaTable.forPath(spark, trustedPath)

   

    #dataFrameView.unpersist()
    
    DataFrameSchemaRaw = dict(dataFrame.dtypes)
    listColumns = dataFrame.columns
    ColumnsList = list(listColumns)

    sqlStringColumns, sqlStringUpdate, sqlStringInsert = '', '', ''
    UpdateColumn, UpdateType = '', ''

    for k, v in DataFrameSchemaRaw.items():
      if isinstance(v, dict):
        v = _uppercase_for_dict_keys(v)
      upper_dict[k.upper()] = v

    for x in keyDict:
      if all(x in sub for sub in [upper_dict, keyDict]):
        UpdateColumnAux, UpdateTypeAux = x + '/', upper_dict[x] + '/'
        UpdateColumn = UpdateColumn + UpdateColumnAux
        UpdateType = UpdateType + UpdateTypeAux

    UpdateColumn, UpdateType = UpdateColumn[:-1], UpdateType[:-1]
    UpdateColumnList, UpdateTypeList = UpdateColumn.split('/'), str(UpdateType).replace(',','.').split('/')

    for x in range (0,len(PrimaryKey.split('/'))): 
      if UpdateTypeList[x] == 'boolean':
        sqlStringKeyAux = "NVL(trustedTable.{},False) = NVL(dataFrame.{},False) AND ".format(UpdateColumnList[x],UpdateColumnList[x])
        sqlStringKey = sqlStringKey + sqlStringKeyAux
      else:
        sqlStringKeyAux = "NVL(trustedTable.{},'') = NVL(dataFrame.{},'') AND ".format(UpdateColumnList[x],UpdateColumnList[x])
        sqlStringKey = sqlStringKey + sqlStringKeyAux

    sqlStringKey = sqlStringKey[:-4]

    for x in range (0,len(ColumnsList)):
      sqlStringColumnsAux = '"{}": "dataFrame.{}", '.format(ColumnsList[x],ColumnsList[x])
      sqlStringColumns = sqlStringColumns + sqlStringColumnsAux

    sqlStringColumns = "{" + sqlStringColumns[:-2] + "}"
    sqlStringUpdate = sqlStringColumns.replace("dataFrame.InsertedDate","NULL") 
    sqlStringInsert = sqlStringColumns.replace("dataFrame.UpdatedDate","NULL")

    sqlStringUpdate = json.loads(sqlStringUpdate)
    sqlStringInsert = json.loads(sqlStringInsert)

    #dfDel = trustedTable.alias("trustedTable").join(dataFrame.alias("dataFrame"),sqlStringKey,"left_anti")
    #dfDel.createOrReplaceTempView('dfDel')
    #spark.sql('')
  
 #   try:
    trustedTable.alias("trustedTable").merge(
        dataFrame.alias("dataFrame"),
        sqlStringKey) \
     .whenMatchedUpdate(set = sqlStringUpdate) \
     .whenNotMatchedInsert(values = sqlStringInsert) \
     .execute()
    v_error = 1
    PartitionParquet(v_error,Partition)
 #   except Exception as e:
 #     v_error = e

  except Exception as e:
    if 'is not a Delta table' in str(e) and Partition == "1":
        v_error = 'entrou primeiro if'
#       dataFrame.write.format("parquet").partitionBy("partition_dt").option("mergeSchema","true").option("overwriteSchema", "true").mode("overwrite").save(trustedPath)
        dataFrame = dataFrame.withColumn("PARTITION_DT", F.date_format(PartitionColumn,'yyyy-MM'))
        dataFrame.write.format("delta") \
               .partitionBy("PARTITION_DT") \
               .option("mergeSchema","true") \
               .option("overwriteSchema", "true") \
               .mode("overwrite") \
               .save(trustedPath)
    elif Partition != "1":
      v_error = 'entrou segundo if'
      dataFrame.write.format("delta") \
               .option("mergeSchema","true") \
               .option("overwriteSchema", "true") \
               .mode("overwrite") \
               .save(trustedPath)
    else:
      raise
  return(v_error)
  


# In[78]:


try:
  #now = datetime.now()
  existe = mssparkutils.fs.ls(rawPath)
  for i in existe:
    if i.name == 'delta':
      RemoveFiles(rawPath + "delta/",True) ### --> movido para antes do for
  files = mssparkutils.fs.ls(rawPath) #Lista todos os arquivos da origem
  for i in range (0, len(files)): #Cria um looping de acordo com a quantidade de arquivos da origem
    if files[i].size == 0:
     CopyFiles(files[i].path,failedPath)
     RemoveFile(files[i].path)
     i +=1
    file = files[i].name #Pega o arquivo da posição '0' na origem
    CheckingFoldesAsCreated(rawPath + "delta/")
    RemoveFiles(rawPath + "delta/",True)
    ConvertParquetTODelta(rawPath,file)
    DeleteDelta(trustedPath,UpdatedDate,FlagDelete)
    v_error = MergeParquetToTrusted(rawPath + "delta/", trustedPath, PrimaryKey, Partition) #Realiza o merge da processing com a Delta Table, passando as respectivas 
    CopyFiles(files[i].path, files[i].path.replace('raw','raw_processados'))
    RemoveFile(files[i].path)
    ##MoveFiles(files[i].path, files[i].path.replace('raw','raw_processados'), True)
    RemoveFiles(rawPath + "delta/",True)
    print(v_error)
#    print(files[i].name)

except Exception as e:
  #return(str(e))
  out = {
    "status" : "failed",
    "error" : str(e),
    "FilePath" : '{}/{}/{}/{}/{}/'.format(Zone,highway,System,SchemaName,TableName)
  }
  mssparkutils.notebook.exit(json.dumps(out))

out = json.dumps({
"status" : "succeeded",
"FilePath" : '{}/{}/{}/{}/{}/'.format(Zone,highway,System,SchemaName,TableName)})

mssparkutils.notebook.exit(out)

