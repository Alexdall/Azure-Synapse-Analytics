#!/usr/bin/env python
# coding: utf-8

# ## 001_DataEngineeringManager
# 
# 
# 

# In[1]:


linkedService = "@saedwdev.dfs.core.windows.net"


# In[2]:


import pyspark.sql.functions as f
from pyspark.sql.types import *
from datetime import datetime


# In[3]:


class DataEngineeringManager(object):
  
  """Class with methods to read and write in Azure ADLS Gen2 and Synapse Datawarehouse"""
  
  def __init__(self):
    #self.__dwDatabase      = dbutils.secrets.get(scope='kvprdbigdata_secret_scope', key='databaseDw')
    #self.__dwServer        = dbutils.secrets.get(scope='kvprdbigdata_secret_scope', key='serverDw')
    #self.__dwUser          = dbutils.secrets.get(scope='kvprdbigdata_secret_scope', key='userDw')
    #self.__dwPass          = dbutils.secrets.get(scope='kvprdbigdata_secret_scope', key='passDw')
    self.__dwJdbcPort      = '1433'
    self.__dbProtocol      = 'sqlserver'
    self.__dbFormat        = 'com.databricks.spark.sqldw'
    self.__tempDir         = 'abfss://rodovias{}/_tempDir/'.format(linkedService)
    self.__container       = 'data-engineering-files'
    self.__pathPurge       = 'purge/controll/tb_purge/'
    self.__pathPurgeResult = 'purge/controll/tb_purge_result/'
    self.__tempPurgeFolder = 'purge/temp/{}'
    
    #key = dbutils.secrets.get(scope='kvprdbigdata_secret_scope', key='adlsAccountKey')
    #spark.conf.set('fs.azure.account.key.dlprdbigdatav2.dfs.core.windows.net',key)
    
  def get_url_jdbc(self):
    dwJdbcExtraOptions = 'encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'
    return f'jdbc:{self.__dbProtocol}://{self.__dwServer}:{self.__dwJdbcPort};database={self.__dwDatabase};user={self.__dwUser};password={self.__dwPass};{dwJdbcExtraOptions}'

  def getDbFormat(self):
    return  self.__dbFormat

  def getTempDir(self):
    return self.__tempDir
  
  def getContainer(self):
    return self.__container
  
  def getPathPurge(self):
    return self.__pathPurge
  
  def getPathPurgeResult(self):
    return self.__pathPurgeResult
  
  def getTempPurgeFolder(self):
    return self.__tempPurgeFolder


# In[4]:


class SynapseManager(DataEngineeringManager):
  
  """Class with methods to read and write in Synapse Datawarehouse"""
  
  def __init__(self):
    super().__init__()
  
  def write_synapse(self, schemaName, targetTable, writeMode, dataFrame = None, fragStrategy='CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN', maxStrLength='1024'):
    """
       Objective: Write the created dataframe into Azure Synapse default datawarehouse

       :schemaName: Name of the Schema
       :targetTable: table containing the data
       :writeMode: append or overwrite
       :dataframe: a dataframe to load in the synapse 
       :fragStrategy: the fragmentation strategy:
         CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN for temporary or staging tables
         CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATED for small tables
         CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH[NOME_COLUNA] for fact or big tables that uses sort or join
        
       :return: a pyspark dataframe containing the data from the synapses
        
       Example of Use:
         >>> test_df = adlsSynapse.write_synapse(schemaName='mobilidade'\
                         , targetTable='relat_falhas'
                         , writeMode='append'
                         , dataframe=df
                         , fragStrategy = 'CLUSTERED COLUMNSTORE INDEX
                         , DISTRIBUTION = HASH[NOME_COLUNA]'
                         ,maxStrLength='1024')
        
    """

    dataFrame.write\
      .format(self.getDbFormat()) \
      .option('maxStrLength', maxStrLength) \
      .option('forwardSparkAzureStorageCredentials', 'true') \
      .option('url', self.get_url_jdbc())\
      .option('dbtable',  f'{schemaName}.{targetTable}') \
      .option('tempDir', self.getTempDir()) \
      .option('tableOptions', fragStrategy)\
      .mode(writeMode) \
      .save()
        
  def read_synapse(self, schemaName, targetTable):
    """
    Objective: Read the schema.Table from the synapse default datawarehouse
    
    :schemaName: Name of the Schema
    :targetTable: table containing the data
    :return: returns a pyspark dataframe containing the data from the synapses
    
    Example of Use:
      >>> test_df = adlsSynapse.read_synapse(schemaName='dbo', targetTable='unitTest')
      >>> test_df.count() > 1
        
    """
    return spark.read\
                .format(self.getDbFormat()) \
                .format("com.databricks.spark.sqldw") \
                .option('forwardSparkAzureStorageCredentials', 'true') \
                .option('url', self.get_url_jdbc())\
                .option('dbtable',  f'{schemaName}.{targetTable}') \
                .option('tempDir', self.getTempDir()) \
                .load()


# In[6]:


class BlobManager(DataEngineeringManager):
  
  """Class with methods to manager and monitory the data lake store in Azure ADLS Gen2"""

  def __init__(self):
    super().__init__()
  
  def read_parquet(self, container, path, col_filter=None, value_filter=None, extension='*.parquet'):
    """
    Objective: Read the parquet file from ADLS Gen2
    :container: Name of the container e.g. mobilidade or rodovias
    :path: path after the container to find the file or folder e.g /raw/relat_falhas
    :col_filter: column to be used as filter (default is none) e.g partition_dt
    :value_filter: value to be used as filter (default is none) e.g '2020-10-01'
    :return: a dataframe with the parquet content
    
    Example of Use:
      >>> test_df = adlsSynapse.read_parquet(container='mobilidade', path='/raw/relat_falhas',col_filter='partition_dt',value_filter='2020-10-01')
      >>> test_df.count() > 1
    """
    
    if  col_filter and value_filter:
      return spark.read.load(f'abfss://{container}{linkedService}/{path}/{col_filter}={value_filter}', format="parquet", pathGlobFilter=extension)
    else:
      return spark.read.load(f'abfss://{container}{linkedService}/{path}', format="parquet", pathGlobFilter=extension)
      
    def read_parquet_BUs(self, container,path, col_filter=None, value_filter=None, business_units=None, bu_replace=False, extension='*.parquet'):
      """
      Objective: Read the parquet file from ADLS Gen2 and creates a dictionary with each business unit
      :container: Name of the container e.g. mobilidade or rodovias
      :path: path after the container to find the file or folder e.g /raw/relat_falhas
      :col_filter: column to be used as filter (default is none) e.g partition_dt
      :value_filter: value to be used as filter (default is none) e.g '2020-10-01'
      :business_units: an array with the required business units e.g business_units['mb','vq']
      :return: a dictionary with each business units content
      
      Example of Use:
        >>> test_df = adlsSynapse.read_parquet_BUs(container='mobilidade', path='/raw/relat_falhas',col_filter='partition_dt',value_filter='2020-10-01', business_units=business_units)
        >>> test_df.count() > 1
      """
      
      df_dict = {}
      if business_units:
        for bu in business_units:
          if not bu_replace:
            path_full = f'{path}/{bu}/'
          else:
            path_full = path.replace('%bu%', bu)
            
          parquet_directory = path_full
          df_dict[bu] = self.read_parquet(container,parquet_directory, col_filter, value_filter, extension)
          print(parquet_directory)
          print(container)
        return df_dict

  def read_delta(self, container, path, col_filter=None, value_filter=None): 
      """
      
        Objective: Read delta table from ADLS Gen2

        :container: Name of the container e.g. mobilidade or rodovias
        :path: path after the container to find the file or folder e.g /raw/relat_falhas
        :col_filter: column to be used as filter (default is none) e.g partition_dt
        :value_filter: value to be used as filter (default is none) e.g '2020-10-01'
        
        :return: a dataframe with the parquet content
        
        Example of Use:
        >>> test_df = adlsSynapse.read_parquet(container='mobilidade', path='/raw/relat_falhas',col_filter='partition_dt',value_filter='2020-10-01')
        >>> test_df.count() > 1
        
        """
      if  col_filter and value_filter: 
        self.__df = spark.read.format('delta').load(f'abfss://{container}{linkedService}/{path}/{col_filter}={value_filter}')
      else: 
        self.__df = spark.read.format('delta').load(f'abfss://{container}{linkedService}/{path}')
      return self.__df

  def write_parquet(self, container,path, dataframe=None,schema=None, partition=None, writeMode='append'):
    """
    Objective: Write a parquet file
    :container: Name of the container e.g. mobilidade or rodovias
    :path: path after the container to find the file or folder e.g /raw/relat_falhas
    :dataframe: dataframe's name (default is none) e.g df
    :partition: name of column used to partition data (default is none) e.g '2020-10-01'
    :writeMode: append or overwrite
    """
    if schema == None:
      if partition == None:
        dataframe.write\
          .mode(writeMode)\
          .parquet(f'abfss://{container}{linkedService}/{path}')
      else:
        dataframe.write\
          .partitionBy(partition)\
          .mode(writeMode)\
          .parquet(f'abfss://{container}{linkedService}/{path}')
        
    if schema:
      if partition == None:
        dataframe.write\
          .mode(writeMode)\
          .option("schema",schema)\
          .parquet(f'abfss://{container}{linkedService}/{path}')
      else:
        dataframe.write\
          .partitionBy(partition)\
          .mode(writeMode)\
          .option("schema",schema)\
          .parquet(f'abfss://{container}@{linkedService}/{path}')

  def write_parquet_coalesce(self, container,path, dataframe=None,schema=None, partition=None, writeMode='append', coalesce=1):
    """
    Objective: Write a parquet file with 'coalesce'
    :container: Name of the container e.g. mobilidade or rodovias
    :path: path after the container to find the file or folder e.g /raw/relat_falhas
    :dataframe: dataframe's name (default is none) e.g df
    :partition: name of column used to partition data (default is none) e.g '2020-10-01'
    :writeMode: append or overwrite
    """
    
    if schema == None:
      if partition == None:
        dataframe.coalesce(coalesce).write\
          .mode(writeMode)\
          .parquet(f'abfss://{container}{linkedService}/{path}')
      else:
        dataframe.coalesce(coalesce).write\
          .partitionBy(partition)\
          .mode(writeMode)\
          .parquet(f'abfss://{container}{linkedService}/{path}')
        
    if schema:
      if partition == None:
        dataframe.coalesce(coalesce).write\
          .mode(writeMode)\
          .option("schema",schema)\
          .parquet(f'abfss://{container}{linkedService}/{path}')
      else:
        dataframe.coalesce(coalesce).write\
          .partitionBy(partition)\
          .mode(writeMode)\
          .option("schema",schema)\
          .parquet(f'abfss://{container}{linkedService}/{path}')

  
  def write_delta(self, container,path, dataframe=None,schema=None, partition=None, writeMode='append'):
    """
    Objective: Write a parquet file
    :container: Name of the container e.g. mobilidade or rodovias
    :path: path after the container to find the file or folder e.g /raw/relat_falhas
    :dataframe: dataframe's name (default is none) e.g df
    :partition: name of column used to partition data (default is none) e.g '2020-10-01'
    :writeMode: append or overwrite
    """
    if schema == None:
      if partition == None:
        dataframe.write\
          .format('delta')\
          .option("mergeSchema", "true")\
          .mode(writeMode)\
          .save(f'abfss://{container}{linkedService}/{path}')
      else:
        dataframe.write\
          .format('delta')\
          .option("mergeSchema", "true")\
          .partitionBy(partition)\
          .mode(writeMode)\
          .save(f'abfss://{container}{linkedService}/{path}')
        
    if schema:
      if partition == None:
        dataframe.write\
          .format('delta')\
          .mode(writeMode)\
          .option("schema",schema)\
          .save(f'abfss://{container}{linkedService}/{path}')
      else:
        dataframe.write\
          .format('delta')\
          .partitionBy(partition)\
          .mode(writeMode)\
          .option("schema",schema)\
          .save(f'abfss://{container}@{linkedService}/{path}')

  
  def write_delta_coalesce(self, container,path, dataframe=None,schema=None, partition=None, writeMode='append', coalesce=1):
    """
    Objective: Write a parquet file with 'coalesce'
    :container: Name of the container e.g. mobilidade or rodovias
    :path: path after the container to find the file or folder e.g /raw/relat_falhas
    :dataframe: dataframe's name (default is none) e.g df
    :partition: name of column used to partition data (default is none) e.g '2020-10-01'
    :writeMode: append or overwrite
    """
    
    if schema == None:
      if partition == None:
        dataframe.coalesce(coalesce).write\
          .format('delta')\
          .mode(writeMode)\
          .save(f'abfss://{container}{linkedService}/{path}')
      else:
        dataframe.coalesce(coalesce).write\
          .format('delta')\
          .partitionBy(partition)\
          .mode(writeMode)\
          .save(f'abfss://{container}{linkedService}/{path}')
        
    if schema:
      if partition == None:
        dataframe.coalesce(coalesce).write\
          .format('delta')\
          .mode(writeMode)\
          .option("schema",schema)\
          .save(f'abfss://{container}{linkedService}/{path}')
      else:
        dataframe.coalesce(coalesce).write\
          .format('delta')\
          .partitionBy(partition)\
          .mode(writeMode)\
          .option("schema",schema)\
          .save(f'abfss://{container}{linkedService}/{path}')


  def deleteFile (self, container, path, recursive=False):
    """
    Objectiv : Remove Files from data lake
    Attention: Beware, the method can delete files, folders and subfolders
    container: Name of the container e.g. mobilidade or rodovias
    path     : path after the container to find the file or folder e.g /raw/relat_falhas
    recursive: True/Flase for delete subfolders recursively
    """
    
    dbutils.fs.rm(f'abfss://{container}{linkedService}/{path}',recursive)
    
  
  def copyFile (self, container, sourcePath, targetPath, recursive=False):
    """
    Objectiv : Move Files from data lake
    Attention: Beware, the method can move files, folders and subfolders
    container: Name of the container e.g. mobilidade or rodovias
    path     : path after the container to find the file or folder e.g /raw/relat_falhas
    recursive: True/Flase for delete subfolders recursively
    """
    
    dbutils.fs.cp(f'abfss://{container}{linkedService}/{sourcePath}',f'abfss://{container}{linkedService}/{targetPath}',recursive)
  
  
  def moveFile (self, container, sourcePath, targetPath, recursive=False):
    """
    Objectiv : Move Files from data lake
    Attention: Beware, the method can move files, folders and subfolders
    container: Name of the container e.g. mobilidade or rodovias
    path     : path after the container to find the file or folder e.g /raw/relat_falhas
    recursive: True/Flase for delete subfolders recursively
    """
    
    dbutils.fs.mv(f'abfss://{container}{linkedService}/{sourcePath}',f'abfss://{container}{linkedService}/{targetPath}',recursive)
  
    
  def recursiveDirSize(self,path):
    """
    Objectiv : Show how much a directory is taking up space on the data lake.
    path     : Full path of the directory in question e.g f'abfss://rodovias@dlprdbigdatav2.dfs.core.windows.net/raw/autoban/kcor/tauxbases/
    """
    
    total = 0
    dir_files = dbutils.fs.ls(path)
    
    for file in dir_files:
      if file.isDir():
        total += self.recursiveDirSize(file.path)
      else:
        total = file.size
    
    return total
  
  def purgeFiles(self, paths: list):
    """
    Objectiv : Purge dimension tables, keeping the most up-to-date version and its history in a single partition.
    Attention: If the parameter paths are null, the method will run the purge process for all dimension tables registered in the tb_purge.csv file.
    paths    : Directory path list to purge.
    
    Example of Use:
         dtLkManager.purgeFiles(None)
         
                  OR
                  
         paths = []
         paths.append(('rodovias','raw/autoban/kcor/tauxbases/','dt_last_atualiz'))
         paths.append(('rodovias','raw/autoban/kcor/tauxacidentes/','dt_last_atualiz'))
         
         dtLkManager.purgeFiles(paths)
    """
    
    informationList = []
    
    if paths is None or len(paths) == 0:
      dfPurge = spark.read\
                  .format("csv")\
                  .option("header","True")\
                  .option("delimiter",";")\
                  .load(f'abfss://{self.getContainer()}{linkedService}/{self.getPathPurge()}')
      
      purge = dfPurge.filter(dfPurge.table_type == 'NT').collect()
    else:
      schemaPurge = StructType([ \
        StructField("container",StringType(),True)\
        , StructField("path",StringType(),True)\
        , StructField("tb_name",StringType(),True)\
        , StructField("update_column",StringType(),True)\
        , StructField("table_type",StringType(),True)\
        , StructField("convert_date",StringType(),True)\
      ])
      
      purge = spark.createDataFrame(data=paths,schema=schemaPurge).collect()

    schemaPurgeResult = StructType([ \
      StructField("container",StringType(),True)\
      , StructField("path",StringType(),True)\
      , StructField("tb_name",StringType(),True)\
      , StructField("initial_size",FloatType(),True)\
      , StructField("final_size", FloatType(), True)\
      , StructField("size_difference", FloatType(), True)\
      , StructField("status",StringType(),True)\
      , StructField("msg_error",StringType(),True)\
    ])
    
    for row in purge:
      try:
        if row.path is not None and str(row.path).strip() !='' and str(row.path).strip() !='\\' and str(row.path).strip() !='/':

          df = self.read_parquet(row.container,row.path,None,None)
  
          self.write_parquet(self.getContainer(), self.getTempPurgeFolder().format(row.path),df,None,row.update_column,'overwrite')
          
          df = self.read_parquet(self.getContainer(), self.getTempPurgeFolder().format(row.path),None,None)
  
          initial_size = self.recursiveDirSize(f'abfss://{row.container}{linkedService}/{row.path}')
          
          if row.update_column == 'S':
            df = df.withColumn(row.update_column,f.date_format(f.from_unixtime(col(row.update_column)/1000).cast(TimestampType()),'yyyy-MM-dd'))
          
          maxDate = df.select(row.update_column).rdd.max()[0]
          
          try:
            df = df.drop(row.update_column)
          except:
            x='Nada a Fazer'
          
          df = df.drop_duplicates()
          
          df = df.withColumn(row.update_column,f.lit(maxDate))
  
          self.deleteFile(row.container,row.path,True)
          
          self.write_parquet_coalesce(row.container, row.path, df, None, row.update_column, 'overwrite')
  
          final_size = self.recursiveDirSize(f'abfss://{row.container}{linkedService}/{row.path}')
    
          if (float(initial_size) - float(final_size)) < float(0.0):
            informationList.append((row.container\
                                    ,row.path\
                                    ,row.tb_name\
                                    ,float(initial_size)\
                                    ,float(final_size)\
                                    ,float(float(initial_size) - float(final_size))\
                                    ,'NOK'\
                                    ,'Tamanho final maoir que o inicial.'))
          else:
            informationList.append((row.container\
                                    ,row.path\
                                    ,row.tb_name\
                                    ,float(initial_size)\
                                    ,float(final_size)\
                                    ,float(float(initial_size) - float(final_size))\
                                    ,'OK'\
                                    ,''))
    
          self.deleteFile(self.getContainer(), self.getTempPurgeFolder().format(row.path),True)
  
          del df
        
      except Exception as e:
        informationList.append((row.container\
                                ,row.path\
                                ,row.tb_name\
                                ,float(0.0)\
                                ,float(0.0)\
                                ,float(0.0)\
                                ,'NOK'\
                                ,str(e)))
    
    dt = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    dfPgRst = spark.createDataFrame(data=informationList,schema=schemaPurgeResult)
    dfPgRst = dfPgRst.withColumn('dt_purge',f.lit(dt))
    dfPgRst = dfPgRst.withColumn('initial_size',f.col('initial_size').cast('double'))
    dfPgRst = dfPgRst.withColumn('final_size',f.col('final_size').cast('double'))
    dfPgRst = dfPgRst.withColumn('size_difference',f.col('size_difference').cast('double'))

    self.write_parquet_coalesce(self.getContainer(), self.getPathPurgeResult(), dfPgRst, None, None, 'append')
    
    print("Purge Routine completed. For more details check the purge dashboard.")
    
  def storeHistory(self,container,path, periodKeep=24):
    """
    Objectiv:
    """
    
    return ""

