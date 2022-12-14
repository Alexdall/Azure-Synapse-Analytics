# Azure-Synapse-Analytics
Azure Synapse Analytics Implementations

* **SynapseAnalytics_PL_Start_SQLPool.json**
  * Verify Dedicated SQL Pool via Rest API and start it if paused.
* **SynapseAnalytics_PL_Pause_SQLPool.json**
  * Verify Dedicated SQL Pool via Rest API and pause it if started.
* **001_DataEngineeringManager.py**
  * Classes and methods to write and read data lake files and database tables
* **Trusted.py**
  * Dynamically handles .parquet files, transforms them into delta tables and backs up source files to a processed directory
* **PL_SourceTORawZone_Full_tables.json**
  * Dynamic pipeline to ingest full tables with no delta
* **PL_RawZoneTOTrustedZone.json**
  * Dynamic pipeline to handle parquet files and create delta tables. It uses the Trusted.py notebook.
* **PL_RefinedZonetoDW.json**
  * Dynamic pipeline to save tables from refined zone into SQL Pool.
