-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Load data to Delta Lake from Azure storage with COPY INTO
-- MAGIC 
-- MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Data Lake Storage Gen2 (ADLS Gen2) and Blob Storage. 
-- MAGIC 
-- MAGIC ADLS Gen2 and Blob Storage both use the ABFS driver; you can use the same patterns to connect to either of these data sources.
-- MAGIC 
-- MAGIC <!-- INSERT LINKS FOR ACCESS  -->

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a target Delta table
-- MAGIC 
-- MAGIC `COPY INTO` requires a target table created with Delta Lake. If using Databricks Runtime (DBR) 11.0 or above, you can create an empty Delta table using the command below.
-- MAGIC 
-- MAGIC When using DBR below 11.0, you'll need to specify the schema for the table during creation.
-- MAGIC 
-- MAGIC Delta Lake is the default format for all tables created in DBR 8.0 and above. When using DBR below 8.0, you'll need to add a `USING DELTA` clause to your create table statement.

-- COMMAND ----------

-- 11.0 and above
CREATE TABLE <database-name>.<table-name>;

-- 8.0 and above
-- CREATE TABLE <database-name>.<table-name>
-- (col_1 TYPE, col_2 TYPE, ...);

-- Below 8.0
-- CREATE TABLE <database-name>.<table-name>
-- (col_1 TYPE, col_2 TYPE, ...)
-- USING delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Loading data with a service principal
-- MAGIC 
-- MAGIC Users with sufficient permissions can create applications in the Azure Active Directory.
-- MAGIC 
-- MAGIC Databricks administrator can use these to create service principals for use in the Databricks workspace.
-- MAGIC 
-- MAGIC Databricks recommends securing access Azure storage by configuring service principals for clusters.
-- MAGIC * [Databricks docs: Accessing ADLS Gen2 and Blob Storage with Azure Databricks](https://docs.microsoft.com/azure/databricks/data/data-sources/azure/azure-storage)

-- COMMAND ----------

COPY INTO <database-name>.<table-name>
FROM 'abfss://container@storageAccount.dfs.core.windows.net/path/to/folder'
FILEFORMAT = CSV
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## COPY INTO with temporary credentials
-- MAGIC 
-- MAGIC `COPY INTO` also supports using temporary credentials to access data from Azure storage.
-- MAGIC * [Databricks docs: Use temporary credentials to load data with COPY INTO](https://docs.microsoft.com/azure/databricks/ingestion/copy-into/temporary-credentials)
-- MAGIC 
-- MAGIC You can use the Azure CLI to generate SAS tokens. Note that you will need proper permissions on the Azure subscription and the storage account to create SAS tokens. (If you do not have the necessary permissions, you will need to talk to your cloud administrator).
-- MAGIC * [Azure docs: Install the Azure CLI](https://docs.microsoft.com/cli/azure/install-azure-cli)
-- MAGIC * [Azure docs: Create a user delegation SAS for a container or blob with the Azure CLI](https://docs.microsoft.com/azure/storage/blobs/storage-blob-user-delegation-sas-create-cli)
-- MAGIC 
-- MAGIC For more details on using SAS tokens to grant access to Azure storage, including instructions using the Azure Portal UI, see:
-- MAGIC [Azure docs: Grant limited access to Azure Storage resources using shared access signatures (SAS)](https://docs.microsoft.com/azure/storage/common/storage-sas-overview)

-- COMMAND ----------

COPY INTO <database-name>.<table-name>
FROM 'abfss://container@storageAccount.dfs.core.windows.net/path/to/folder' WITH (
  CREDENTIAL (AZURE_SAS_TOKEN = '<sas-token>')
)
FILEFORMAT = CSV
COPY_OPTIONS ('mergeSchema' = 'true')
