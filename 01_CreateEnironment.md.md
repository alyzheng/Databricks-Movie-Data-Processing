# Deploy Resources


1. To set up an environment, log into your Azure Subscription and deploy the following two resources to the same resource group:  
    - [Databricks](https://docs.azuredatabricks.net/getting-started/try-databricks.html)
    - [Azure Data Lake Gen2](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-quickstart-create-account). You can also use the default storage account that is created when deploying an Azure Databricks Workspace.  

2. Create two File Systems in your ADLS Gen2 called **delta**, **bronze** and **silver**.

3. [Create a service principle](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) and client secret so that we can mount our Azure Data Lake Gen2 to our databricks cluster. Please make note of your Azure Tenant Id as well.     

4. In your ADLS, under Access control (IAM) add a Role assignment, where the role is Storage Blob Data Contributor assigned to the service principle that was just created.  

5. [Create a cluster](https://docs.databricks.com/getting-started/quick-start.html#step-2-create-a-cluster) in your Azure Databricks workspace. 


## Mount ADLS Gen2

In our case we will be using built in datasets that Databricks provides. We will read data from DBFS and land it directly into our Bronze Delta tables that live inside an ADLS Gen2. Therefore, we will structure our DBFS as follows.   
- Delta: Delta Lake Tables. The mount path will be */mnt/delta*. 
    - Bronze: raw data
    - Silver: tabularized and saved as parquet files
    - Gold: Business and Query table i.e. transformed, aggregated, joined


1. Now create a Databricks Python Notebook called [`01_CreateEnvironment`] and attach it to the newly created cluster.  

1. Provide values for the following variables in plain text or  by using the [Azure Databricks CLI and the Secrets API](https://docs.databricks.com/user-guide/secrets/index.html).  
    ```python
    storage_account_name = ""
    storage_account_access_key = ""
    ```

1. We need to authenticate against our ADLS Gen2, so lets set some configuration that we will use in the next step.  
    ```python
    extra_configs={
      "fs.azure.account.key."+storage_account_name+".blob.core.windows.net": storage_account_access_key
    }
    ```

1. Mount our delta file system to the cluster with the following code. 
    ```python
try : 
    dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = mount_point,
    extra_configs={
      "fs.azure.account.key."+storage_account_name+".blob.core.windows.net": storage_account_access_key
    }
  )
    print("Storage Mounted.")
except Exception as e:
    if "Directory already mounted" in str(e):
        pass # Ignore error if already mounted.
    else:
        raise e
print("Success.")
    ```

Next we will set up our [landing to bronze process], in Delta Lake fashion we will implement a batch process and a stream process.  
