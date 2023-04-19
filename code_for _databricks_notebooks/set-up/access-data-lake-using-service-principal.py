# Databricks notebook source
#hardcode secret (unsafe)
client_id = "b5a519a4-12b0-40b3-ab25-e322e8f2af0b"
tenant_id = "3e3254bd-320b-4d90-9296-8e939ca344d3"
client_secret = "Dwj8Q~i0fLxKIX7PRvuScZOJ4qLyR69OcEvllb66"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.supermarketdl1234.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.supermarketdl1234.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.supermarketdl1234.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.supermarketdl1234.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.supermarketdl1234.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls('abfss://raw@supermarketdl1234.dfs.core.windows.net/')

# COMMAND ----------

def mount_adls(storage_acc_name, container_name):
    client_id = "b5a519a4-12b0-40b3-ab25-e322e8f2af0b"
    tenant_id = "3e3254bd-320b-4d90-9296-8e939ca344d3"
    client_secret = "Dwj8Q~i0fLxKIX7PRvuScZOJ4qLyR69OcEvllb66"
    
    #set Spark confiq
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #unmount the mount point if already exist
    if any(mount.mountPoint == f'/mnt/{storage_acc_name}/{container_name}' for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f'/mnt/{storage_acc_name}/{container_name}')
    
    #mount storage account and container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_acc_name}/{container_name}",
        extra_configs = configs)
    
    #print
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('supermarketdl1234','raw')
mount_adls('supermarketdl1234','processed')
mount_adls('supermarketdl1234','presentation')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/supermarketdl1234'))

# COMMAND ----------

