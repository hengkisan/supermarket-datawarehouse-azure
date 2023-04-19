## Supermarket Data Warehousing Using Azure (Databricks, Data Factory, Data Lake Gen 2)

In this project, Azure resources are used to create data warehouse for supermarket sales. The proposed data warehouse schema can be described as below:
<p align="center">
  <img src="https://user-images.githubusercontent.com/122197570/233065797-6caf1bb3-1d1f-4f97-aff4-ba219e03ab31.png">
</p>

Currently, branches, payment, date_dim and products tables are obtained from various sources then they are being processed into clean data and uploaded to Azure Data Lake. Sales and sales_details tables are derived from raw source that resembles the output of Point of Sales (POS) software used by supermarkets. This raw source however is artificially created.
<p align="center">
  <img src="https://user-images.githubusercontent.com/122197570/233066093-3591927d-75d3-42c6-adb9-df7378f2cafd.png">
</p>

Databricks Pyspark engine is used to transform raw source into the desired target tables (the code is provided on the following repository). The code allows the table to read data from various folder by inserting the folder name on **p_file_date**.
<p align="center">
  <img src="https://user-images.githubusercontent.com/122197570/233069485-792c11fd-0e78-485c-be9e-a3977c717273.png">
</p>

The folders are stored on container called raw. As the sales table is assumed to be updated weekly, the folders inside **raw** container are named according to the last date of the week (eg. 2020-01-07 folder contains sales data from 2020-01-01 to 2020-01-07). In case there are wrong data inputs, all rows with the same receipt_id should be inputted on the next pipeline run as the sales table is partitioned by branch_id and receipt_id.
<p align="center">
  <img src="https://user-images.githubusercontent.com/122197570/233069718-05e9d075-1cd7-48a9-a900-cb7313acbe41.png">
</p>

Azure Data Factory is used for pipeline scheduling and orchestration in this project. Get Metadata activity and If Condition are used to check whether the specified folder exists. If it exists, Data Factory will execute Databricks Notebook and transform raw source data to sales and sales_details tables. Otherwise, it will do nothing and will not throw any error because of the folder absence.
<p align="center">
  <img src="https://user-images.githubusercontent.com/122197570/233069808-a8f8a5e6-5ee9-4c0c-8f54-af7c46d65fe2.png">
</p>

Tumbling window trigger is chosen with the following setup. With this trigger type, End Window Tumbling Date (in this case, the last date of the week) can be inputted as the parameter for the pipeline. 
<p align="center">
  <img src="https://user-images.githubusercontent.com/122197570/233069867-de78e8fa-4e92-4530-abad-107ee6c4d322.png">
</p>
