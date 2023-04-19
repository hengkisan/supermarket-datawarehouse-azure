# Databricks notebook source
dbutils.widgets.text('p_file_date','2020-01-07')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

sales_schema = StructType(fields=[
    StructField('branch_id', IntegerType(), False),
    StructField('receipt_id', IntegerType(), False),
    StructField('product_id', IntegerType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('payment_id', IntegerType(), True),
    StructField('date', IntegerType(), True),
    StructField('items_price', DoubleType(), True)
    ])

# COMMAND ----------

df_sales_raw = spark.read.csv(f'/mnt/supermarketdl1234/raw/{v_file_date}/sales.csv', sep=';', header=True, schema=sales_schema)

# COMMAND ----------

df_sales_raw.createOrReplaceTempView('sales_raw')

# COMMAND ----------

df_sales_processed = spark.sql("select branch_id, receipt_id, date, payment_id, sum(items_price) as total_price from sales_raw GROUP BY branch_id, receipt_id, date, payment_id order by branch_id, receipt_id, date, payment_id")

# COMMAND ----------

df_sales_processed = df_sales_processed.select(
    'date', 'payment_id','total_price', 'branch_id', 'receipt_id'
)

# COMMAND ----------

spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists('supermarket_processed.sales')):
    df_sales_processed.write.mode('overwrite').insertInto('supermarket_processed.sales')
else:
    df_sales_processed.write.mode('overwrite').format('parquet').partitionBy('branch_id', 'receipt_id').saveAsTable('supermarket_processed.sales')

# COMMAND ----------



# COMMAND ----------

df_sales_details_processed = spark.sql("select branch_id, receipt_id, product_id, quantity, items_price from sales_raw")
display(df_sales_details_processed)

# COMMAND ----------

df_sales_details_processed = df_sales_details_processed.select(
   'quantity', 'items_price', 'product_id', 'branch_id', 'receipt_id'
)
display(df_sales_details_processed)

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists('supermarket_processed.sales_details')):
    df_sales_details_processed.write.mode('overwrite').insertInto('supermarket_processed.sales_details')
else:
    df_sales_details_processed.write.mode('overwrite').format('parquet').partitionBy('branch_id', 'receipt_id').saveAsTable('supermarket_processed.sales_details')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from supermarket_processed.sales_details
# MAGIC ORDER BY branch_id, receipt_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select sa.branch_id,sa.receipt_id,product_id,items_price, total_price from supermarket_processed.sales sa
# MAGIC LEFT JOIN supermarket_processed.sales_details sade
# MAGIC on sa.receipt_id = sade.receipt_id and sa.branch_id = sade.branch_id
# MAGIC order by sa.branch_id,sa.receipt_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from supermarket_processed.sales_details
# MAGIC where receipt_id > 500

# COMMAND ----------

