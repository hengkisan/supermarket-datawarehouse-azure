-- Databricks notebook source
create database if not exists supermarket_processed
location '/mnt/supermarketdl1234/processed'

-- COMMAND ----------

desc DATABASE supermarket_processed

-- COMMAND ----------

drop database if exists supermarket_processed cascade

-- COMMAND ----------

