import os
import configparser
import socket
socket.getaddrinfo('localhost', 8080)

from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.functions import col
from snowflake.snowpark.dataframe import df_api_usage

#get Config
config = configparser.ConfigParser()
ini_path = os.path.join(os.getcwd(), 'C:\\Users\\a833122\\PycharmProjects\\SparktoSnopark\\config.ini')
config.read(ini_path)
#connect Config
sfAccount = config['snowflake']['sfAccount']
sfUsername = config['snowflake']['sfUsername']
sfPassword = config['snowflake']['sfPassword']
sfDatabase = config['snowflake']['sfDatabase']
sfWarehouse = config['snowflake']['sfWarehouse']
sfSchema = config['snowflake']['sfSchema']
print(sfAccount)
connection_parameters = {
    "account": sfAccount,
    "user": sfUsername,
    "password": sfPassword,
    "database": sfDatabase,
    "warehouse": sfWarehouse,
    "schema": sfSchema,
}
print(connection_parameters)

session = Session.builder.configs(connection_parameters).create()
session.file.put("Mall_Customers.csv", "@my_stage/Mall_Customers.csv")
#SHIPMENT_DETAILS_F_Table = session.table("SHIPMENT_DETAILS_F")
df_table = session.table("MALL_DATA")
df_filter=df_table.select('*')
df_final=df_table.filter(col("CUSTOMERID") != 'CustomerID').select(col("CUSTOMERID"),
col("GENDER"),col("AGE"),col("Annual Income (k$)").alias("AI"))
df_groupby=df_final.groupBy("GENDER","AI").agg(count(col("CUSTOMERID")).alias("custid"))
df_groupby_sort=df_groupby.sort(col("AI").desc())
df_groupby_sort.show()
df_groupby_sort.write.mode("Overwrite").saveAsTable("mall_agg_data")