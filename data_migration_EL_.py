# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 13:04:09 2023

@author: PKG
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, coalesce
from pyspark.sql import SQLContext
from pyspark.sql import Row
import time
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType, FloatType, DoubleType, TimestampType
from pyspark.sql.functions import col

# Part 1: Aurora Postgres Table to SQL server RDS
args = getResolvedOptions(sys.argv, ['source_conn_url', 'source_conn_db', 'source_conn_user', 'source_conn_pwd', 'target_conn_url', 'target_conn_db', 'target_conn_user', 'target_conn_pwd', 'target_conn_url_red', 'target_conn_db_red', 'target_conn_user_red', 'target_conn_pwd_red' ,'redshift_iam_role'])

# Aurora Postgres
source_conn = {
    "url": args['source_conn_url'],
    "db": args['source_conn_db']
}
source_db_properties = {
    "username": args['source_conn_user'],
    "password": args['source_conn_pwd']
}

# SQL server RDS
target_conn = {
    "url": args['target_conn_url'],
    "db": args['target_conn_db']
}

target_db_properties = {
    "username": args['target_conn_user'],
    "password": args['target_conn_pwd']
}

# Redshift Cluster
target_conn_red = {
    "url": args['target_conn_url_red'],
    "db": args['target_conn_db_red']
}

target_db_properties_red = {
    "username": args['target_conn_user_red'],
    "password": args['target_conn_pwd_red']
}

def extract_FULL_sourcedata(spark, source_table_name, target_table_name):
    
    # Reading data from Source : Aurora Postgres
    df1 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://"+source_conn["url"]+":5432/"+source_conn["db"]) \
        .option("dbtable", source_table_name) \
        .option("user", source_db_properties["username"]) \
        .option("password", source_db_properties["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    print("Record Count at Source Aurora Postgres table : ",source_table_name," = ",df1.count())
    
    # Reading data from Target : SQL server RDS
    df2 = spark.read \
        .format("jdbc") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("url","jdbc:sqlserver://;serverName="+target_conn["url"]+";DATABASE="+target_conn["db"]) \
        .option("user",target_db_properties["username"]) \
        .option("password",target_db_properties["password"]) \
        .option("dbtable",target_table_name) \
        .load() 
    print("Record Count at Target SQL server RDS table : ",target_table_name," = ",df2.count())
    
    # Reading data from Target: Redshift
    df3 = glueContext.read.format("jdbc") \
        .option("url","jdbc:redshift://" + target_conn_red["url"] + ":5439/" + target_conn_red["db"]) \
        .option("user", target_db_properties_red["username"]) \
        .option("password", target_db_properties_red["password"]) \
        .option("driver", "com.amazon.redshift.jdbc.Driver") \
        .option("dbtable", target_table_name) \
        .option("aws_iam_role", args['redshift_iam_role']) \
        .option("tempdir", "s3://bucket_name/temp/") \
        .load()
    
    # --------------------------------------------------------------------------------------------
    # get the column names and datatypes from SQL Server
    sqlserver_columns = df2.dtypes

    # iterate over the Redshift columns and cast the datatype if the name matches a SQL Server column
    for col_name, col_type in df1.dtypes:
        for sqlserver_col_name, sqlserver_col_type in sqlserver_columns:
            if col_name == sqlserver_col_name:
                df1 = df1.withColumn(col_name, df1[col_name].cast(sqlserver_col_type))
     
    # get the column names and datatypes from Redshift
    redshift_columns = df3.dtypes
    # iterate over the Redshift columns and cast the datatype if the name matches a SQL Server column
    for col_name, col_type in df1.dtypes:
        for redshift_col_name, redshift_col_type in redshift_columns:
            if col_name == redshift_col_name:
                df1 = df1.withColumn(col_name, df1[col_name].cast(redshift_col_type))           
     
    # --------------------------------------------------------------------------------------------    
    # Elements of df1 (Aurora Postgres) not in df2 (SQL server RDS)
    df_final_1 = df1.exceptAll(df2)
    print("Record Count after exceptAll Operation for table : ",target_table_name," = ",df_final_1.count())
    
    # Elements of df1 (Aurora Postgres) not in df3 (Redshift Cluster)
    df_final_2 = df1.exceptAll(df3)
    print("Record Count after exceptAll Operation for table : ",target_table_name," : ",df_final_2.count())
    
    # --------------------------------------------------------------------------------------------
    # Writing data to Target : SQL server RDS
    if len(df_final_1.head(1)) > 0:
        df_final_1.write \
            .mode("append")\
            .format("jdbc") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .option("url","jdbc:sqlserver://;serverName="+target_conn["url"]+";DATABASE="+target_conn["db"]) \
            .option("user",target_db_properties["username"]) \
            .option("password",target_db_properties["password"]) \
            .option("dbtable",target_table_name) \
            .save()
        print("SQL Server- Writing Done")
    else:
        print("No new Records at Source")
    
    # Writing data to Target : SQL server RDS
    if len(df_final_2.head(1)) > 0:
        df_final_2.write \
            .format("com.databricks.spark.redshift") \
            .option("url", "jdbc:redshift://" + target_conn_red["url"] + ":5439/" + target_conn_red["db"]) \
            .option("user", target_db_properties_red["username"]).option("password",target_db_properties_red["password"]) \
            .option("dbtable", target_table_name) \
            .option("tempdir", "s3://bucket_name/temp/") \
            .option("aws_iam_role", args['redshift_iam_role']) \
            .mode("append") \
            .save()
        print("Redshift- Writing Done")
    else:
        print("No new Records at Source")
    
if __name__ == "__main__":
    glueContext = GlueContext(SparkContext.getOrCreate())
    glueJob = Job(glueContext)
    spark = glueContext.sparkSession
    
    source_table_names = ['schema.table1', 'schema.table2', 'schema.table3', 'schema.table4']
    target_table_names = ['schema.table1', 'schema.table2', 'schema.table3', 'schema.table4']
    
    for i in range(len(source_table_names)):
        extract_FULL_sourcedata(spark, source_table_names[i], target_table_names[i])

    glueJob.commit()
