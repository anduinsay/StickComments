#! /usr/bin/env python

"""
This process the computation part for gaming channels, the input is subreddit channel and month

First, need to setup SparkSession, second read data from S3
then filter dataframe, then collect model results to single DF
last call Luigi Task to write it to postgres

"""

import os
import configparser
import datetime
import argparse
from dateutil.parser import parse
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, to_date, lit, datediff, udf, struct
from pyspark.sql.types import *
from itertools import chain


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))

    dbuser = config.get('db', 'user')
    dbpwd = config.get('db', 'password')
    dbip= config.get('db', 'ip')
    dbport = config.get('db', 'port')
    dbname = config.get('db', 'database')
    access_id = config.get('aws', 'aws_access_key_id') 
    access_key = config.get('aws', 'aws_secret_access_key')

    dburl = 'jdbc:postgresql://{ip}:{port}/{database}'.format(ip=dbip,
                                                              port=dbport,
                                                              database=dbname)

    # Parse result from SparkSubmitTask 
    parser = argparse.ArgumentParser()
    parser.add_argument('subreddit')
    parser.add_argument('month')
    args = parser.parse_args()

    subreddit = args.subreddit
    month = args.month
    year = month.split('-')[0]

    spark = SparkSession.builder \
            .appName('Count Comments') \
            .config('spark.task.maxFailures', '20') \
            .config('spark.executor.cores', '3') \
            .config('spark.executor.memory', '6gb') \
	        .config('spark.dynamicAllocation.enabled', False) \
     	    .config('spark.sql.session.timeZone', 'America/New_York') \
            .getOrCreate()

    sc = spark.sparkContext

    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    hadoop_conf.set('fs.s3a.awsAccessKeyId', access_id)
    hadoop_conf.set('fs.s3a.awsSecretAccessKey', access_key)

    infilepath = 's3a://redditcom/token/{year}-{month:02d}'
    outfilepath = 's3a://redditcom/result/{subreddit/{month:02d}'
    cols = ['subreddit', 'month', 'user', 'comments']
    
    # Get and filter data 
    df = spark.read.json(infilepath.format(year=year, month=month))
    subreddit_df = df.filter(df.subreddit == subreddit)
    tokens = subreddit_df.filter(df.select('author'))
    

    num_c = tokens.count()
    num_a = tokens.distinct().count()
    if num_a >= 1:
        
        val = Row(subreddit=subreddit, month = month \
                  user = num_a, comments = num_c)
        line = (subreddit, month, user, comments)
        month_df = spark.createDataFrame([line], cols)

        week_df.write.jdbc(
            dburl, 'redditres',
            mode='append',
            properties={'user': dbuser, 'password': dbpwd}
        )

