'''
The reddit preprocess part preprocess the raw data download from pushshift.io
Loop Through the YYYY-MM format, use predefined schema to reduce the storage size.
Tokenize and remove stop word, then send back to S3 in YYYY-MM dataframe
'''

'''
Only use The following code in Jupyter Notebook

import findspark
findspark.init()

import pyspark # Call this only after findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
'''

import os
import json
import configparser
from nltk.corpus import stopwords
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, from_unixtime, size

stopwords = set(stopwords.words('english'))

infilepath = 's3a://redditcom/RC_{year}-{month:02d}'
outfilepath = 's3a://redditcom/token/{year}-{month:02d}'

#Tokenize word
udf_tokenize = udf(lambda x: \
    [word \
        for word in x.lower().split() \
        if (len(word) > 3 and \
            word.isalpha() and \
            word not in stopwords)],
    ArrayType(StringType()))

#Define Reddit Schema
schema = StructType([
    StructField('archived', BooleanType()),
    StructField('author', StringType()),
    StructField('author_flair_css_class', StringType()),
    StructField('author_flair_text', StringType()),
    StructField('body', StringType()),
    StructField('controversiality', IntegerType()),
    StructField('created_utc', StringType()),
    StructField('distinguished', StringType()),
    StructField('downs', IntegerType()),
    StructField('edited', StringType()),
    StructField('gilded', IntegerType()),
    StructField('id', StringType()),
    StructField('link_id', StringType()),
    StructField('name', StringType()),
    StructField('parent_id', StringType()),
    StructField('retrieved_on', IntegerType()),
    StructField('score', IntegerType()),
    StructField('score_hidden', BooleanType()),
    StructField('subreddit', StringType()),
    StructField('subreddit_id', StringType()),
    StructField('ups', IntegerType())
])


columns_to_drop = [
    'archived', 'author_flair_css_class', 'controversiality', \
    'author_flair_text', 'distinguished', 'downs', \
    'edited', 'gilded', 'id', 'link_id', 'name', 'parent_id', \
    'removal_reason', 'retrieved_on', 'score_hidden', 'subreddit_id', 'ups'
]



if __name__ == '__main__':
    config = configparser.ConfigParser()

    config.read(os.path.expanduser('~/.aws/credentials'))
    access_id = config.get('aws', 'aws_access_key_id')
    access_key = config.get('aws', 'aws_secret_access_key')

    spark = SparkSession.builder \
            .appName("Reddit Proprocess") \
            .config("spark.executor.cores", "3") \
            .config("spark.executor.memory", "6gb") \
     .config("spark.sql.session.timeZone", "America/New_York") \
            .getOrCreate()

    sc = spark.sparkContext

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.awsAccessKeyId", access_id)
    hadoop_conf.set("fs.s3a.awsSecretAccessKey", access_key)

    for year in range(2007, 2016):
        for month in range(1, 13):
            print 'Reading {year}: {month:02d}'.format(year=year, month=month)
            rdd = sc.textFile(infilepath.format(year=year, month=month))
            df = rdd.map(json.loads).toDF(schema=schema).persist()

            df = df.drop(*columns_to_drop) \
                 .filter(df.body != '[deleted]')

            df = df.withColumn('created_utc',
                               df.created_utc.cast(IntegerType()))
            df = df.withColumn('created_utc',
                               from_unixtime(
                                   df.created_utc, format='yyyy-MM-dd'))

            df = df.withColumn('body', udf_tokenize('body')) \
                 .filter(size('body') != 0)

            df.write.json(outfilepath.format(year=year, month=month))

            spark.catalog.clearCache()
