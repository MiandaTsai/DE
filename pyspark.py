#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pprint import pprint 
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import time


# change configurations (15,16)
conf = SparkConf().setAppName("botSparkStreaming")
conf = SparkConf().setAll([('spark.kryoserializer.buffer.max','1024m')('spark.executor.memory', '8g'), ('spark.executor.cores', '3'), ('spark.cores.max', '4'), ('spark.driver.memory','8g'), ('spark.yarn.executor.memoryOverhead','2048')])
sc.stop()
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 5) 

# create HBase table before running
table = 'bot'
broker = "quickstart.cloudera:9092"

# create Kafka topic befoe running
topic = "bot1"

# HBASE configurations
hbaseZK = "quickstart.cloudera"
keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
hbaseConf = {"hbase.zookeeper.quorum": hbaseZK, "hbase.mapred.outputtable": table,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
 
# log
def log(str):
    t = time.strftime(r"%Y-%m-%d %H:%M:%S", time.localtime())
    print("[%s]%s" % (t, str))
 
# prepare RDD with dictionard format
def fmt_data(msg_dict):
    if msg_dict is not None:
        # rowkey = str(msg_dict['module'])+'-'+msg_dict['date']
        rowkey = msg_dict[0]
        put_list = []
        # for k, v in msg_dict.items():
        for k, v in msg_dict[1].items():
            col_name = k
            col_value = str(v)
            col_family = 'blog'
            # conver dictionary to tuple (rowkey, [row key, column family, column name, value]) for HBase put
            msg_tuple = (rowkey, [rowkey, col_family, col_name, col_value])
            print("rowkey:" + rowkey + "\ndata " + str(msg_tuple) + " append success")
            put_list.append(msg_tuple)
    return put_list
 
#transfer RDD to HBASE
def connectAndWrite(data):
    if not data.isEmpty():
        # deserialize json in RDD (None,[json]) to dictionary
        # msg_list = data.map(lambda x: json.loads(x[1]))
        msg_list = data.map(lambda x: (x[0], json.loads(x[1])))
        # log for check
        log(msg_list.collect())
        try:
            # convert dictionary to tuple for HBase put
            msg_row = msg_list.map(lambda x: fmt_data(x))
            # print(msg_row.flatMap(lambda x: x).map(lambda x: x).collect())
            # flatmap RDD then save in HBase
            msg_row.flatMap(lambda x: x).map(lambda x: x).saveAsNewAPIHadoopDataset(conf=hbaseConf, keyConverter=keyConv,valueConverter=valueConv)
        except Exception as ex:
            print(str(ex) + " put data fail")
 
 
kafkaStreams = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams={"metadata.broker.list": broker})
kafkaStreams.pprint()
kafkaStreams.foreachRDD(connectAndWrite)
 
 
log('start consumer')
ssc.start()
ssc.awaitTermination()