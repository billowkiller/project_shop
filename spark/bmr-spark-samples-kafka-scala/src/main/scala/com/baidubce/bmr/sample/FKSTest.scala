/*
 * Copyright (C) 2015 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidubce.bmr.sample

import java.util.HashMap
import java.text.SimpleDateFormat
import java.util.{Locale, Date}
import java.net._
import scala.util.parsing.json.JSON
import scala.io.Source

import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import org.apache.hadoop.hbase.client.{Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

/**
 * 
 * Consumes messages from one or more topics in Kafka and perform ETL.
 * Usage: FKSTest <master> <group> <topics> <numThreads>
 *   <master> is the hostname of the cluster master
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 * 
 * NOTE: KafkaUtils.createStream use kafka zk seperated by comma, and in BMR
 *  there are at least 3 zk servers working. We simplfy the process using only
 *  one zk, but in the production environment you'd better utilize all the zks.
 */

object FKSTest {

  private val parser = new LogParser
  private val logDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US)
  private val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  private val nowDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  def fetchDate(GMSTime: String): String = {
      simpleDateFormat.format(logDateFormat.parse(GMSTime))
  }
  def fetchNow(): String = {
      nowDateFormat.format(new java.util.Date())
  }
  
  def createConf(master: String, table: String): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    conf.set("hbase.zookeeper.quorum", master + ":2181")
    conf.set("hbase.master", master + ":16000")
    conf.set("hbase.rootdir", "hdfs://" + master + ":8020/apps/hbase/data")

    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.job.output.key.class", classOf[Text].getName)
    jobConf.set("mapreduce.job.output.value.class", classOf[LongWritable].getName)
    jobConf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)
    jobConf
  }

  // prepare hbase table
  def setup(master: String): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", master + ":2181")
    conf.set("hbase.master", master + ":16000")
    conf.set("hbase.rootdir", "hdfs://" + master + ":8020/apps/hbase/data")
    val admin=new HBaseAdmin(conf)

    def createTable(tableName: String, family: String): Unit = {
      val userTable = TableName.valueOf(tableName)
      val tableDescr = new HTableDescriptor(userTable)
      tableDescr.addFamily(new HColumnDescriptor(family.getBytes))
      if (!admin.tableExists(userTable)) {
        admin.createTable(tableDescr)
      } 
    }

    createTable("PVUV", "statistics")
    createTable("GeoLocation", "location")
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: FKSTest <master> <group> <topics> <numThreads>")
      System.exit(1)
    }
    
    val Array(master, group, topics, numThreads) = args
    setup(master)

    val sparkConf = new SparkConf().setAppName("FKSTest")
    // do calculation every 10 seconds
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("checkpoint")
    val treemapVar = ssc.sparkContext.broadcast(
        Utils.prepareIPDB("http://bmr.bj.bcebos.com/data/ip.db"))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val logs = KafkaUtils.createStream(ssc, master + ":2181", group, topicMap)
                .map(s => parser.parseRecord(s._2)).filter(_.remoteAddr != "").cache()

    /* pv calculation */
    val PV = logs.map(s => (fetchDate(s.timeLocal), 1))
        .reduceByKey(_ + _).map(s => (fetchNow(), s._2))

    PV.foreachRDD ( rdd => {
      rdd.filter(_._1.length() > 0).map(s => convert("statistics", "PV", s))
          .saveAsNewAPIHadoopDataset(createConf(master, "PVUV"))
    })

    /* uv calculation */
    val UV = logs.map(s => (fetchDate(s.timeLocal), s.remoteAddr))
      .groupByKey().map(s => (fetchNow(), s._2.toSet.size))

    UV.foreachRDD ( rdd => {
      rdd.filter(_._1.length() > 0).map(s => convert("statistics", "UV", s))
          .saveAsNewAPIHadoopDataset(createConf(master, "PVUV"))
    })

    /* location aggregating */
    val location = logs.map(s => (Utils.fetchLocByIP(treemapVar.value, s.remoteAddr), 1)).reduceByKey(_ + _)
    
    location.foreachRDD ( rdd => {
      val temp = rdd.filter(_._1.length() > 0).map(s => s._1 + ":" + s._2).collect().mkString(";")
      if (temp.length > 0) {
        val table = new HTable(createConf(master, "GeoLocation"), TableName.valueOf("GeoLocation"))
        table.put(convertString("location", "aggregation", temp))
        table.close()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def convert(f: String, c: String, t: (String, Int)) = {
    val ts = (System.currentTimeMillis()/10000 * 10).toString
    val rowkey = Utils.getMD5(ts) + ts
    val p = new Put(Bytes.toBytes(rowkey))
    p.add(Bytes.toBytes(f), Bytes.toBytes(c), Bytes.toBytes(t._2))
    (rowkey, p)
  }

  def convertString(f: String, c: String, t: String) = {
    val ts = (System.currentTimeMillis()/10000 * 10).toString
    val p = new Put(Bytes.toBytes(Utils.getMD5(ts) + ts))
    p.add(Bytes.toBytes(f), Bytes.toBytes(c), Bytes.toBytes(t))
    p
  }
}
