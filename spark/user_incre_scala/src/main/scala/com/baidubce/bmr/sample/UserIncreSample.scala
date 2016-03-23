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

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.Tuple2
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


import org.apache.spark.{SparkConf, SparkContext}

object UserIncreSample {

    private val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    def nextMonth(cal: Calendar): String = {
      cal.add(Calendar.MONTH, 1);
      simpleDateFormat.format(cal.getTime());
    }

    def fetchKey(json: String, key: String): String = {
      compact(render(parse(json) \ key))
    }

    def unionByName(a: DataFrame, b: DataFrame): DataFrame = {
      val columns = a.columns.toSet.intersect(b.columns.toSet).map(col).toSeq
      a.select(columns: _*).unionAll(b.select(columns: _*))
    }

    def main(args: Array[String]) {

        val path = "/app/dc/spark/bce/idmapping/log/idmlogs/"
        val months = List("201508", "201509","201510","201511","201512","201601", "201602","201603")
        val sparkConf = new SparkConf().setAppName("User Increment")
        val sc = new SparkContext(sparkConf)
        val hiveContext = new HiveContext(sc)
        import hiveContext.implicits._
        import hiveContext.sql

        val tid = """"a1003""""
        val initDF = sc.emptyRDD[String].toDF()
        initDF.registerTempTable("temp_table")
        for(month <- months) {
          val oldDF = sql("SELECT * FROM temp_table")
          val uidRDD = sc.textFile(path + month + "*/*/idmapping*").filter(tid == fetchKey(_, "tid"))
            .map(fetchKey(_, "uid")).distinct().toDF()
          val newDF = unionByName(oldDF, uidRDD).distinct
          newDF.registerTempTable("temp_table")
          println(month + " user increases: " + (newDF.count() - oldDF.count()).toString)
        }
    }
}
