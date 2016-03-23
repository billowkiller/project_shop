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
package com.baidubce.bmr.sample;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.Tuple2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;

public final class AccessLogStatsJavaSample {
    private static SimpleDateFormat logDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

    private static String fetchDate(String gmsTime) {
        String date;
        try {
            date = simpleDateFormat.format(logDateFormat.parse(gmsTime));
        } catch (Exception e) {
            date = null;
        }
        return date;
    }

    private static final Pattern LOGPATTERN = Pattern.compile(
        "(\\S+)\\s+-\\s+\\[(.*?)\\]\\s+\"(.*?)\"\\s+(\\d{3})\\s+(\\S+)\\s+"
                + "\"(.*?)\"\\s+\"(.*?)\"\\s+(.*?)\\s+\"(.*?)\"\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)");

    private static Tuple2<String, String> extractKey(String line) {
        Matcher m = LOGPATTERN.matcher(line);
        if (m.find()) {
            String addrIp = m.group(1);
            String date = fetchDate(m.group(2));
            return new Tuple2<String, String>(date, addrIp);
        }
        return new Tuple2<String, String>(null, null);
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: spark-submit com.baidubce.bmr.sample.AccessLogStatsJavaSample <input>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("AccessLogStatsJavaSample");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse the log to LogRecord and cache
        JavaPairRDD<String, String> distFile = sc.textFile(args[0]).mapToPair(
                new PairFunction<String, String, String>() {
                @Override
                public Tuple2<String, String> call(String s) {
                    return extractKey(s);
                }
            });
        distFile.cache();

        /*
         * change the LogRecord to (date, 1) format, and caculate each day's page view
         */
        System.out.println("------PV------");
        JavaPairRDD<String, Integer> pv = distFile.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<String, String> tuple) {
                    return new Tuple2<String, Integer>(tuple._1(), 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer i1, Integer i2) {
                    return i1 + i2;
                }
            });
        // for large dataset, use collect() may cause out of memory error
        for (Tuple2<?, ?> t : pv.collect()) {
            System.out.println(t._1() + "\t" + t._2());  // print PV by day in driver
        }
        /*
         * change the LogRecord to (date, remoteAddr) format,
         * and group the keys to aggregate the remoteAddr.
         * We change the grouped Iteratable Type to Set for uniqueness.
         * Finally we can calculate the number of Unique Visitors
         */
        System.out.println("------UV------");
        JavaPairRDD<String, Integer> uv = distFile.groupByKey().mapToPair(
                new PairFunction<Tuple2<String, Iterable<String>>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<String, Iterable<String>> tuple) {
                    int size = new HashSet((Collection<?>) tuple._2()).size();
                    return new Tuple2<String, Integer>(tuple._1(), size);
                }
            });
        // for large dataset, use collect() may cause out of memory error
        for (Tuple2<?, ?> t : uv.collect()) {
            System.out.println(t._1() + "\t" + t._2());
        }  // print UV by day in driver
    }
}
