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

import scala.io.Source
import java.util.TreeMap
import java.io.InputStream
import java.security.MessageDigest


object Utils {
  def ipToLong(ip: String): Long = {
    val ip_num = for ( elem <- ip.split('.')) yield elem.toInt
    def base:Long = 256
    base * (base * (base * ip_num(0) + ip_num(1)) + ip_num(2)) + ip_num(3)
  }

  /* fetch ipdb from the bos or other url */
  def prepareIPDB(ipdb: String): TreeMap[Long, String] = {
    val content = Source.fromURL(ipdb).mkString
    val treemap = new TreeMap[Long, String]
    for(line <- content.split('\n')) {
      val r = line.split(' ')
      treemap.put(ipToLong(r(0)), r(1))
    }
    treemap
  }

  def fetchLocByIP(treemap: TreeMap[Long, String], ip: String): String = {
    treemap.lowerEntry(ipToLong(ip)).getValue()
  }

  def getMD5(s: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    digest.digest(s.getBytes).map("%02x".format(_)).mkString
  }
}
