/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.oap

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{OapExtensions, SparkSession, SparkSessionExtensions}
import org.apache.spark.util.Utils

class ExtensionsSuite extends SparkFunSuite {

  type ExtensionsBuilder = SparkSessionExtensions => Unit

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def withSession(builder: ExtensionsBuilder)(f: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("test-sql-context")
      .config("spark.memory.offHeap.size", "100m")
      .config("spark.sql.testkey", "true")
      .withExtensions(builder)
      .getOrCreate()

    try f(spark) finally {
      stop(spark)
    }
  }


  private def withOapExtensionsSession(f: SparkSession => Unit): Unit =
    withSession(new OapExtensions)(f)

  test("extensions") {
    withOapExtensionsSession { session =>
      val path = Utils.createTempDir().getAbsolutePath
      session.sql(s"""CREATE TEMPORARY VIEW parquet_test (a INT, b STRING)
                     | USING parquet
                     | OPTIONS (path '$path')""".stripMargin)


      val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
      session.createDataFrame(data)
        .toDF("key", "value").createOrReplaceTempView("t")

      session.sql("insert overwrite table parquet_test select * from t")
      session.sql("create oindex index1 on parquet_test (a)")
      val rows = session.sql("select a from parquet_test where a = 1").collect()
      assert(rows.length == 1)
      assert(rows.head.getInt(0) == 1)
    }
  }
}
