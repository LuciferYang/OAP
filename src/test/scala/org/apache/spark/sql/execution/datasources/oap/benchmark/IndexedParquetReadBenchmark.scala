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
package org.apache.spark.sql.execution.datasources.oap.benchmark

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{DefaultRecordReader, VectorizedOapRecordReader}

import org.apache.spark.sql.execution.datasources.oap.benchmark.BenchmarkUtils._
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadBenchmark.{spark, withSQLConf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure parquet with oap read performance for IntScan.
 * To run this:
 * spark-submit --class <this class> --jars <spark sql test jar>
 */
object IndexedParquetReadBenchmark {

  def intScanBenchmark(values: Int): Unit = {

    // Benchmarks driving reader component directly.
    val benchmark = new Benchmark("Indexed Sql Single Int Column Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "tempTable") {
        spark.range(values).createOrReplaceTempView("t1")
        spark.sql("select cast(id as INT) as id from t1")
          .write.parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tempTable")

        val min = 0
        val mid = values/2
        val max = values - 1

        // use mr indexed read, query minValue
        benchmark.addCase("SQL Parquet MR -> min") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql(s"""select sum(id) from tempTable where id = $min""").collect()
          }
        }

        // use vec indexed read, query minValue
        benchmark.addCase("SQL Parquet Vectorized -> min") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
            spark.sql(s"""select sum(id) from tempTable where id = $min""").collect()
          }
        }

        // use mr indexed read, query midValue
        benchmark.addCase("SQL Parquet MR -> mid") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql(s"""select sum(id) from tempTable where id = $mid""").collect()
          }
        }

        // use vec indexed read, query midValue
        benchmark.addCase("SQL Parquet Vectorized -> mid") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
            spark.sql(s"""select sum(id) from tempTable where id = $mid""").collect()
          }
        }

        // use mr indexed read, query maxValue
        benchmark.addCase("SQL Parquet MR -> max") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql(s"""select sum(id) from tempTable where id = $max""").collect()
          }
        }

        // use vec indexed read, query maxValue
        benchmark.addCase("SQL Parquet Vectorized -> max") { iter =>
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
            spark.sql(s"""select sum(id) from tempTable where id = $max""").collect()
          }
        }

        benchmark.run()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    intScanBenchmark(1024 * 1024 * 15)
  }
}
