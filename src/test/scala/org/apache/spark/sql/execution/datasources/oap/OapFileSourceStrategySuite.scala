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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}
import org.apache.spark.util.Utils

abstract class OapFileSourceStrategySuite extends QueryTest with SharedOapContext with
  BeforeAndAfterEach {

  // TODO move Parquet TestSuite from FilterSuite
  import testImplicits._

  private var currentPath: String = _
  private var defaultEis: Boolean = true

  protected def testTableName: String

  protected def fileFormat: String

  protected def verifyFileFormat(format: FileFormat): Boolean

  protected def verifyOptimizedSameResult(plan1: SparkPlan, plan2: SparkPlan): Boolean =
    !plan1.sameResult(plan2)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // In this suite we don't want to skip index even if the cost is higher.
    defaultEis = sqlContext.conf.getConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION)
    sqlContext.conf.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION, false)
  }

  override def afterAll(): Unit = {
    sqlContext.conf.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION, defaultEis)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
    currentPath = path
    sql(s"""CREATE TEMPORARY VIEW $testTableName (a INT, b STRING)
           | USING $fileFormat
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable(s"$testTableName")
  }

  test("Optimized by OapFileSourceStrategy") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    withIndex(TestIndex(s"$testTableName", "index1")) {
      sql(s"create oindex index1 on $testTableName (b)")
      val plan =
        sql(s"SELECT b FROM $testTableName WHERE b = 'this is test 1'").queryExecution.optimizedPlan
      val optimizedSparkPlans = OapFileSourceStrategy(plan)
      assert(optimizedSparkPlans.size == 1)

      val optimizedSparkPlan = optimizedSparkPlans.head
      assert(optimizedSparkPlan.isInstanceOf[ProjectExec])
      assert(optimizedSparkPlan.children.nonEmpty)
      assert(optimizedSparkPlan.children.length == 1)

      val filter = optimizedSparkPlan.children.head
      assert(filter.isInstanceOf[FilterExec])
      assert(filter.children.nonEmpty)
      assert(filter.children.length == 1)

      val scan = filter.children.head
      assert(scan.isInstanceOf[FileSourceScanExec])
      val relation = scan.asInstanceOf[FileSourceScanExec].relation
      assert(relation.isInstanceOf[HadoopFsRelation])
      assert(verifyFileFormat(relation.fileFormat))

      val sparkPlans = FileSourceStrategy(plan)
      assert(sparkPlans.size == 1)
      val sparkPlan = sparkPlans.head
      assert(verifyOptimizedSameResult(sparkPlan, optimizedSparkPlan))
    }
  }

  test("Not optimized by OapFileSourceStrategy") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    withIndex(TestIndex(s"$testTableName", "index1")) {
      sql(s"create oindex index1 on $testTableName (b)")
      val plan =
        sql(s"SELECT b FROM $testTableName WHERE a = 1").queryExecution.optimizedPlan
      val optimizedSparkPlans = OapFileSourceStrategy(plan)
      assert(optimizedSparkPlans.size == 1)
      val optimizedSparkPlan = optimizedSparkPlans.head

      val sparkPlans = FileSourceStrategy(plan)
      assert(sparkPlans.size == 1)
      val sparkPlan = sparkPlans.head

      assert(sparkPlan.sameResult(optimizedSparkPlan))
    }
  }
}

class OapFileSourceStrategyForParquetSuite extends OapFileSourceStrategySuite {
  protected def testTableName: String = "parquet_test"

  protected def fileFormat: String = "parquet"

  protected def verifyFileFormat(format: FileFormat): Boolean =
    format.isInstanceOf[OptimizedParquetFileFormat]
}

class OapFileSourceStrategyForOrcSuite extends OapFileSourceStrategySuite {
  protected def testTableName: String = "orc_test"

  protected def fileFormat: String = "orc"

  protected def verifyFileFormat(format: FileFormat): Boolean =
    format.isInstanceOf[OptimizedOrcFileFormat]
}

class OapFileSourceStrategyForOapSuite extends OapFileSourceStrategySuite {
  protected def testTableName: String = "oap_test"

  protected def fileFormat: String = "oap"

  protected def verifyFileFormat(format: FileFormat): Boolean =
    format.isInstanceOf[OapFileFormat]

  // Oap Data File always use original plan.
  override protected def verifyOptimizedSameResult(plan1: SparkPlan, plan2: SparkPlan): Boolean =
    plan1.sameResult(plan2)
}
