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
import org.apache.spark.sql.execution.{DataSourceScanExec, FileSourceScanExec, FilterExec, OapFileSourceScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}
import org.apache.spark.util.Utils

abstract class OapFileSourceStrategySuite extends QueryTest with SharedOapContext with
  BeforeAndAfterEach {

  // TODO move Parquet TestSuite from FilterSuite
  import testImplicits._

  protected def testTableName: String

  protected def fileFormat: String

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
    sql(s"""CREATE TEMPORARY VIEW $testTableName (a INT, b STRING)
           | USING $fileFormat
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable(s"$testTableName")
  }

  protected def verifyProjectFilterScan(
      indexColumn: String,
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean,
      verifyScanAndGetRelation: SparkPlan => HadoopFsRelation): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    withIndex(TestIndex(s"$testTableName", "index1")) {
      sql(s"create oindex index1 on $testTableName ($indexColumn)")
      val plan =
        sql(s"SELECT b FROM $testTableName WHERE b = 'this is test 1'")
          .queryExecution.optimizedPlan
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
      val relation = verifyScanAndGetRelation(scan)
      assert(relation.isInstanceOf[HadoopFsRelation])
      assert(verifyFileFormat(relation.fileFormat))

      val sparkPlans = FileSourceStrategy(plan)
      assert(sparkPlans.size == 1)
      val sparkPlan = sparkPlans.head
      assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))
    }
  }

  protected def verifyProjectScan(
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean,
      verifyScanAndGetRelation: SparkPlan => HadoopFsRelation): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    val plan = sql(s"SELECT a FROM $testTableName").queryExecution.optimizedPlan
    val optimizedSparkPlans = OapFileSourceStrategy(plan)
    assert(optimizedSparkPlans.size == 1)

    val optimizedSparkPlan = optimizedSparkPlans.head
    assert(optimizedSparkPlan.isInstanceOf[ProjectExec])
    assert(optimizedSparkPlan.children.nonEmpty)
    assert(optimizedSparkPlan.children.length == 1)

    val scan = optimizedSparkPlan.children.head
    val relation = verifyScanAndGetRelation(scan)
    assert(relation.isInstanceOf[HadoopFsRelation])
    assert(verifyFileFormat(relation.fileFormat))

    val sparkPlans = FileSourceStrategy(plan)
    assert(sparkPlans.size == 1)
    val sparkPlan = sparkPlans.head
    assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))
  }

  protected def verifyScan(
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean,
      verifyScanAndGetRelation: SparkPlan => HadoopFsRelation): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    val plan = sql(s"FROM $testTableName").queryExecution.optimizedPlan
    val optimizedSparkPlans = OapFileSourceStrategy(plan)
    assert(optimizedSparkPlans.size == 1)
    val optimizedSparkPlan = optimizedSparkPlans.head
    val relation = verifyScanAndGetRelation(optimizedSparkPlan)
    assert(verifyFileFormat(relation.fileFormat))

    val sparkPlans = FileSourceStrategy(plan)
    assert(sparkPlans.size == 1)
    val sparkPlan = sparkPlans.head

    assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))
  }

  protected def verifyOapScanAndGetRelation: SparkPlan => HadoopFsRelation = scan => {
    assert(scan.isInstanceOf[OapFileSourceScanExec])
    scan.asInstanceOf[OapFileSourceScanExec].relation
  }

  protected def verifyScanAndGetRelation: SparkPlan => HadoopFsRelation = scan => {
    assert(scan.isInstanceOf[FileSourceScanExec])
    scan.asInstanceOf[FileSourceScanExec].relation
  }
}

class OapFileSourceStrategyForParquetSuite extends OapFileSourceStrategySuite {
  protected def testTableName: String = "parquet_test"

  protected def fileFormat: String = "parquet"

  test("Project-> Filter -> Scan : Optimized") {
    verifyProjectFilterScan(
      indexColumn = "b",
      format => format.isInstanceOf[OptimizedParquetFileFormat],
      (plan1, plan2) => !plan1.sameResult(plan2),
      verifyOapScanAndGetRelation
    )
  }

  test("Project-> Filter -> Scan : Not Optimized") {
    verifyProjectFilterScan(
      indexColumn = "a",
      format => format.isInstanceOf[ParquetFileFormat],
      (plan1, plan2) => plan1.sameResult(plan2),
      verifyScanAndGetRelation
    )
  }

  test("Project -> Scan : Optimized") {
    withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true") {
      verifyProjectScan(
        format => format.isInstanceOf[OptimizedParquetFileFormat],
        (plan1, plan2) => !plan1.sameResult(plan2),
        verifyOapScanAndGetRelation
      )
    }
  }

  test("Project -> Scan : Not Optimized") {
    verifyProjectScan(
      format => format.isInstanceOf[ParquetFileFormat],
      (plan1, plan2) => plan1.sameResult(plan2),
      verifyScanAndGetRelation
    )
  }

  test("Scan : Not Optimized") {
    verifyScan(
      format => format.isInstanceOf[ParquetFileFormat],
      (plan1, plan2) => plan1.sameResult(plan2),
      verifyScanAndGetRelation
    )
  }
}

class OapFileSourceStrategyForOrcSuite extends OapFileSourceStrategySuite {
  protected def testTableName: String = "orc_test"

  protected def fileFormat: String = "orc"

  test("Project-> Filter -> Scan : Optimized") {
    verifyProjectFilterScan(
      indexColumn = "b",
      format => format.isInstanceOf[OptimizedOrcFileFormat],
      (plan1, plan2) => !plan1.sameResult(plan2),
      verifyOapScanAndGetRelation
    )
  }

  test("Project-> Filter -> Scan : Not Optimized") {
    verifyProjectFilterScan(
      indexColumn = "a",
      format => format.isInstanceOf[OrcFileFormat],
      (plan1, plan2) => plan1.sameResult(plan2),
      verifyScanAndGetRelation
    )
  }

  test("Project -> Scan : Not Optimized") {
    verifyProjectScan(
      format => format.isInstanceOf[OrcFileFormat],
      (plan1, plan2) => plan1.sameResult(plan2),
      verifyScanAndGetRelation
    )
  }

  test("Scan : Not Optimized") {
    verifyScan(
      format => format.isInstanceOf[OrcFileFormat],
      (plan1, plan2) => plan1.sameResult(plan2),
      verifyScanAndGetRelation
    )
  }
}

class OapFileSourceStrategyForOapSuite extends OapFileSourceStrategySuite {
  protected def testTableName: String = "oap_test"

  protected def fileFormat: String = "oap"

  test("Project-> Filter -> Scan") {
    verifyProjectFilterScan(
      indexColumn = "b",
      format => format.isInstanceOf[OapFileFormat],
      (plan1, plan2) => !plan1.sameResult(plan2),
      verifyOapScanAndGetRelation
    )
  }

  test("Project -> Scan") {
    verifyProjectScan(
      format => format.isInstanceOf[OapFileFormat],
      (plan1, plan2) => !plan1.sameResult(plan2),
      verifyOapScanAndGetRelation
    )
  }

  test("Scan") {
    verifyScan(
      format => format.isInstanceOf[OapFileFormat],
      (plan1, plan2) => !plan1.sameResult(plan2),
      verifyOapScanAndGetRelation
    )
  }
}
