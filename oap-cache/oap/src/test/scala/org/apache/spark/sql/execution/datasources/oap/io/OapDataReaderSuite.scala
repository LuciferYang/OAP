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

package org.apache.spark.sql.execution.datasources.oap.io

import java.io.File

import org.apache.hadoop.fs.Path
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OapException, PartitionedFile}
import org.apache.spark.sql.execution.datasources.oap.{INDEX_STAT, OapFileFormat}
import org.apache.spark.sql.execution.datasources.oap.INDEX_STAT.INDEX_STAT
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class OapDataReaderSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {

  private var tmpDir: File = _
  private lazy val fs = (new Path(tmpDir.getPath)).getFileSystem(configuration)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tmpDir = Utils.createTempDir()
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tmpDir)
    } finally {
      super.afterAll()
    }
  }

  // Override afterEach because we don't want to check open streams
  override def beforeEach(): Unit = {}
  override def afterEach(): Unit = {}

  test("get Data file class for reading: Parquet") {
    val mockOapDataReaderV1 = mock(classOf[OapDataReaderV1])
    assert(
      DataFileUtils.getDataFileClassFor(
        OapFileFormat.PARQUET_DATA_FILE_CLASSNAME)
          == OapFileFormat.PARQUET_DATA_FILE_CLASSNAME)
  }

  test("get Data file class for reading: undefined data reader class from DataSourceMeta") {
    val whatever = mock(classOf[OapDataReaderV1])
    val WHATEVER = "whatever"
    val e = intercept[OapException](DataFileUtils.getDataFileClassFor(WHATEVER))
    assert(e.getMessage.contains("Undefined data reader class name"))
  }
}
