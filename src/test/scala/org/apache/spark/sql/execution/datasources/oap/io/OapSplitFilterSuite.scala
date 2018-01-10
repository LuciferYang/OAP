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

import com.google.common.collect.Lists
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.mockito.Mockito

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging


class OapSplitFilterSuite extends SparkFunSuite with Logging {

  test("startOffset is negative") {
    intercept[AssertionError] {
      OapSplitFilter(-1, 1)
    }
  }

  test("endOffset is negative") {
    intercept[AssertionError] {
      OapSplitFilter(0, -1)
    }
  }

  test("endOffset less then startOffset") {
    intercept[AssertionError] {
      OapSplitFilter(15, 10)
    }
  }

  test("test isUsefulRowGroup") {
    val rowGroupMeta0 = new RowGroupMeta().withNewStart(0L).withNewEnd(100L)
    assert(OapSplitFilter(0L, 130L).isUsefulRowGroup(rowGroupMeta0))

    val rowGroupMeta1 = new RowGroupMeta().withNewStart(101L).withNewEnd(200L)
    assert(!OapSplitFilter(0L, 70L).isUsefulRowGroup(rowGroupMeta1))
  }

  test("test isUsefulBLock") {
    val columnMeta0 = Mockito.mock(classOf[ColumnChunkMetaData])
    Mockito.when(columnMeta0.getStartingPos).thenReturn(0L)
    val rowGroup0 = Mockito.mock(classOf[BlockMetaData])
    Mockito.when(rowGroup0.getCompressedSize).thenReturn(100L)
    Mockito.when(rowGroup0.getColumns).thenReturn(Lists.newArrayList(columnMeta0))
    assert(OapSplitFilter(0L, 130L).isUsefulBLock(rowGroup0))

    val columnMeta1 = Mockito.mock(classOf[ColumnChunkMetaData])
    Mockito.when(columnMeta1.getStartingPos).thenReturn(101L)
    val rowGroup1 = Mockito.mock(classOf[BlockMetaData])
    Mockito.when(rowGroup1.getCompressedSize).thenReturn(100L)
    Mockito.when(rowGroup1.getColumns).thenReturn(Lists.newArrayList(columnMeta1))
    assert(!OapSplitFilter(0L, 70L).isUsefulBLock(rowGroup1))
  }

}
