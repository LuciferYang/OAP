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

import scala.collection.JavaConverters._

import org.apache.parquet.hadoop.metadata.BlockMetaData

case class OapSplitFilter(startOffset: Long, endOffset: Long) {

  assert(startOffset >= 0L, "startOffset should more than 0.")
  assert(endOffset >= 0L, "endOffset should more than 0.")
  assert(endOffset >= startOffset, "endOffset should more than startOffset.")

  def isUsefulBLock(block: BlockMetaData): Boolean = {
    val md = block.getColumns.get(0)
    val startOffSet = md.getStartingPos
    val totalSize = block.getCompressedSize
    val midPoint = startOffSet + totalSize / 2
    midPoint >= this.startOffset && midPoint < this.endOffset
  }

  def isUsefulRowGroup(rowGroupMeta: RowGroupMeta): Boolean = {
    val start = rowGroupMeta.start
    val totalSize = rowGroupMeta.end - start
    val midPoint = start + totalSize / 2
    midPoint >= this.startOffset && midPoint < this.endOffset
  }

  def oapSplitTotalRows(meta: OapDataFileHandle): Long = {
    val usefulGroupIds = (0 until meta.groupCount)
      .filter(groupId => isUsefulRowGroup(meta.rowGroupsMeta(groupId)))
    oapSplitTotalRows(meta, usefulGroupIds)
  }


  def oapSplitTotalRows(meta: OapDataFileHandle, usefulGroupIds: Iterable[Int]): Long =
    usefulGroupIds.map(groupId => if (groupId < meta.groupCount - 1) {
      meta.rowCountInEachGroup
    } else {
      meta.rowCountInLastGroup
    }).sum

  def oapSplitTotalRows(meta: ParquetDataFileHandle): Long =
    meta.footer.getBlocks.asScala
      .map(block =>
        if (isUsefulBLock(block)) block.getRowCount
        else 0).sum
}

object  OapSplitFilter {
  val DEFAULT: OapSplitFilter = OapSplitFilter(0L, Long.MaxValue)
}
