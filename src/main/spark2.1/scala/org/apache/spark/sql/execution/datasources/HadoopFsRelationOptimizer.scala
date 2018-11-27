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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionSet}
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.orc.ReadOnlyOrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ReadOnlyParquetFileFormat}
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, StructType}

object HadoopFsRelationOptimizer extends Logging {

  def optimize(relation: HadoopFsRelation, partitionFilters: Seq[Expression],
      pushedDownFilters: Seq[Filter], outputSchema: StructType): HadoopFsRelation =
    relation.fileFormat match {
      case _: ReadOnlyParquetFileFormat =>
        logInfo("index operation for parquet, retain ReadOnlyParquetFileFormat.")
        relation
      case _: ReadOnlyOrcFileFormat =>
        logInfo("index operation for orc, retain ReadOnlyOrcFileFormat.")
        relation
      // There are two scenarios will use OptimizedParquetFileFormat:
      // 1. canUseCache: OAP_PARQUET_ENABLED is true and OAP_PARQUET_DATA_CACHE_ENABLED is true
      //    and PARQUET_VECTORIZED_READER_ENABLED is true and WHOLESTAGE_CODEGEN_ENABLED is
      //    true and all fields in outputSchema are AtomicType.
      // 2. canUseIndex: OAP_PARQUET_ENABLED is true and hasAvailableIndex.
      // Other scenarios still use ParquetFileFormat.
      case _: ParquetFileFormat
        if relation.sparkSession.conf.get(OapConf.OAP_PARQUET_ENABLED) =>

        val optimizedParquetFileFormat = new OptimizedParquetFileFormat
        val selectedPartitions = relation.location.listFiles(partitionFilters)
        optimizedParquetFileFormat
          .init(relation.sparkSession,
            relation.options,
            selectedPartitions.flatMap(p => p.files))

        def canUseCache: Boolean = {
          val runtimeConf = relation.sparkSession.conf
          val ret = runtimeConf.get(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED) &&
            runtimeConf.get(SQLConf.PARQUET_VECTORIZED_READER_ENABLED) &&
            runtimeConf.get(SQLConf.WHOLESTAGE_CODEGEN_ENABLED) &&
            outputSchema.forall(_.dataType.isInstanceOf[AtomicType])
          if (ret) {
            logInfo("data cache enable and suitable for use , " +
              "will replace with OptimizedParquetFileFormat.")
          }
          ret
        }

        def canUseIndex: Boolean = {
          val ret = optimizedParquetFileFormat.hasAvailableIndex(pushedDownFilters)
          if (ret) {
            logInfo("hasAvailableIndex = true, " +
              "will replace with OptimizedParquetFileFormat.")
          }
          ret
        }

        if (canUseCache || canUseIndex) {
          relation.copy(fileFormat = optimizedParquetFileFormat)(relation.sparkSession)
        } else {
          logInfo("hasAvailableIndex = false and data cache disable, will retain " +
            "ParquetFileFormat.")
          relation
        }

      case _: OrcFileFormat
        if relation.sparkSession.conf.get(OapConf.OAP_ORC_ENABLED) =>
        val optimizedOrcFileFormat = new OptimizedOrcFileFormat
        val selectedPartitions = relation.location.listFiles(partitionFilters)
        optimizedOrcFileFormat
          .init(relation.sparkSession,
            relation.options,
            selectedPartitions.flatMap(p => p.files))

        if (optimizedOrcFileFormat.hasAvailableIndex(pushedDownFilters)) {
          logInfo("hasAvailableIndex = true, will replace with OapFileFormat.")
          // isOapOrcFileFormat is used to indicate to read orc data with oap index accelerated.
          val orcOptions: Map[String, String] =
            Map(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key ->
              relation.sparkSession.sessionState.conf.orcFilterPushDown.toString) ++
              Map("isOapOrcFileFormat" -> "true") ++
              relation.options

          relation.copy(fileFormat = optimizedOrcFileFormat,
            options = orcOptions)(relation.sparkSession)

        } else {
          logInfo("hasAvailableIndex = false, will retain ParquetFileFormat.")
          relation
        }

      case _: OapFileFormat =>
        val selectedPartitions = relation.location.listFiles(partitionFilters)
        relation.fileFormat.asInstanceOf[OapFileFormat].init(
          relation.sparkSession,
          relation.options,
          selectedPartitions.flatMap(p => p.files))
        relation

      case _: FileFormat =>
        relation
    }
}
