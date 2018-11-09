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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.oap.io.{DataFileContext, OapDataReaderV1, ParquetVectorizedContext}
import org.apache.spark.sql.execution.datasources.oap.utils.FilterHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.util.SerializableConfiguration

private[sql] class OptimizedParquetFileFormat extends OapFileFormat {

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory =
    throw new UnsupportedOperationException("OptimizedParquetFileFormat " +
      "only support read operation")

  /**
   * Returns whether the reader will return the rows as batch or not.
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    conf.parquetVectorizedReaderEnabled && conf.wholeStageEnabled &&
      schema.length <= conf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    // TODO we need to pass the extra data source meta information via the func parameter
    meta match {
      case Some(m) =>
        logDebug(s"Building OapDataReader with "
          + m.dataReaderClassName.substring(m.dataReaderClassName.lastIndexOf(".") + 1)
          + " ...")

        val filterScanners = indexScanners(m, filters)
        // TODO refactor this.
        hitIndexColumns = filterScanners match {
          case Some(s) =>
            s.scanners.flatMap { scanner =>
              scanner.keyNames.map(n => n -> scanner.meta.indexType)
            }.toMap
          case _ => Map.empty
        }

        val requiredIds = requiredSchema.map(dataSchema.fields.indexOf(_)).toArray
        val pushed = FilterHelper.tryToPushFilters(sparkSession, requiredSchema, filters)

        val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
        // TODO why add `sparkSession.sessionState.conf.wholeStageEnabled` condition
        val enableVectorizedReader: Boolean =
          sparkSession.sessionState.conf.parquetVectorizedReaderEnabled &&
            sparkSession.sessionState.conf.wholeStageEnabled &&
            resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
        val returningBatch = supportBatch(sparkSession, resultSchema)

        // Sets flags for `CatalystSchemaConverter`
        hadoopConf.setBoolean(
          SQLConf.PARQUET_BINARY_AS_STRING.key,
          sparkSession.sessionState.conf.isParquetBinaryAsString)
        hadoopConf.setBoolean(
          SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
          sparkSession.sessionState.conf.isParquetINT96AsTimestamp)
        hadoopConf.setBoolean(
          SQLConf.PARQUET_INT64_AS_TIMESTAMP_MILLIS.key,
          sparkSession.sessionState.conf.isParquetINT64AsTimestampMillis)

        val broadcastedHadoopConf =
          sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

        (file: PartitionedFile) => {
          assert(file.partitionValues.numFields == partitionSchema.size)
          val conf = broadcastedHadoopConf.value.value
          // For parquet, if enableVectorizedReader is true, init ParquetVectorizedContext.
          // Otherwise context is none.
          val context: Option[DataFileContext] = if (enableVectorizedReader) {
            Some(ParquetVectorizedContext(partitionSchema,
              file.partitionValues, returningBatch))
          } else {
            None
          }

          val reader = new OapDataReaderV1(file.filePath, m, partitionSchema, requiredSchema,
            filterScanners, requiredIds, pushed, oapMetrics, conf, enableVectorizedReader, options,
            filters, context)
          reader.read(file)
        }
      case None => (_: PartitionedFile) => {
        // TODO For parquet should refer to ParquetFileFormat
        Iterator.empty
      }
    }
  }
}
