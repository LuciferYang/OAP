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

package org.apache.spark.sql.execution

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.oap.{OapFileFormat, OapMetricsManager, OptimizedParquetFileFormat}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.types.StructType

/**
 * Physical plan node for scanning data from HadoopFsRelations.
 *
 * @param relation         The file-based relation to scan.
 * @param output           Output attributes of the scan,
 *                         including data attributes and partition attributes.
 * @param requiredSchema   Required schema of the underlying relation, excluding partition columns.
 * @param partitionFilters Predicates to use for partition pruning.
 * @param dataFilters      Filters on non-partition columns.
 * @param tableIdentifier  identifier for the table in the metastore.
 */
case class OapFileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with ColumnarBatchScan {

  // Note that some vals referring the file-based relation are lazy intentionally
  // so that this plan can be canonicalized on executor side too. See SPARK-23731.
  override lazy val supportsBatch: Boolean = relation.fileFormat.supportBatch(
    relation.sparkSession, StructType.fromAttributes(output))

  override lazy val needsUnsafeRowConversion: Boolean = {
    if (relation.fileFormat.isInstanceOf[OptimizedParquetFileFormat]) {
      SparkSession.getActiveSession.get.sessionState.conf.parquetVectorizedReaderEnabled
    } else {
      false
    }
  }

  override def vectorTypes: Option[Seq[String]] =
    relation.fileFormat.vectorTypes(
      requiredSchema = requiredSchema,
      partitionSchema = relation.partitionSchema,
      relation.sparkSession.sessionState.conf)

  @transient private lazy val selectedPartitions: Seq[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret = relation.location.listFiles(partitionFilters, dataFilters)
    val timeTakenMs = ((System.nanoTime() - startTime) + optimizerMetadataTimeNs) / 1000 / 1000

    metrics("numFiles").add(ret.map(_.files.size.toLong).sum)
    metrics("metadataTime").add(timeTakenMs)

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics("numFiles") :: metrics("metadataTime") :: Nil)

    ret
  }

  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    val bucketSpec = if (relation.sparkSession.sessionState.conf.bucketingEnabled) {
      relation.bucketSpec
    } else {
      None
    }
    bucketSpec match {
      case Some(spec) =>
        // For bucketed columns:
        // -----------------------
        // `HashPartitioning` would be used only when:
        // 1. ALL the bucketing columns are being read from the table
        //
        // For sorted columns:
        // ---------------------
        // Sort ordering should be used when ALL these criteria's match:
        // 1. `HashPartitioning` is being used
        // 2. A prefix (or all) of the sort columns are being read from the table.
        //
        // Sort ordering would be over the prefix subset of `sort columns` being read
        // from the table.
        // eg.
        // Assume (col0, col2, col3) are the columns read from the table
        // If sort columns are (col0, col1), then sort ordering would be considered as (col0)
        // If sort columns are (col1, col0), then sort ordering would be empty as per rule #2
        // above

        def toAttribute(colName: String): Option[Attribute] =
          output.find(_.name == colName)

        val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
        if (bucketColumns.size == spec.bucketColumnNames.size) {
          val partitioning = HashPartitioning(bucketColumns, spec.numBuckets)
          val sortColumns =
            spec.sortColumnNames.map(x => toAttribute(x)).takeWhile(x => x.isDefined).map(_.get)

          val sortOrder = if (sortColumns.nonEmpty) {
            // In case of bucketing, its possible to have multiple files belonging to the
            // same bucket in a given relation. Each of these files are locally sorted
            // but those files combined together are not globally sorted. Given that,
            // the RDD partition will not be sorted even if the relation has sort columns set
            // Current solution is to check if all the buckets have a single file in it

            val files = selectedPartitions.flatMap(partition => partition.files)
            val bucketToFilesGrouping =
              files.map(_.getPath.getName).groupBy(file => BucketingUtils.getBucketId(file))
            val singleFilePartitions = bucketToFilesGrouping.forall(p => p._2.length <= 1)

            if (singleFilePartitions) {
              // TODO Currently Spark does not support writing columns sorting in descending order
              // so using Ascending order. This can be fixed in future
              sortColumns.map(attribute => SortOrder(attribute, Ascending))
            } else {
              Nil
            }
          } else {
            Nil
          }
          (partitioning, sortOrder)
        } else {
          (UnknownPartitioning(0), Nil)
        }
      case _ =>
        (UnknownPartitioning(0), Nil)
    }
  }

  @transient
  private val pushedDownFilters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
  logInfo(s"Pushed Filters: ${pushedDownFilters.mkString(",")}")

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")

    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName + seqToString(location.rootPaths)
    val metadata =
      Map(
        "Format" -> relation.fileFormat.toString,
        "ReadSchema" -> requiredSchema.catalogString,
        "Batched" -> supportsBatch.toString,
        "PartitionFilters" -> seqToString(partitionFilters),
        "PushedFilters" -> seqToString(pushedDownFilters),
        "Location" -> locationDesc)
    val withOptPartitionCount =
      relation.partitionSchemaOption.map { _ =>
        metadata + ("PartitionCount" -> selectedPartitions.size.toString)
      } getOrElse {
        metadata
      }
    withOptPartitionCount
  }

  private lazy val inputRDD: RDD[InternalRow] = {

    // init accumulator before buildReader,
    // now use OapFileSourceScanExec must be OapFileFormat or subclass
    assert(relation.fileFormat.isInstanceOf[OapFileFormat])
    relation.fileFormat.asInstanceOf[OapFileFormat].initMetrics(metrics)

    val readFile: PartitionedFile => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = relation.options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    relation.bucketSpec match {
      case Some(bucketing) if relation.sparkSession.sessionState.conf.bucketingEnabled =>
        createBucketedReadRDD(bucketing, readFile, selectedPartitions, relation)
      case _ =>
        createNonBucketedReadRDD(readFile, selectedPartitions, relation)
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  override lazy val metrics: Map[String, SQLMetric] =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files"),
      "metadataTime" -> SQLMetrics.createMetric(sparkContext, "metadata time (ms)"),
      "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time")) ++
      OapMetricsManager.metrics(sparkContext)

  protected override def doExecute(): RDD[InternalRow] = {
    if (supportsBatch) {
      // in the case of fallback, this batched scan should never fail because of:
      // 1) only primitive types are supported
      // 2) the number of columns should be smaller than spark.sql.codegen.maxFields
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      val unsafeRows = {
        val scan = inputRDD
        if (needsUnsafeRowConversion) {
          scan.mapPartitionsWithIndexInternal { (index, iter) =>
            val proj = UnsafeProjection.create(schema)
            proj.initialize(index)
            iter.map(proj)
          }
        } else {
          scan
        }
      }
      val numOutputRows = longMetric("numOutputRows")
      unsafeRows.map { r =>
        numOutputRows += 1
        r
      }
    }
  }

  override val nodeNamePrefix: String = "OapFile"

  /**
   * Create an RDD for bucketed reads.
   * The non-bucketed variant of this function is [[createNonBucketedReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec         the bucketing spec.
   * @param readFile           a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation         [[HadoopFsRelation]] associated with the read.
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: PartitionedFile => Iterator[InternalRow],
      selectedPartitions: Seq[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    // TODO Bucketed Read RDD should know Oap data cache locations.
    val bucketed =
      selectedPartitions.flatMap { p =>
        p.files.map { f =>
          val hosts = getBlockHosts(getBlockLocations(f), 0, f.getLen)
          PartitionedFile(p.values, f.getPath.toUri.toString, 0, f.getLen, hosts)
        }
      }.groupBy { f =>
        BucketingUtils
          .getBucketId(new Path(f.filePath).getName)
          .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
      }

    val filePartitions = Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
      FilePartition(bucketId, bucketed.getOrElse(bucketId, Nil))
    }

    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
  }

  /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   *
   * @param readFile           a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation         [[HadoopFsRelation]] associated with the read.
   */
  private def createNonBucketedReadRDD(
      readFile: PartitionedFile => Iterator[InternalRow],
      selectedPartitions: Seq[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    val defaultMaxSplitBytes =
      fsRelation.sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = fsRelation.sparkSession.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    // Use Oap not support file split, so no longer call `fileFormat.isSplitable`
    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        val blockLocations = getBlockLocations(file)
        val cachedHosts = OapRuntime.getOrCreate.fiberSensor.getHosts(file.getPath.toString)
        val hosts = cachedHosts.toBuffer ++ getBlockHosts(blockLocations, 0, file.getLen)
        Seq(PartitionedFile(
          partition.values, file.getPath.toUri.toString, 0, file.getLen, hosts.toArray))
      }
    }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition =
          FilePartition(
            partitions.size,
            currentFiles.toArray.toSeq) // Copy to a new Array.
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // Assign files to partitions using "Next Fit Decreasing"
    splitFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()

    new FileScanRDD(fsRelation.sparkSession, readFile, partitions)
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(
      blockLocations: Array[BlockLocation],
      offset: Long, length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if offset <= b.getOffset && offset + length < b.getLength =>
        b.getHosts -> (offset + length - b.getOffset).min(length)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }

  override def doCanonicalize(): OapFileSourceScanExec = {
    OapFileSourceScanExec(
      relation,
      output.map(QueryPlan.normalizeExprId(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(partitionFilters, output),
      QueryPlan.normalizePredicates(dataFilters, output),
      None)
  }
}
