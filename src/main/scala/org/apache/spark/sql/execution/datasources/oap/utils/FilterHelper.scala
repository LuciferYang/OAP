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

package org.apache.spark.sql.execution.datasources.oap.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetFiltersWrapper
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

object FilterHelper {

  def tryToPushFilters(
      filters: Seq[Filter],
      conf: Configuration,
      path: String,
      enableParquetFilterPushDown: Boolean,
      pushDownDate: Boolean,
      pushDownTimestamp: Boolean,
      pushDownDecimal: Boolean,
      pushDownStartWith: Boolean,
      pushDownInFilterThreshold: Int,
      caseSensitive: Boolean): Option[FilterPredicate] = if (enableParquetFilterPushDown) {
    val filePath = new Path(path)
    val footerFileMetaData =
      ParquetFileReader.readFooter(conf, filePath, SKIP_ROW_GROUPS).getFileMetaData
    val parquetSchema = footerFileMetaData.getSchema
    val parquetFilters = ParquetFiltersWrapper.createFilter(pushDownDate, pushDownTimestamp,
      pushDownDecimal, pushDownStartWith, pushDownInFilterThreshold, caseSensitive)
    filters
      // Collects all converted Parquet filter predicates. Notice that not all predicates can be
      // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
      // is used here.
      .flatMap(parquetFilters.createFilter(parquetSchema, _))
      .reduceOption(FilterApi.and)
  } else {
    None
  }

  def setFilterIfExist(configuration: Configuration, pushed: Option[FilterPredicate]): Unit = {
    pushed match {
      case Some(filters) => ParquetInputFormat.setFilterPredicate(configuration, filters)
      case _ => // do nothing
    }
  }
}
