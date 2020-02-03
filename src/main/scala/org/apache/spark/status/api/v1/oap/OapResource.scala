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
package org.apache.spark.status.api.v1.oap

import javax.ws.rs._
import javax.ws.rs.core.MediaType

import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.datasources.oap.filecache.CacheStats
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.oap.ui.FiberCacheManagerSummary
import org.apache.spark.status.api.v1.BaseAppResource

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class OapResource extends BaseAppResource {

  @GET
  @Path("fibercachemanagers")
  def fiberList(): Seq[FiberCacheManagerSummary] = {
    val seqExecutorSummary = withUI(_.store.executorList(true))
    seqExecutorSummary.map(
      executorSummary => {
        val cacheStats = OapRuntime.getOrCreate.fiberSensor.getExecutorToCacheManager().
            getOrDefault(executorSummary.id, CacheStats())
          val indexDataCacheSeparationEnable = SparkEnv.get.conf.getBoolean(
            OapConf.OAP_INDEX_DATA_SEPARATION_ENABLE.key,
            OapConf.OAP_INDEX_DATA_SEPARATION_ENABLE.defaultValue.get)

        new FiberCacheManagerSummary(
          executorSummary.id,
          executorSummary.hostPort,
          isActive = true,
          indexDataCacheSeparationEnable,
          executorSummary.memoryUsed,
          executorSummary.maxMemory,
          cacheStats.totalCacheSize,
          cacheStats.totalCacheCount,
          cacheStats.backendCacheSize,
          cacheStats.backendCacheCount,
          cacheStats.dataFiberSize,
          cacheStats.dataFiberCount,
          cacheStats.indexFiberSize,
          cacheStats.indexFiberCount,
          cacheStats.pendingFiberSize,
          cacheStats.pendingFiberCount,
          cacheStats.dataFiberHitCount,
          cacheStats.dataFiberMissCount,
          cacheStats.dataFiberLoadCount,
          cacheStats.dataTotalLoadTime,
          cacheStats.dataEvictionCount,
          cacheStats.indexFiberHitCount,
          cacheStats.indexFiberMissCount,
          cacheStats.indexFiberLoadCount,
          cacheStats.indexTotalLoadTime,
          cacheStats.indexEvictionCount
        )
      }
    )
  }
}
