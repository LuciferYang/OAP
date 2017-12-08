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

package org.apache.spark.sql.execution.datasources.oap.filecache

import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class FiberCacheManagerSuite extends SharedOapContext {

  private val kbSize = 1024
  private val mbSize = kbSize * kbSize

  private def generateData(size: Int): Array[Byte] =
    Utils.randomizeInPlace(new Array[Byte](size))


  test("unit test") {
    val memorySizeInMB = (MemoryManager.maxMemory / mbSize).toInt
    val origStats = FiberCacheManager.getStats
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(kbSize)
      val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber #$i")
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      val fiberCache2 = FiberCacheManager.get(fiber, configuration)
      assert(fiberCache.toArray sameElements data)
      assert(fiberCache2.toArray sameElements data)
    }
    val stats = FiberCacheManager.getStats.minus(origStats)
    assert(stats.missCount() == memorySizeInMB * 2)
    assert(stats.hitCount() == memorySizeInMB * 2)
    assert(stats.evictionCount() >= memorySizeInMB)
  }

  test("remove a fiber is in use") {
    val memorySizeInMB = (MemoryManager.maxMemory / mbSize).toInt
    val dataInUse = generateData(kbSize)
    val fiberInUse = TestFiber(() => MemoryManager.putToDataFiberCache(dataInUse), s"test fiber #0")
    val fiberCacheInUse = FiberCacheManager.get(fiberInUse, configuration)
    (1 to memorySizeInMB * 2).foreach { i =>
      val data = generateData(1024)
      val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber #$i")
      val fiberCache = FiberCacheManager.get(fiber, configuration)
      assert(fiberCache.toArray sameElements data)
    }
    assert(fiberCacheInUse.isDisposed)
  }

  test("add a very large fiber") {
    val memorySizeInMB = (MemoryManager.maxMemory / mbSize).toInt
    // Cache concurrency is 4, means maximum ENTRY size is memory size / 4
    val data = generateData(memorySizeInMB * mbSize / 8)
    val fiber = TestFiber(() => MemoryManager.putToDataFiberCache(data), s"test fiber #0")
    val fiberCache = FiberCacheManager.get(fiber, configuration)
    assert(!fiberCache.isDisposed)

    val data1 = generateData(memorySizeInMB * mbSize / 2)
    val fiber1 = TestFiber(() => MemoryManager.putToDataFiberCache(data1), s"test fiber #1")
    val fiberCache1 = FiberCacheManager.get(fiber1, configuration)
    assert(fiberCache1.isDisposed)
  }
}
