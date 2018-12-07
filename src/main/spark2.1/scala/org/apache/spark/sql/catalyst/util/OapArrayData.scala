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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.execution.vectorized.ColumnVector
import org.apache.spark.unsafe.bitset.BitSetMethods

object OapArrayData {

  implicit class ArrayDataExtensions(array: ArrayData) {

    def setNullAt(i: Int): Unit = throw new UnsupportedOperationException

    def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException

    // default implementation (slow)
    def setBoolean(i: Int, value: Boolean): Unit = update(i, value)
    def setByte(i: Int, value: Byte): Unit = update(i, value)
    def setShort(i: Int, value: Short): Unit = update(i, value)
    def setInt(i: Int, value: Int): Unit = update(i, value)
    def setLong(i: Int, value: Long): Unit = update(i, value)
    def setFloat(i: Int, value: Float): Unit = update(i, value)
    def setDouble(i: Int, value: Double): Unit = update(i, value)
  }

  implicit class GenericArrayDataExtensions(arrayData: GenericArrayData)
    extends ArrayDataExtensions(arrayData) {

    override def setNullAt(ordinal: Int): Unit = arrayData.array(ordinal) = null

    override def update(ordinal: Int, value: Any): Unit = arrayData.array(ordinal) = value
  }

  implicit class UnsafeArrayDataExtensions(arrayData: UnsafeArrayData)
    extends ArrayDataExtensions(arrayData) {

    override def setNullAt(ordinal: Int): Unit = {
      BitSetMethods.set(arrayData.getBaseObject, arrayData.getBaseOffset + 8, ordinal)
    }

    override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException
  }

  implicit class ColumnVectorArrayDataExtensions(arrayData: ColumnVector.Array)
    extends ArrayDataExtensions(arrayData) {

    override def setNullAt(ordinal: Int): Unit = throw new UnsupportedOperationException

    override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException
  }
}
