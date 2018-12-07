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

package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.DataType;

public abstract class ReadOnlyColumnVector extends ColumnVector {

  protected ReadOnlyColumnVector(DataType type) {
    super(0, type, MemoryMode.ON_HEAP);
  }

  @Override
  protected void reserveInternal(int capacity) {
    // this method doNothing
  }

  @Override
  public long nullsNativeAddress() {
    throw new UnsupportedOperationException("ReadOnlyColumnVector not support this method.");
  }

  @Override
  public long valuesNativeAddress() {
    throw new UnsupportedOperationException("ReadOnlyColumnVector not support this method.");
  }

  @Override
  public void putNotNull(int rowId) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putNull(int rowId) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putNulls(int rowId, int count) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putBoolean(int rowId, boolean value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putByte(int rowId, byte value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putShort(int rowId, short value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putInt(int rowId, int value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public int getDictId(int rowId) {
    return 0;
  }

  @Override
  public void putLong(int rowId, long value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putFloat(int rowId, float value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putDouble(int rowId, double value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public int getArrayLength(int rowId) {
    throw new UnsupportedOperationException("ReadOnlyColumnVector not support this method.");
  }

  @Override
  public int getArrayOffset(int rowId) {
    throw new UnsupportedOperationException("ReadOnlyColumnVector not support this method.");
  }

  @Override
  public void loadBytes(ColumnVector.Array array) {
    throw new RuntimeException("This column vector is read only");
  }

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int count) {
    throw new RuntimeException("This column vector is read only");
  }
}
