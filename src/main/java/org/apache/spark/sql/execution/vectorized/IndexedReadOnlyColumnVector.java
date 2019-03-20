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

import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class IndexedReadOnlyColumnVector extends ColumnVector {

  private ColumnVector column;

  private IntList indexList;

  public IndexedReadOnlyColumnVector(ColumnVector column, IntList indexList) {
    super(column.dataType());
    this.column = column;
    this.indexList = indexList;
  }

  public void updateIndexList(IntList indexList) {
    this.indexList = indexList;
  }

  @Override
  public void close() {
    column.close();
    indexList = null;
  }

  @Override
  public boolean hasNull() {
    return column.hasNull() && hasNullInternal();
  }

  @Override
  public int numNulls() {
    return column.numNulls() == 0 ? 0 : numNullsInternal();
  }

  @Override
  public boolean isNullAt(int id) {
    return column.isNullAt(mapToRowId(id));
  }

  @Override
  public boolean getBoolean(int id) {
    return column.getBoolean(mapToRowId(id));
  }

  @Override
  public byte getByte(int id) {
    return column.getByte(mapToRowId(id));
  }

  @Override
  public short getShort(int id) {
    return column.getShort(mapToRowId(id));
  }

  @Override
  public int getInt(int id) {
    return column.getInt(mapToRowId(id));
  }

  @Override
  public long getLong(int id) {
    return column.getLong(mapToRowId(id));
  }

  @Override
  public float getFloat(int id) {
    return column.getFloat(mapToRowId(id));
  }

  @Override
  public double getDouble(int id) {
    return column.getDouble(mapToRowId(id));
  }

  @Override
  public ColumnarArray getArray(int id) {
    return column.getArray(mapToRowId(id));
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return column.getMap(mapToRowId(ordinal));
  }

  @Override
  public Decimal getDecimal(int id, int precision, int scale) {
    return column.getDecimal(mapToRowId(id), precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int id) {
    return column.getUTF8String(mapToRowId(id));
  }

  @Override
  public byte[] getBinary(int id) {
    return column.getBinary(mapToRowId(id));
  }

  @Override
  protected ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("IndexedReadOnlyColumnVector not support this method");
  }

  private int mapToRowId(int id) {
    return indexList.getInt(id);
  }

  private boolean hasNullInternal() {
    int size = indexList.size();
    for (int i = 0; i < size; i++) {
      if (column.isNullAt(mapToRowId(i))) {
        return true;
      }
    }
    return false;
  }

  private int numNullsInternal() {
    int ret = 0;
    int size = indexList.size();
    for (int i = 0; i < size; i++) {
      if (column.isNullAt(mapToRowId(i))) {
        ret++;
      }
    }
    return ret;
  }
}
