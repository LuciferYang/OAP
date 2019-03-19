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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntList;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class IndexedOnHeapColumnVector extends OnHeapColumnVector {

  public static IndexedOnHeapColumnVector[] allocateColumns(int capacity, StructType schema) {
    return allocateColumns(capacity, schema.fields());
  }

  public static IndexedOnHeapColumnVector[] allocateColumns(int capacity, StructField[] fields) {
    IndexedOnHeapColumnVector[] vectors = new IndexedOnHeapColumnVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      vectors[i] = new IndexedOnHeapColumnVector(capacity, fields[i].dataType());
    }
    return vectors;
  }

  private IntList indexList;

  public IndexedOnHeapColumnVector(int capacity, DataType type) {
    super(capacity, type);
  }

  public void updateIndexList(IntList indexList) {
    this.indexList = indexList;
  }

  @Override
  public void reset() {
    this.indexList = null;
    super.reset();
  }

  @Override
  public void close() {
    this.indexList = null;
    super.close();
  }

  @Override
  public boolean hasNull() {
    if (!super.hasNull()) {
      return false;
    } else {
      byte[] nulls = super.getNulls();
      for (int i = 0; i < indexList.size(); i++) {
        if (nulls[i] == 1) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public int numNulls() {
    if (!super.hasNull()) {
      return 0;
    } else {
      int nullsCount = 0;
      byte[] nulls = super.getNulls();
      for (int i = 0; i < indexList.size(); i++) {
        if (nulls[i] == 1) {
          nullsCount++;
        }
      }
      return nullsCount;
    }
  }

  @Override
  public boolean isNullAt(int rowId) {
    return super.isNullAt(mapRowId(rowId));
  }

  @Override
  public boolean getBoolean(int rowId) {
    return super.getBoolean(mapRowId(rowId));
  }

  @Override
  public byte getByte(int rowId) {
    return super.getByte(mapRowId(rowId));
  }

  @Override
  public short getShort(int rowId) {
    return super.getShort(mapRowId(rowId));
  }

  @Override
  public int getInt(int rowId) {
    return super.getInt(mapRowId(rowId));
  }

  @Override
  public long getLong(int rowId) {
    return super.getLong(mapRowId(rowId));
  }

  @Override
  public float getFloat(int rowId) {
    return super.getFloat(mapRowId(rowId));
  }

  @Override
  public double getDouble(int rowId) {
    return super.getDouble(mapRowId(rowId));
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (super.isNullAt(mapRowId(rowId))) return null;
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(getInt(rowId), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(rowId), precision, scale);
    } else {
      byte[] bytes = getBinary(rowId);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    int mapedRowId = mapRowId(rowId);
    if (super.isNullAt(mapedRowId)) return null;
    if (dictionary == null) {
      return arrayData().getBytesAsUTF8String(
        getArrayOffset(mapedRowId), getArrayLength(mapedRowId));
    } else {
      byte[] bytes = dictionary.decodeToBinary(dictionaryIds.getDictId(mapedRowId));
      return UTF8String.fromBytes(bytes);
    }
  }

  @Override
  public byte[] getBinary(int rowId) {
    int mapedRowId = mapRowId(rowId);
    if (super.isNullAt(mapedRowId)) return null;
    if (dictionary == null) {
      return arrayData().getBytes(getArrayOffset(mapedRowId), getArrayLength(mapedRowId));
    } else {
      return dictionary.decodeToBinary(dictionaryIds.getDictId(mapedRowId));
    }
  }

  private int mapRowId(int rowId) {
    return indexList.getInt(rowId);
  }
}
