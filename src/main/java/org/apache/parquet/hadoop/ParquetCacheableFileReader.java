/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache;
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheManager;
import org.apache.spark.sql.execution.datasources.oap.filecache.ParquetChunkFiberId;
import org.apache.spark.sql.execution.datasources.oap.io.DataFile;
import org.apache.spark.sql.internal.oap.OapConf$;
import org.apache.spark.sql.oap.OapRuntime$;
import org.apache.spark.unsafe.Platform;

public class ParquetCacheableFileReader extends ParquetFileReader {

  private FiberCacheManager cacheManager = OapRuntime$.MODULE$.getOrCreate().fiberCacheManager();

  private DataFile dataFile;

  private boolean useBinaryCache;

  private int groupCount;

  private int fieldCount;

  ParquetCacheableFileReader(Configuration conf, Path file, ParquetMetadata footer)
          throws IOException {
    super(conf, file, footer);
    this.useBinaryCache =
            conf.getBoolean(OapConf$.MODULE$.OAP_PARQUET_BINARY_DATA_CACHE_ENABLE().key(), false);
    // TODO remove this code
    this.groupCount =getRowGroups().size();
    this.fieldCount = this.getFileMetaData().getSchema().getColumns().size();
  }

  public PageReadStore readNextRowGroup() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }
    this.currentRowGroup = new ColumnChunkPageReadStore(block.getRowCount());
    // prepare the list of consecutive chunks to read them in one scan
    List<OapConsecutiveChunkList> allChunks = new ArrayList<>();
    OapConsecutiveChunkList currentChunks = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      BenchmarkCounter.incrementTotalBytes(mc.getTotalSize());
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        long startingPos = mc.getStartingPos();
        // first chunk or not consecutive => new list
        if (currentChunks == null || currentChunks.endPos() != startingPos) {
          currentChunks = new OapConsecutiveChunkList(startingPos);
          allChunks.add(currentChunks);
        }
        currentChunks.addChunk(
          new ChunkDescriptor(columnDescriptor, mc, startingPos, (int) mc.getTotalSize()));
      }
    }
    // actually read all the chunks
    for (OapConsecutiveChunkList consecutiveChunks : allChunks) {
      final List<Chunk> chunks = consecutiveChunks.readAll(f);
      for (Chunk chunk : chunks) {
        currentRowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
      }
    }

    // avoid re-reading bytes the dictionary reader is used after this call
    if (nextDictionaryReader != null) {
      nextDictionaryReader.setRowGroup(currentRowGroup);
    }

    advanceToNextBlock();

    return currentRowGroup;
  }

  private class OapConsecutiveChunkList extends ConsecutiveChunkList {

    OapConsecutiveChunkList(long offset) {
      super(offset);
    }

    @Override
    public List<ParquetFileReader.Chunk> readAll(SeekableInputStream f) throws IOException {
      List<Chunk> result = new ArrayList<>(chunks.size());
      byte[] chunksBytes = new byte[length];
      if (useBinaryCache) {
        ParquetChunkFiberId fiberId =
                new ParquetChunkFiberId(getPath().toUri().toString(), groupCount, fieldCount,
                        offset, length);
        fiberId.input(f);
        FiberCache fiberCache = cacheManager.get(fiberId);
        Platform.copyMemory(null, fiberCache.getBaseOffset(), chunksBytes,
                Platform.BYTE_ARRAY_OFFSET, length);
        // To avoid object leak
        fiberId.input(null);
      } else {
        f.seek(offset);
        f.readFully(chunksBytes);
        // report in a counter the data we just scanned
        BenchmarkCounter.incrementBytesRead(length);
      }

      int currentChunkOffset = 0;
      for (int i = 0; i < chunks.size(); i++) {
        ChunkDescriptor descriptor = chunks.get(i);
        if (i < chunks.size() - 1) {
          result.add(new Chunk(descriptor, chunksBytes, currentChunkOffset));
        } else {
          // because of a bug, the last chunk might be larger than descriptor.size
          result.add(new WorkaroundChunk(descriptor, chunksBytes, currentChunkOffset, f));
        }
        currentChunkOffset += descriptor.size;
      }
      return result;
    }
  }
}
