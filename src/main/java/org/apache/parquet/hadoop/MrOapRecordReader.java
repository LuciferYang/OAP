/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import java.io.IOException;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.RecordReader;
import org.apache.parquet.hadoop.metadata.ParquetFooter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;


public class MrOapRecordReader<T> implements RecordReader<T> {

    private Configuration configuration;
    private Path file;

    private InternalParquetRecordReader<T> internalReader;

    private ReadSupport<T> readSupport;

    private ParquetFooter footer;

    public MrOapRecordReader(
        ReadSupport<T> readSupport,
        Path file,
        Configuration configuration,
        ParquetFooter footer) {
      this.readSupport = readSupport;
      this.file = file;
      this.configuration = configuration;
      this.footer = footer;
    }

    @Override
    public void close() throws IOException {
      internalReader.close();
    }

    @Override
    public T getCurrentValue() throws IOException, InterruptedException {
      return internalReader.getCurrentValue();
    }

    @Override
    public void initialize() throws IOException, InterruptedException {
      ParquetFileReader parquetFileReader = null;
      try {
        ParquetMetadata parquetMetadata = footer.toParquetMetadata();
        parquetFileReader =
          new ParquetFileReader(configuration, parquetMetadata.getFileMetaData(), file,
                  parquetMetadata.getBlocks(), Lists.newArrayList());
        this.internalReader = new InternalParquetRecordReader<>(readSupport);
        this.internalReader.initialize(parquetFileReader, configuration);
      } catch (IOException e) {
        if(null != parquetFileReader) parquetFileReader.close();
        throw e;
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return internalReader.nextKeyValue();
    }
}
