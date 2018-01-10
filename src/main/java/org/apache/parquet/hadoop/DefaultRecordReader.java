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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.RecordReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.spark.sql.execution.datasources.oap.io.OapSplitFilter;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import com.google.common.collect.Lists;

public class DefaultRecordReader<T> implements RecordReader<T> {

    private Configuration configuration;
    private Path file;

    private InternalParquetRecordReader<T> internalReader;

    private ReadSupport<T> readSupport;

    private ParquetMetadata footer;

    private OapSplitFilter splitFilter;

    DefaultRecordReader(ReadSupport<T> readSupport,
                        Path file,
                        Configuration configuration,
                        ParquetMetadata footer,
                        OapSplitFilter splitFilter) {
        this.readSupport = readSupport;
        this.file = file;
        this.configuration = configuration;
        this.footer = footer;
        this.splitFilter = splitFilter;
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
    public float getProgress() throws IOException, InterruptedException {
        return internalReader.getProgress();
    }

    public void initialize() throws IOException, InterruptedException {
        if(this.footer == null){
            footer = readFooter(configuration, file, NO_FILTER);
        }

        List<BlockMetaData> usefulRowGroups = Lists.newArrayList();
        for (BlockMetaData rowGroup : footer.getBlocks()) {
            if(splitFilter.isUsefulBLock(rowGroup)) {
                usefulRowGroups.add(rowGroup);
            }
        }
        ParquetFileReader parquetFileReader = ParquetFileReader.open(configuration, file,
                new ParquetMetadata(footer.getFileMetaData(), usefulRowGroups));
        this.internalReader = new InternalParquetRecordReader<T>(readSupport);
        this.internalReader.initialize(parquetFileReader, configuration);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return internalReader.nextKeyValue();
    }
}
