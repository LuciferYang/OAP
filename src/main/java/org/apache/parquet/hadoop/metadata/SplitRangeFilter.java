package org.apache.parquet.hadoop.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Preconditions;

public class SplitRangeFilter {

    private final long startOffset;
    private final long endOffset;

    public SplitRangeFilter(Configuration conf) {
        this.startOffset = conf.getLong("oap.split.startOffset", -1L);
        this.endOffset = conf.getLong("oap.split.endOffset", -1L);
        Preconditions.checkArgument(startOffset >= 0L, "startOffset should more than 0.");
        Preconditions.checkArgument(endOffset >= 0L, "endOffset should more than 0.");
        Preconditions.checkArgument(endOffset >= startOffset, "endOffset should more than startOffset.");
    }

    public boolean isUsefulBLock(BlockMetaData block) {
        ColumnChunkMetaData md = block.getColumns().get(0);
        long startOffSet = md.getStartingPos();
        long totalSize = block.getCompressedSize();
        long midPoint = startOffSet + totalSize / 2;
        return midPoint >= this.startOffset && midPoint < this.endOffset;
    }
}
