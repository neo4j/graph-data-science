package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphdb.Result;

public class MemRecResult {
    public final String requiredMemory;
    public final String treeView;
    public final long bytesMin, bytesMax;
    public int[] foo;

    public MemRecResult(final MemoryTree memoryRequirements) {
        this(memoryRequirements.render(), memoryRequirements.memoryUsage());
    }

    private MemRecResult(
            final String treeView,
            final MemoryRange estimateMemoryUsage) {
        this(estimateMemoryUsage.toString(), treeView, estimateMemoryUsage.min, estimateMemoryUsage.max);
    }

    private MemRecResult(
            final String requiredMemory,
            final String treeView,
            final long bytesMin,
            final long bytesMax) {
        this.requiredMemory = requiredMemory;
        this.treeView = treeView;
        this.bytesMin = bytesMin;
        this.bytesMax = bytesMax;
    }

    public MemRecResult(final Result.ResultRow row) {
        this(
                row.getString("requiredMemory"),
                row.getString("treeView"),
                row.getNumber("bytesMin").longValue(),
                row.getNumber("bytesMax").longValue()
        );
    }
}
