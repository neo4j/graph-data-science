package org.neo4j.graphalgo.core.huge;

import org.junit.Assert;
import org.junit.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.core.huge.HugeAdjacencyList.PAGE_MASK;
import static org.neo4j.graphalgo.core.huge.HugeAdjacencyList.PAGE_SHIFT;
import static org.neo4j.graphalgo.core.huge.HugeAdjacencyList.computeAdjacencyByteSize;
import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;

public class HugeAdjacencyListTest {

    @Test
    public void shouldComputeMemoryEstimationForSinglePage() {
        GraphDimensions dimensions = new GraphDimensions.Builder()
                .setNodeCount(100)
                .setMaxRelCount(100)
                .build();

        MemoryTree memRec = HugeAdjacencyList.memoryEstimation(false).apply(dimensions, 1);

        long classSize = 24;
        long bestCaseAdjacencySize = 500;
        long worstCaseAdjacencySize = 500;

        // PageUtil.numPagesFor(bestCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK)
        int minPages = PageUtil.numPagesFor(bestCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        // PageUtil.numPagesFor(worstCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        int maxPages = PageUtil.numPagesFor(worstCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        // long bytesPerPage = MemoryUsage.sizeOfByteArray(PAGE_SIZE);
        long bytesPerPage = BitUtil.align(16 + 262144L, 8);
        // long minMemoryReqs = minPages * bytesPerPage + MemoryUsage.sizeOfObjectArray(minPages);
        long minMemoryReqs = minPages * bytesPerPage + BitUtil.align(16 + minPages * 4, 8);
        // long maxMemoryReqs = maxPages * bytesPerPage + MemoryUsage.sizeOfObjectArray(maxPages);
        long maxMemoryReqs = maxPages * bytesPerPage + BitUtil.align(16 + maxPages * 4, 8);

        MemoryRange expected = MemoryRange.of(minMemoryReqs + classSize, maxMemoryReqs + classSize);

        assertEquals(expected, memRec.memoryUsage());
    }

    @Test
    public void shouldComputeMemoryEstimationForMultiplePage() {
        GraphDimensions dimensions = new GraphDimensions.Builder()
                .setNodeCount(100_000_000L)
                .setMaxRelCount(100_000_000_000L)
                .build();

        MemoryTree memRec = HugeAdjacencyList.memoryEstimation(false).apply(dimensions, 1);

        long classSize = 24;
        long bestCaseAdjacencySize = 100_500_000_000L;
        long worstCaseAdjacencySize = 300_300_000_000L;

        // PageUtil.numPagesFor(bestCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK)
        int minPages = PageUtil.numPagesFor(bestCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        // PageUtil.numPagesFor(worstCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        int maxPages = PageUtil.numPagesFor(worstCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        // long bytesPerPage = MemoryUsage.sizeOfByteArray(PAGE_SIZE);
        long bytesPerPage = BitUtil.align(16 + 262144L, 8);
        // long minMemoryReqs = minPages * bytesPerPage + MemoryUsage.sizeOfObjectArray(minPages);
        long minMemoryReqs = minPages * bytesPerPage + BitUtil.align(16 + minPages * 4, 8);
        // long maxMemoryReqs = maxPages * bytesPerPage + MemoryUsage.sizeOfObjectArray(maxPages);
        long maxMemoryReqs = maxPages * bytesPerPage + BitUtil.align(16 + maxPages * 4, 8);

        MemoryRange expected = MemoryRange.of(minMemoryReqs + classSize, maxMemoryReqs + classSize);

        assertEquals(expected, memRec.memoryUsage());
    }

    @Test
    public void shouldComputeAdjacencyByteSize() {
        long avgDegree = 1000;
        long nodeCount = 100_000_000;
        long delta = 100_000;
        // long firstAdjacencyIdAvgByteSize = ceilDiv(encodedVLongSize(nodeCount), 2);
        long firstAdjacencyIdAvgByteSize = ceilDiv(ceilDiv(64 - Long.numberOfLeadingZeros(nodeCount - 1), 7), 2);
        // int relationshipByteSize = encodedVLongSize(delta);
        long relationshipByteSize = ceilDiv(64 - Long.numberOfLeadingZeros(delta - 1), 7);
        // int degreeByteSize = Integer.BYTES;
        int degreeByteSize = 4;
        // long compressedAdjacencyByteSize = relationshipByteSize * (avgDegree - 1);
        long compressedAdjacencyByteSize = relationshipByteSize * (avgDegree - 1);
        // return (degreeByteSize + firstAdjacencyIdAvgByteSize + compressedAdjacencyByteSize) * nodeCount;
        long expected = (degreeByteSize + firstAdjacencyIdAvgByteSize + compressedAdjacencyByteSize) * nodeCount;

        Assert.assertEquals(expected, computeAdjacencyByteSize(avgDegree, nodeCount, delta));
    }

    @Test
    public void shouldComputeAdjacencyByteSizeNoNodes() {
        long avgDegree = 0;
        long nodeCount = 0;
        long delta = 0;
        Assert.assertEquals(0, computeAdjacencyByteSize(avgDegree, nodeCount, delta));
    }

    @Test
    public void shouldComputeAdjacencyByteSizeNoRelationships() {
        long avgDegree = 0;
        long nodeCount = 100;
        long delta = 0;
        Assert.assertEquals(400, computeAdjacencyByteSize(avgDegree, nodeCount, delta));
    }
}
