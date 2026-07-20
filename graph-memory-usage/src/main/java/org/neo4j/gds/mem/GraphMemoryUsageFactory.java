/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.mem;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.openjdk.jol.info.RecordlessGraphWalker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public final class GraphMemoryUsageFactory {

    private GraphMemoryUsageFactory() {}

    public static GraphMemoryUsage of(GraphStore graphStore) {
        var totalSize = new MutableLong();
        var detailMemory = internalSizeOfGraph(graphStore, totalSize);

        var memoryUsage = Estimate.humanReadable(totalSize.longValue());

        return new GraphMemoryUsage(
            memoryUsage,
            totalSize.longValue(),
            detailMemory
        );
    }

    private static final Pattern ADJ_DEGREES = Pattern.compile(".*.adjacencyList.degrees.*$");
    private static final Pattern ADJ_LIST = Pattern.compile(".*.adjacencyList.pages.*$");
    private static final Pattern ADJ_OFFSETS = Pattern.compile("^.*adjacencyList.offsets.*$");
    private static final Object DUMMY = new Object();

    private static final class PackedUnsupported extends RuntimeException {
        private static final Class<?> PAL_CLASS;

        static {
            try {
                PAL_CLASS = Class.forName("org.neo4j.gds.compression.packed.PackedAdjacencyList");
            } catch (ClassNotFoundException e) {
                throw new LinkageError("Location of the PackedAdjacencyList has changed, adapt this code.");
            }
        }

        private static void check(Class<?> cls) {
            if (PAL_CLASS.isAssignableFrom(cls)) {
                throw new PackedUnsupported();
            }
        }

        private PackedUnsupported() {
            super("PackedAdjacencyList does not support sizeOf.", null, false, false);
        }
    }

    private static Map<String, Object> internalSizeOfGraph(GraphStore graphStore, MutableLong totalSize) {
        if (MemoryUsage.sizeOf(DUMMY) == -1L) {
            return Map.of();
        }
        var mappingSparseLongArray = new MutableLong();
        var mappingForward = new MutableLong();
        var mappingBackward = new MutableLong();
        var nodesTotal = new MutableLong();
        var adjacencyDegrees = new MutableLong();
        var adjacencyOffsets = new MutableLong();
        var adjacencyLists = new MutableLong();
        var relationshipsTotal = new MutableLong();

        var graphWalker = new RecordlessGraphWalker(gpr -> {
            PackedUnsupported.check(gpr.klass());

            var size = gpr.size();
            var path = gpr.path();
            var klass = gpr.klass();

            if (IdMap.class.isAssignableFrom(klass)) {
                nodesTotal.add(size);
            } else if (AdjacencyList.class.isAssignableFrom(klass)) {
                relationshipsTotal.add(size);
            }

            if (path.startsWith(".nodes.nodeTranslator.sparseLongArray")) {
                mappingSparseLongArray.add(size);
            }
            if (path.startsWith(".nodes.nodeTranslator.internalToOriginalIds")) {
                mappingForward.add(size);
            }
            if (path.startsWith(".nodes.nodeTranslator.originalToInternalIds")) {
                mappingBackward.add(size);
            }
            if (ADJ_DEGREES.matcher(path).matches()) {
                adjacencyDegrees.add(size);
            }
            if (ADJ_LIST.matcher(path).matches()) {
                adjacencyLists.add(size);
            }
            if (ADJ_OFFSETS.matcher(path).matches()) {
                adjacencyOffsets.add(size);
            }

            totalSize.add(size);
        });

        try {
            graphWalker.walk(graphStore);
        } catch (PackedUnsupported ignore) {
        }

        var mappingTotal = mappingSparseLongArray.longValue() + mappingForward.longValue() + mappingBackward.longValue();
        var adjacencyTotal = adjacencyDegrees.longValue() + adjacencyOffsets.longValue() + adjacencyLists.longValue();

        var details = new HashMap<String, Object>();
        details.put("total", totalSize.longValue());
        details.put("nodes", Map.of(
            "sparseLongArray", mappingSparseLongArray.longValue(),
            "forwardMapping", mappingForward.longValue(),
            "backwardMapping", mappingBackward.longValue(),
            "mapping", mappingTotal,
            "total", nodesTotal.longValue()
        ));
        details.put("relationships", Map.of(
            "degrees", adjacencyDegrees.longValue(),
            "offsets", adjacencyOffsets.longValue(),
            "targetIds", adjacencyLists.longValue(),
            "adjacencyLists", adjacencyTotal,
            "total", relationshipsTotal.longValue()
        ));

        if (graphStore instanceof CSRGraphStore csrGraphStore) {
            var adjacencyListDetails = new HashMap<String, Object>();
            var unionGraph = csrGraphStore.getUnion();
            unionGraph.relationshipTopologies().forEach((relationshipType, adjacency) -> {
                var mi = adjacency.adjacencyList().memoryInfo();
                var out = new HashMap<>();
                out.put("pages", mi.pages());
                out.put("bytesTotal", mi.bytesTotal().orElse(0));
                out.put("bytesOnHeap", mi.bytesOnHeap().orElse(0));
                out.put("bytesOffHeap", mi.bytesOffHeap().orElse(0));
                out.put("pageSizes", mi.pageSizes().toMap());
                out.put("heapAllocations", mi.heapAllocations().toMap());
                out.put("nativeAllocations", mi.nativeAllocations().toMap());
                out.put("headerAllocations", mi.headerAllocations().toMap());
                out.put("headerBits", mi.headerBits().toMap());
                mi.blockCount().ifPresent(blockCount -> out.put("blockCount", blockCount));
                mi.blockLengths().ifPresent(blockLengths -> out.put("blockLengths", blockLengths.toMap()));
                mi.indexOfMaxValue().ifPresent(indexOfMaxValue -> out.put("indexOfMaxValue", indexOfMaxValue.toMap()));
                mi.indexOfMinValue().ifPresent(indexOfMinValue -> out.put("indexOfMinValue", indexOfMinValue.toMap()));
                mi.maxBits().ifPresent(maxBits -> out.put("maxBits", maxBits.toMap()));
                mi.minBits().ifPresent(minBits -> out.put("minBits", minBits.toMap()));
                mi.meanBits().ifPresent(meanBits -> out.put("meanBits", meanBits.toMap()));
                mi.medianBits().ifPresent(medianBits -> out.put("medianBits", medianBits.toMap()));
                mi.stdDevBits().ifPresent(stdDevBits -> out.put("stdDevBits", stdDevBits.toMap()));
                mi.headTailDiffBits().ifPresent(headTailDiffBits -> out.put("headTailDiffBits", headTailDiffBits.toMap()));
                mi.bestMaxDiffBits().ifPresent(bestMaxDiffBits -> out.put("bestMaxDiffBits", bestMaxDiffBits.toMap()));
                mi.pforExceptions().ifPresent(pforExceptions -> out.put("exceptions", pforExceptions.toMap()));
                adjacencyListDetails.put(relationshipType.name(), out);
            });
            details.put("adjacencyLists", adjacencyListDetails);
        }
        return Collections.unmodifiableMap(details);
    }
}
