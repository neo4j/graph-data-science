/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.impl.jaccard;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.AdjacencyBuilder;
import org.neo4j.graphalgo.core.loading.ImportSizing;
import org.neo4j.graphalgo.core.loading.RelationshipImporter;
import org.neo4j.graphalgo.core.loading.Relationships;
import org.neo4j.graphalgo.core.loading.RelationshipsBatchBuffer;
import org.neo4j.graphalgo.core.loading.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.StatementConstants;

import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class SimilarityGraphBuilder {

    private static final int DEFAULT_WEIGHT_PROPERTY_ID = -2;

    static MemoryEstimation memoryEstimation(int topk, int top) {
        return MemoryEstimations.setup("", (dimensions, concurrency) -> {
            long maxNodesToCompare = Math.min(dimensions.maxRelCount(), dimensions.nodeCount());
            long maxNumberOfSimilarityResults = maxNodesToCompare * (maxNodesToCompare - 1) / 2;
            long maxNodesWithNewRels = maxNodesToCompare;
            if (top > 0) {
                maxNumberOfSimilarityResults = Math.min(maxNumberOfSimilarityResults, top);
                maxNodesWithNewRels = maxNumberOfSimilarityResults * 2;
            }
            int averageDegree = Math.toIntExact(maxNumberOfSimilarityResults / maxNodesWithNewRels);
            if (topk > 0) {
                averageDegree = Math.min(averageDegree, topk);
            }
            return MemoryEstimations.builder(HugeGraph.class)
                .add(
                    "adjacency list",
                    AdjacencyList.compressedMemoryEstimation(averageDegree, maxNodesWithNewRels)
                )
                .add("adjacency offsets", AdjacencyOffsets.memoryEstimation())
                .build();
        });
    }

    private final Graph baseGraph;
    private final AllocationTracker tracker;
    private final int concurrency;
    private final int bufferSize;

    SimilarityGraphBuilder(Graph baseGraph, AllocationTracker tracker) {
        this.baseGraph = baseGraph;
        this.tracker = tracker;
        this.concurrency = 1;
        this.bufferSize = (int) Math.min(baseGraph.nodeCount(), ParallelUtil.DEFAULT_BATCH_SIZE);
    }

    Graph build(Stream<SimilarityResult> stream) {
        Relationships relationships = loadRelationships(stream);
        return ((HugeGraph) baseGraph).copyWithNewRelationships(
            relationships.relationshipCount(),
            null,
            relationships.outAdjacency(),
            null,
            relationships.outOffsets(),
            true,
            null,
            relationships.outRelProperties(),
            null,
            relationships.outRelPropertyOffsets()
        );
    }

    private Relationships loadRelationships(Stream<SimilarityResult> stream) {
        ImportSizing importSizing = ImportSizing.of(concurrency, baseGraph.nodeCount());
        int pageSize = importSizing.pageSize();
        int numberOfPages = importSizing.numberOfPages();

        RelationshipsBuilder outgoingRelationshipsBuilder = new RelationshipsBuilder(
            new DeduplicationStrategy[]{DeduplicationStrategy.NONE},
            tracker,
            1);
        AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(
            outgoingRelationshipsBuilder,
            numberOfPages,
            pageSize,
            tracker,
            new LongAdder(),
            new int[]{DEFAULT_WEIGHT_PROPERTY_ID},
            new double[]{0.0}
        );

        RelationshipImporter importer = new RelationshipImporter(tracker, outBuilder, null);
        RelationshipImporter.Imports imports = importer.imports(false, true, false, true);

        RelationshipsBatchBuffer buffer = new RelationshipsBatchBuffer(baseGraph, StatementConstants.ANY_RELATIONSHIP_TYPE, bufferSize);
        RelationshipWriter writer = new RelationshipWriter(imports, buffer);
        // TODO: Similarity graph creation is not thread safe yet.
        //       We need to synchronize when the stream is parallel, but this hurts non-parallel performance, so we provide two methods for now.
        if (stream.isParallel()) {
            stream.forEach(writer);
        } else {
            stream.forEach(writer::acceptNonParallel);
        }
        writer.flush();

        importer.flushTasks().forEach(Runnable::run);
        AdjacencyList outAdjacencyList = outgoingRelationshipsBuilder.adjacency();
        AdjacencyOffsets outAdjacencyOffsets = outgoingRelationshipsBuilder.globalAdjacencyOffsets();
        AdjacencyList outWeightList = outgoingRelationshipsBuilder.weights();
        AdjacencyOffsets outWeightOffsets = outgoingRelationshipsBuilder.globalWeightOffsets();

        return new Relationships(
            writer.relationshipCount,
            writer.relationshipCount,
            null,
            outAdjacencyList,
            null,
            outAdjacencyOffsets,
            Optional.of(0.0),
            null,
            outWeightList,
            null,
            outWeightOffsets
        );
    }

    private static class RelationshipWriter implements Consumer<SimilarityResult> {

        private static final long NO_RELATIONSHIP_REFERENCE = -1L;

        final RelationshipImporter.Imports imports;
        final RelationshipsBatchBuffer buffer;
        final RelationshipImporter.PropertyReader relPropertyReader;
        long relationshipCount = 0;

        RelationshipWriter(RelationshipImporter.Imports imports, RelationshipsBatchBuffer buffer) {
            this.imports = imports;
            this.buffer = buffer;
            this.relPropertyReader = RelationshipImporter.preLoadedPropertyReader();
        }

        @Override
        public synchronized void accept(SimilarityResult result) {
            add(result.node1, result.node2, result.similarity);
        }

        public void acceptNonParallel(SimilarityResult result) {
            add(result.node1, result.node2, result.similarity);
        }

        private void add(long node1, long node2, double similarity) {
            buffer.add(
                node1,
                node2,
                NO_RELATIONSHIP_REFERENCE,
                Double.doubleToLongBits(similarity)
            );

            if (buffer.isFull()) {
                flush();
                reset();
            }
        }

        void flush() {
            long imported = imports.importRels(buffer, relPropertyReader);
            relationshipCount += RawValues.getHead(imported);
        }

        private void reset() {
            buffer.reset();
        }
    }
}
