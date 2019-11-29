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

package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.StatementConstants;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class RelationshipStreamBuilder {

    public interface Relationship {
        long sourceNodeId();
        long targetNodeId();
        double property();
    }

    private static final int DEFAULT_WEIGHT_PROPERTY_ID = -2;

    private final IdMapping idMapping;
    private final ExecutorService executorService;

    private final int concurrency;
    private final int bufferSize;
    private final AllocationTracker tracker;

    public RelationshipStreamBuilder(IdMapping idMapping, ExecutorService executorService, AllocationTracker tracker) {
        this.idMapping = idMapping;
        this.concurrency = 1;
        this.bufferSize = (int) Math.min(idMapping.nodeCount(), ParallelUtil.DEFAULT_BATCH_SIZE);
        this.executorService = executorService;
        this.tracker = tracker;
    }

    public <T extends Relationship> Relationships loadRelationships(Stream<T> stream) {
        ImportSizing importSizing = ImportSizing.of(concurrency, idMapping.nodeCount());
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

        RelationshipsBatchBuffer buffer = new RelationshipsBatchBuffer(idMapping, StatementConstants.ANY_RELATIONSHIP_TYPE, bufferSize);
        RelationshipWriter writer = new RelationshipWriter(imports, buffer);
        stream.forEach(writer);
        writer.flush();

        ParallelUtil.run(importer.flushTasks(), executorService);
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

    private static class RelationshipWriter implements Consumer<Relationship> {

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

        // TODO: Stream-based relationship creation is not thread-safe.
        @Override
        public synchronized void accept(Relationship result) {
            add(result.sourceNodeId(), result.targetNodeId(), result.property());
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
