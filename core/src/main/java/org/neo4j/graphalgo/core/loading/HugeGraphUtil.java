/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.SetBitsIterable;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public final class HugeGraphUtil {

    private HugeGraphUtil() {}

    public static IdMapBuilder idMapBuilder(
        long maxOriginalId,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        return new IdMapBuilder(
            maxOriginalId,
            executorService,
            tracker
        );
    }

    public static RelationshipsBuilder createRelImporter(
        IdMap idMap,
        Orientation orientation,
        boolean loadRelationshipProperty,
        Aggregation aggregation,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        return new RelationshipsBuilder(
            idMap,
            orientation,
            loadRelationshipProperty,
            aggregation,
            executorService,
            tracker
        );
    }

    public static HugeGraph create(IdMap idMap, HugeGraph.Relationships relationships, AllocationTracker tracker) {
        return HugeGraph.create(
            idMap,
            Collections.emptyMap(),
            relationships.topology(),
            relationships.properties(),
            tracker
        );
    }

    public static class IdMapBuilder {

        final AllocationTracker tracker;
        final ExecutorService executorService;

        private final BitSet seenOriginalIds;
        private final SparseNodeMapping.Builder originalToInternalBuilder;

        private long nextAvailableId;
        private IdMap idMap;

        IdMapBuilder(
            long maxOriginalId,
            ExecutorService executorService,
            AllocationTracker tracker
        ) {
            this.executorService = executorService;
            this.tracker = tracker;

            this.originalToInternalBuilder = SparseNodeMapping.Builder.create(maxOriginalId + 1, tracker);
            this.nextAvailableId = 0;
            seenOriginalIds = new BitSet(maxOriginalId);
        }

        public void addNode(long originalId) {
            if (idMap != null) {
                throw new UnsupportedOperationException("Cannot add new nodes after `idMap` has been called");
            }

            if (!seenOriginalIds.get(originalId)) {
                originalToInternalBuilder.set(originalId, nextAvailableId++);
                seenOriginalIds.set(originalId);
            }
        }

        public IdMap build() {
            if (idMap == null) {
                SparseNodeMapping originalToInternal = originalToInternalBuilder.build();

                HugeLongArray internalToNeo = HugeLongArray.newArray(nextAvailableId, tracker);
                new SetBitsIterable(seenOriginalIds).forEach(nodeId -> internalToNeo.set(
                    originalToInternal.get(nodeId),
                    nodeId
                ));

                idMap = new IdMap(internalToNeo, originalToInternal, internalToNeo.size());
            }
            return idMap;
        }
    }

    public static class RelationshipsBuilder {

        static final int DUMMY_PROPERTY_ID = -2;
        private final org.neo4j.graphalgo.core.loading.RelationshipsBuilder relationshipsBuilder;
        private final RelationshipImporter relationshipImporter;
        private final RelationshipImporter.Imports imports;
        private final RelationshipsBatchBuffer relationshipBuffer;
        private final IdMapping idMapping;
        private final Orientation orientation;
        private final boolean loadRelationshipProperty;
        private final ExecutorService executorService;

        private long importedRelationships = 0;

        public RelationshipsBuilder(
            IdMapping idMapping,
            Orientation orientation,
            boolean loadRelationshipProperty,
            Aggregation aggregation,
            ExecutorService executorService,
            AllocationTracker tracker
        ) {
            this.orientation = orientation;
            this.loadRelationshipProperty = loadRelationshipProperty;
            this.executorService = executorService;
            this.idMapping = idMapping;

            ImportSizing importSizing = ImportSizing.of(1, idMapping.nodeCount());
            int pageSize = importSizing.pageSize();
            int numberOfPages = importSizing.numberOfPages();

            int[] propertyKeyIds = loadRelationshipProperty ? new int[]{DUMMY_PROPERTY_ID} : new int[0];
            double[] defaultValues = loadRelationshipProperty ? new double[]{Double.NaN} : new double[0];
            Aggregation[] aggregations = loadRelationshipProperty ? new Aggregation[]{Aggregation.NONE} : new Aggregation[0];

            this.relationshipsBuilder = new org.neo4j.graphalgo.core.loading.RelationshipsBuilder(
                new Aggregation[]{aggregation},
                tracker,
                loadRelationshipProperty ? 1 : 0
            );

            AdjacencyBuilder adjacencyBuilder = AdjacencyBuilder.compressing(
                relationshipsBuilder,
                numberOfPages,
                pageSize,
                tracker,
                new LongAdder(),
                propertyKeyIds,
                defaultValues,
                aggregations
            );

            this.relationshipImporter = new RelationshipImporter(tracker, adjacencyBuilder);
            this.imports = relationshipImporter.imports(orientation, loadRelationshipProperty);
            this.relationshipBuffer = new RelationshipsBatchBuffer(idMapping, -1, ParallelUtil.DEFAULT_BATCH_SIZE);
        }

        public void add(long source, long target) {
            addFromInternal(idMapping.toMappedNodeId(source), idMapping.toMappedNodeId(target));
        }

        public void add(long source, long target, double relationshipPropertyValue) {
            addFromInternal(idMapping.toMappedNodeId(source), idMapping.toMappedNodeId(target), relationshipPropertyValue);
        }

        public <T extends Relationship> void add(Stream<T> relationshipStream) {
            relationshipStream.forEach(this::add);
        }

        public synchronized <T extends Relationship>  void add(T relationship) {
            add(relationship.sourceNodeId(), relationship.targetNodeId(), relationship.property());
        }

        public void addFromInternal(long source, long target) {
            relationshipBuffer.add(source, target, -1L, -1L);
            if (relationshipBuffer.isFull()) {
                flushBuffer();
                relationshipBuffer.reset();
            }
        }

        public void addFromInternal(long source, long target, double relationshipPropertyValue) {
            relationshipBuffer.add(source, target, -1L, Double.doubleToLongBits(relationshipPropertyValue));
            if (relationshipBuffer.isFull()) {
                flushBuffer();
                relationshipBuffer.reset();
            }
        }

        public <T extends Relationship> void addFromInternal(Stream<T> relationshipStream) {
            relationshipStream.forEach(this::addFromInternal);
        }

        public synchronized <T extends Relationship> void addFromInternal(T relationship) {
            addFromInternal(relationship.sourceNodeId(), relationship.targetNodeId(), relationship.property());
        }

        public HugeGraph.Relationships build() {
            flushBuffer();

            ParallelUtil.run(relationshipImporter.flushTasks(), executorService);
            return HugeGraph.Relationships.of(
                importedRelationships,
                orientation,
                relationshipsBuilder.adjacencyList(),
                relationshipsBuilder.globalAdjacencyOffsets(),
                loadRelationshipProperty ? relationshipsBuilder.properties() : null,
                loadRelationshipProperty ? relationshipsBuilder.globalPropertyOffsets() : null,
                Double.NaN
            );
        }

        private void flushBuffer() {
            RelationshipImporter.PropertyReader propertyReader = loadRelationshipProperty ? RelationshipImporter.preLoadedPropertyReader() : null;

            long newImportedInOut = imports.importRelationships(relationshipBuffer, propertyReader);
            importedRelationships += RawValues.getHead(newImportedInOut);
            relationshipBuffer.reset();
        }
    }

    public interface Relationship {
        long sourceNodeId();
        long targetNodeId();
        double property();
    }
}
