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
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.SetBitsIterable;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public final class GraphGenerator {

    private GraphGenerator() {}

    public static NodeImporter createNodeImporter(
        long maxOriginalId,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        return new NodeImporter(
            maxOriginalId,
            executorService,
            tracker
        );
    }

    public static RelImporter createRelImporter(
        NodeImporter nodeImporter,
        Projection projection,
        boolean loadRelationshipProperty,
        Aggregation aggregation
    ) {
        return createRelImporter(
            nodeImporter.idMap(),
            projection,
            loadRelationshipProperty,
            aggregation,
            nodeImporter.executorService,
            nodeImporter.tracker
        );
    }

    public static RelImporter createRelImporter(
        IdMap idMap,
        Projection projection,
        boolean loadRelationshipProperty,
        Aggregation aggregation,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        return new RelImporter(
            idMap,
            projection,
            loadRelationshipProperty,
            aggregation,
            executorService,
            tracker
        );
    }

    public static class NodeImporter {

        final AllocationTracker tracker;
        final ExecutorService executorService;

        private final BitSet seenOriginalIds;
        private final SparseNodeMapping.Builder originalToInternalBuilder;

        private long nextAvailableId;
        private IdMap idMap;

        NodeImporter(
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

        public IdMap idMap() {
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

    public static class RelImporter {

        static final int DUMMY_PROPERTY_ID = -2;
        private final RelationshipsBuilder relationshipsBuilder;
        private final RelationshipImporter relationshipImporter;
        private final RelationshipImporter.Imports imports;
        private final RelationshipsBatchBuffer relationshipBuffer;
        private final IdMap idMap;
        private final boolean loadUndirected;
        private final boolean loadRelationshipProperty;
        private final ExecutorService executorService;
        private final AllocationTracker tracker;

        private long importedRelationships = 0;

        public RelImporter(
            IdMap idMap,
            Projection projection,
            boolean loadRelationshipProperty,
            Aggregation aggregation,
            ExecutorService executorService,
            AllocationTracker tracker
        ) {
            this.loadRelationshipProperty = loadRelationshipProperty;
            this.executorService = executorService;
            this.tracker = tracker;
            this.idMap = idMap;
            this.loadUndirected = projection == Projection.UNDIRECTED;

            ImportSizing importSizing = ImportSizing.of(1, idMap.nodeCount());
            int pageSize = importSizing.pageSize();
            int numberOfPages = importSizing.numberOfPages();

            int[] propertyKeyIds = loadRelationshipProperty ? new int[]{DUMMY_PROPERTY_ID} : new int[0];

            this.relationshipsBuilder = new RelationshipsBuilder(
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
                new double[]{}
            );

            this.relationshipImporter = new RelationshipImporter(tracker, adjacencyBuilder);
            this.imports = relationshipImporter.imports(
                loadUndirected,
                projection == Projection.NATURAL,
                projection == Projection.REVERSE,
                loadRelationshipProperty
            );
            this.relationshipBuffer = new RelationshipsBatchBuffer(idMap, -1, ParallelUtil.DEFAULT_BATCH_SIZE);
        }

        public void add(long source, long target) {
            addFromInternal(idMap.toMappedNodeId(source), idMap.toMappedNodeId(target));
        }

        public void add(long source, long target, double relationshipPropertyValue) {
            addFromInternal(idMap.toMappedNodeId(source), idMap.toMappedNodeId(target), relationshipPropertyValue);
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

        public HugeGraph buildGraph() {
            flushBuffer();

            Relationships relationships = buildRelationships();
            AdjacencyList adjacencyList = relationships.adjacencyList();
            AdjacencyOffsets adjacencyOffsets = relationships.adjacencyOffsets();
            Optional<AdjacencyList> properties = loadRelationshipProperty
                ? Optional.of(relationships.properties())
                : Optional.empty();
            Optional<AdjacencyOffsets> propertyOffsets = loadRelationshipProperty
                ? Optional.of(relationships.propertyOffsets())
                : Optional.empty();

            return HugeGraph.create(
                tracker,
                idMap,
                Collections.emptyMap(),
                relationships.relationshipCount(),
                adjacencyList,
                adjacencyOffsets,
                Optional.empty(),
                properties,
                propertyOffsets,
                loadUndirected
            );
        }

        private void flushBuffer() {
            RelationshipImporter.PropertyReader propertyReader = loadRelationshipProperty ? RelationshipImporter.preLoadedPropertyReader() : null;

            long newImportedInOut = imports.importRels(relationshipBuffer, propertyReader);
            importedRelationships += RawValues.getHead(newImportedInOut);
            relationshipBuffer.reset();
        }

        private Relationships buildRelationships() {
            ParallelUtil.run(relationshipImporter.flushTasks(), executorService);
            return new Relationships(
                -1,
                importedRelationships,
                relationshipsBuilder.adjacencyList(),
                relationshipsBuilder.globalAdjacencyOffsets(),
                loadRelationshipProperty ? relationshipsBuilder.properties() : null,
                loadRelationshipProperty ? relationshipsBuilder.globalPropertyOffsets() : null
            );
        }
    }

    public interface Relationship {
        long sourceNodeId();
        long targetNodeId();
        double property();
    }
}
