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
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.SetBitsIterable;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

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
        Direction direction,
        boolean undirected,
        boolean loadRelationshipProperty,
        DeduplicationStrategy deduplicationStrategy
    ) {
        return createRelImporter(
            nodeImporter,
            direction,
            undirected,
            loadRelationshipProperty,
            deduplicationStrategy,
            true
        );
    }

    public static RelImporter createRelImporter(
        NodeImporter nodeImporter,
        Direction direction,
        boolean undirected,
        boolean loadRelationshipProperty,
        DeduplicationStrategy deduplicationStrategy,
        boolean legacyMode
    ) {
        return createRelImporter(
            nodeImporter.idMap(),
            direction,
            undirected,
            loadRelationshipProperty,
            deduplicationStrategy,
            nodeImporter.executorService,
            nodeImporter.tracker,
            legacyMode
        );
    }

    public static RelImporter createRelImporter(
        IdMap idMap,
        Direction direction,
        boolean undirected,
        boolean loadRelationshipProperty,
        DeduplicationStrategy deduplicationStrategy,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        return createRelImporter(
            idMap,
            direction,
            undirected,
            loadRelationshipProperty,
            deduplicationStrategy,
            executorService,
            tracker,
            true
        );
    }

    public static RelImporter createRelImporter(
        IdMap idMap,
        Direction direction,
        boolean undirected,
        boolean loadRelationshipProperty,
        DeduplicationStrategy deduplicationStrategy,
        ExecutorService executorService,
        AllocationTracker tracker,
        boolean legacyMode
    ) {
        return new RelImporter(
            idMap,
            direction,
            undirected,
            loadRelationshipProperty,
            deduplicationStrategy,
            executorService,
            tracker,
            legacyMode
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
        private RelationshipsBuilder outRelationshipsBuilder;
        private RelationshipsBuilder inRelationshipsBuilder;
        private final RelationshipImporter relationshipImporter;
        private final RelationshipImporter.Imports imports;
        private final RelationshipsBatchBuffer relationshipBuffer;
        private final IdMap idMap;
        private final Direction direction;
        private final boolean undirected;
        private final boolean loadRelationshipProperty;
        private final ExecutorService executorService;
        private final AllocationTracker tracker;
        private final boolean legacyMode;

        private long importedRelationships = 0;


        boolean loadOutgoing;
        boolean loadIncoming;

        public RelImporter(
            IdMap idMap,
            Direction direction,
            boolean undirected,
            boolean loadRelationshipProperty,
            DeduplicationStrategy deduplicationStrategy,
            ExecutorService executorService,
            AllocationTracker tracker,
            boolean legacyMode
        ) {
            this.direction = direction;
            this.undirected = undirected;
            this.loadRelationshipProperty = loadRelationshipProperty;
            this.executorService = executorService;
            this.tracker = tracker;
            this.idMap = idMap;
            this.legacyMode = legacyMode;

            if (undirected && direction != Direction.OUTGOING) {
                throw new IllegalArgumentException(String.format(
                    "Direction must be %s if graph is undirected, but got %s",
                    Direction.OUTGOING,
                    direction
                ));
            }

            if (!legacyMode && direction == Direction.BOTH) {
                throw new IllegalArgumentException("Direction.BOTH is invalid in non-legacy mode.");
            }

            ImportSizing importSizing = ImportSizing.of(1, idMap.nodeCount());
            int pageSize = importSizing.pageSize();
            int numberOfPages = importSizing.numberOfPages();

            AdjacencyBuilder outAdjacencyBuilder = null;
            AdjacencyBuilder inAdjacencyBuilder = null;
            int[] propertyKeyIds = loadRelationshipProperty ? new int[]{DUMMY_PROPERTY_ID} : new int[0];

            this.loadOutgoing = direction == Direction.OUTGOING || direction == Direction.BOTH;
            this.loadIncoming = direction == Direction.INCOMING || direction == Direction.BOTH;

            if (loadOutgoing) {
                this.outRelationshipsBuilder = new RelationshipsBuilder(
                    new DeduplicationStrategy[]{deduplicationStrategy},
                    tracker,
                    loadRelationshipProperty ? 1 : 0
                );

                outAdjacencyBuilder = AdjacencyBuilder.compressing(
                    outRelationshipsBuilder,
                    numberOfPages,
                    pageSize,
                    tracker,
                    new LongAdder(),
                    propertyKeyIds,
                    new double[]{}
                );
            }

            if (loadIncoming || undirected) {
                this.inRelationshipsBuilder = new RelationshipsBuilder(
                    new DeduplicationStrategy[]{deduplicationStrategy},
                    tracker,
                    loadRelationshipProperty ? 1 : 0
                );

                inAdjacencyBuilder = AdjacencyBuilder.compressing(
                    inRelationshipsBuilder,
                    numberOfPages,
                    pageSize,
                    tracker,
                    new LongAdder(),
                    propertyKeyIds,
                    new double[]{}
                );
            }

            this.relationshipImporter = new RelationshipImporter(
                tracker,
                outAdjacencyBuilder,
                inAdjacencyBuilder
            );
            this.imports = relationshipImporter.imports(
                undirected,
                loadOutgoing,
                loadIncoming,
                loadRelationshipProperty
            );

            relationshipBuffer = new RelationshipsBatchBuffer(idMap, -1, ParallelUtil.DEFAULT_BATCH_SIZE);
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

            if (legacyMode) {
                return HugeGraph.create(
                    tracker,
                    idMap,
                    Collections.emptyMap(),
                    relationships.relationshipCount(),
                    relationships.inAdjacency(),
                    relationships.outAdjacency(),
                    relationships.inOffsets(),
                    relationships.outOffsets(),
                    Optional.empty(),
                    loadRelationshipProperty && loadIncoming ? Optional.of(relationships.inRelProperties()) : Optional.empty(),
                    loadRelationshipProperty && loadOutgoing ? Optional.of(relationships.outRelProperties()) : Optional.empty(),
                    loadRelationshipProperty && loadIncoming ? Optional.of(relationships.inRelPropertyOffsets()) : Optional.empty(),
                    loadRelationshipProperty && loadOutgoing ? Optional.of(relationships.outRelPropertyOffsets()) : Optional.empty(),
                    undirected
                );
            } else {
                // In non-legacy mode we load either outgoing or incoming or undirected.
                // The corresponding adjacency list is always stored in the outgoing
                // adjacency list of the resulting graph.
                return HugeGraph.create(
                    tracker,
                    idMap,
                    Collections.emptyMap(),
                    relationships.relationshipCount(),
                    null,
                    loadOutgoing ? relationships.outAdjacency() : relationships.inAdjacency(),
                    null,
                    loadOutgoing ? relationships.outOffsets() : relationships.inOffsets(),
                    Optional.empty(),
                    Optional.empty(),
                    loadRelationshipProperty ?
                        loadOutgoing
                            ? Optional.of(relationships.outRelProperties())
                            : Optional.of(relationships.inRelProperties())
                        : Optional.empty(),
                    Optional.empty(),
                    loadRelationshipProperty ?
                        loadOutgoing
                            ? Optional.of(relationships.outRelPropertyOffsets())
                            : Optional.of(relationships.inRelPropertyOffsets())
                        : Optional.empty(),
                    undirected
                );
            }
        }

        private void flushBuffer() {
            RelationshipImporter.PropertyReader propertyReader = loadRelationshipProperty ? RelationshipImporter.preLoadedPropertyReader() : null;

            long newImportedInOut = imports.importRels(relationshipBuffer, propertyReader);
            importedRelationships += direction == Direction.BOTH ? RawValues.getHead(newImportedInOut) / 2 : RawValues.getHead(newImportedInOut);
            relationshipBuffer.reset();
        }

        private Relationships buildRelationships() {
            ParallelUtil.run(relationshipImporter.flushTasks(), executorService);
            return new Relationships(
                -1,
                importedRelationships,
                loadIncoming ? inRelationshipsBuilder.adjacency() : null,
                loadOutgoing ? outRelationshipsBuilder.adjacency() : null,
                loadIncoming ? inRelationshipsBuilder.globalAdjacencyOffsets() : null,
                loadOutgoing ? outRelationshipsBuilder.globalAdjacencyOffsets() : null,
                Optional.empty(),
                loadRelationshipProperty && loadIncoming ? inRelationshipsBuilder.weights() : null,
                loadRelationshipProperty && loadOutgoing ? outRelationshipsBuilder.weights() : null,
                loadRelationshipProperty && loadIncoming ? inRelationshipsBuilder.globalWeightOffsets() : null,
                loadRelationshipProperty && loadOutgoing ? outRelationshipsBuilder.globalWeightOffsets() : null
            );
        }
    }

    public interface Relationship {
        long sourceNodeId();
        long targetNodeId();
        double property();
    }
}
