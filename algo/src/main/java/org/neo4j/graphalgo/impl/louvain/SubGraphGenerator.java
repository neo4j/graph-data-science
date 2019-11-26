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

package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.AdjacencyBuilder;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.ImportSizing;
import org.neo4j.graphalgo.core.loading.RelationshipImporter;
import org.neo4j.graphalgo.core.loading.Relationships;
import org.neo4j.graphalgo.core.loading.RelationshipsBatchBuffer;
import org.neo4j.graphalgo.core.loading.RelationshipsBuilder;
import org.neo4j.graphalgo.core.loading.SparseNodeMapping;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public class SubGraphGenerator {

    public static NodeImporter create(
        long oldNodeCount,
        long maxCommunityId,
        Direction direction,
        boolean undirected,
        boolean loadRelationshipProperty,
        AllocationTracker tracker
    ) {
        return new NodeImporter(
            oldNodeCount,
            maxCommunityId,
            direction,
            undirected,
            loadRelationshipProperty,
            tracker
        );
    }

    static class NodeImporter {

        private final SparseNodeMapping.Builder neoToInternalBuilder;
        private Direction direction;
        private final boolean undirected;
        private boolean loadRelationshipProperty;
        private final AllocationTracker tracker;
        private final MutableLong nextAvailableId;
        private final BitSet seenNeoIds;

        NodeImporter(
            long oldNodeCount,
            long maxCommunityId,
            Direction direction,
            boolean undirected,
            boolean loadRelationshipProperty,
            AllocationTracker tracker
        ) {
            this.direction = direction;
            this.undirected = undirected;
            this.loadRelationshipProperty = loadRelationshipProperty;
            this.tracker = tracker;

            this.neoToInternalBuilder = SparseNodeMapping.Builder.create(maxCommunityId, tracker);
            this.nextAvailableId = new MutableLong(0);
            seenNeoIds = new BitSet(Math.min(maxCommunityId, oldNodeCount));
        }

        void addNode(long originalId) {
            if (!seenNeoIds.get(originalId)) {
                neoToInternalBuilder.set(originalId, nextAvailableId.getAndIncrement());
                seenNeoIds.set(originalId);
            }
        }

        RelImporter build() {
            SparseNodeMapping neoToInternal = neoToInternalBuilder.build();

            HugeLongArray internalToNeo = HugeLongArray.newArray(nextAvailableId.getValue(), tracker);
            for (LongCursor nodeId : seenNeoIds.asLongLookupContainer()) {
                internalToNeo.set(neoToInternal.get(nodeId.value), nodeId.value);
            }

            IdMap idMap = new IdMap(internalToNeo, neoToInternal, internalToNeo.size());

            return new RelImporter(idMap, direction, undirected, loadRelationshipProperty, tracker);
        }
    }

    static class RelImporter {

        private RelationshipsBuilder outRelationshipsBuilder;
        private RelationshipsBuilder inRelationshipsBuilder;
        private final RelationshipImporter relationshipImporter;
        private final RelationshipImporter.Imports imports;
        private final RelationshipsBatchBuffer relationshipBuffer;
        private final IdMap idMap;
        private final boolean undirected;
        private final boolean loadRelationshipProperty;
        private final AllocationTracker tracker;
        private long importedRelationships = 0;

        boolean loadOutgoing;
        boolean loadIncoming;

        RelImporter(
            IdMap idMap,
            Direction direction,
            boolean undirected,
            boolean loadRelationshipProperty,
            AllocationTracker tracker
        ) {
            this.undirected = undirected;
            this.loadRelationshipProperty = loadRelationshipProperty;
            this.tracker = tracker;
            this.idMap = idMap;

            if (undirected && direction != Direction.OUTGOING) {
                throw new IllegalArgumentException(String.format(
                    "Direction must be %s if graph is undirected, but got %s",
                    Direction.OUTGOING,
                    direction
                ));
            }

            ImportSizing importSizing = ImportSizing.of(1, idMap.nodeCount());
            int pageSize = importSizing.pageSize();
            int numberOfPages = importSizing.numberOfPages();

            AdjacencyBuilder outAdjacencyBuilder = null;
            AdjacencyBuilder inAdjacencyBuilder = null;
            int[] propertyKeyIds = loadRelationshipProperty ? new int[]{1} : new int[0];

            this.loadOutgoing = direction == Direction.OUTGOING || direction == Direction.BOTH;
            this.loadIncoming = direction == Direction.INCOMING || direction == Direction.BOTH;

            if (loadOutgoing) {
                this.outRelationshipsBuilder = new RelationshipsBuilder(
                    new DeduplicationStrategy[]{DeduplicationStrategy.SUM},
                    AllocationTracker.EMPTY,
                    loadRelationshipProperty ? 1 : 0
                );

                outAdjacencyBuilder = AdjacencyBuilder.compressing(
                    outRelationshipsBuilder,
                    numberOfPages, pageSize,
                    AllocationTracker.EMPTY,
                    new LongAdder(),
                    propertyKeyIds,
                    new double[]{}
                );
            }

            if (loadIncoming || undirected) {
                this.inRelationshipsBuilder = new RelationshipsBuilder(
                    new DeduplicationStrategy[]{DeduplicationStrategy.SUM},
                    AllocationTracker.EMPTY,
                    loadRelationshipProperty ? 1 : 0
                );

                inAdjacencyBuilder = AdjacencyBuilder.compressing(
                    inRelationshipsBuilder,
                    numberOfPages, pageSize,
                    AllocationTracker.EMPTY,
                    new LongAdder(),
                    propertyKeyIds,
                    new double[]{}
                );
            }

            this.relationshipImporter = new RelationshipImporter(
                AllocationTracker.EMPTY,
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

        void add(long source, long target) {
            relationshipBuffer.add(idMap.toMappedNodeId(source), idMap.toMappedNodeId(target), -1L, -1L);
            if (relationshipBuffer.isFull()) {
                flushBuffer();
                relationshipBuffer.reset();
            }
        }

        void add(long source, long target, double relationshipPropertyValue) {
            relationshipBuffer.add(idMap.toMappedNodeId(source), idMap.toMappedNodeId(target), -1L, Double.doubleToLongBits(relationshipPropertyValue));
            if (relationshipBuffer.isFull()) {
                flushBuffer();
                relationshipBuffer.reset();
            }
        }

        Graph build() {
            flushBuffer();
            Relationships relationships = buildRelationships();

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
        }

        private void flushBuffer() {
            RelationshipImporter.PropertyReader propertyReader = loadRelationshipProperty ? RelationshipImporter.preLoadedPropertyReader() : null;

            long newImportedInOut = imports.importRels(relationshipBuffer, propertyReader);
            importedRelationships += RawValues.getHead(newImportedInOut) / 2;
            relationshipBuffer.reset();
        }

        private Relationships buildRelationships() {
            ParallelUtil.run(relationshipImporter.flushTasks(), null);
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
}
