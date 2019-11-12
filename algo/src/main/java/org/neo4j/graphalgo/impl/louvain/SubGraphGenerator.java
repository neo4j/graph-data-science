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

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class SubGraphGenerator {

    static class NodeImporter {

        private final SparseNodeMapping.Builder neoToInternalBuilder;
        private final AllocationTracker tracker;
        private final AtomicLong nextAvailableId;
        private final BitSet seenNeoIds;

        NodeImporter(long oldNodeCount, long maxCommunityId, AllocationTracker tracker) {
            this.tracker = tracker;

            this.neoToInternalBuilder = SparseNodeMapping.Builder.create(maxCommunityId, tracker);
            this.nextAvailableId = new AtomicLong(0);
            seenNeoIds = new BitSet(oldNodeCount);
        }

        void addNode(long originalId) {
            if (!seenNeoIds.get(originalId)) {
                neoToInternalBuilder.set(originalId, nextAvailableId.getAndIncrement());
                seenNeoIds.set(originalId);
            }
        }

        RelImporter build() {
            SparseNodeMapping neoToInternal = neoToInternalBuilder.build();

            HugeLongArray internalToNeo = HugeLongArray.newArray(nextAvailableId.get(), tracker);
            for(LongCursor nodeId : seenNeoIds.asLongLookupContainer()) {
                internalToNeo.set(neoToInternal.get(nodeId.value), nodeId.value);
            }

            IdMap idMap = new IdMap(internalToNeo, neoToInternal, internalToNeo.size());

            return new RelImporter(idMap, tracker);
        }
    }

    static class RelImporter {

        private final RelationshipsBuilder outRelationshipsBuilder;
        private final RelationshipsBuilder inRelationshipsBuilder;
        private final RelationshipImporter relationshipImporter;
        private final RelationshipImporter.Imports imports;
        private final RelationshipsBatchBuffer relationshipBuffer;
        private final IdMap idMap;
        private long importedRelationships = 0;
        private final AllocationTracker tracker;

        RelImporter(
            IdMap idMap,
            AllocationTracker tracker
        ) {
            this.tracker = tracker;
            this.idMap = idMap;

            ImportSizing importSizing = ImportSizing.of(1, idMap.nodeCount());
            int pageSize = importSizing.pageSize();
            int numberOfPages = importSizing.numberOfPages();

            this.outRelationshipsBuilder = new RelationshipsBuilder(
                new DeduplicationStrategy[]{DeduplicationStrategy.SUM},
                AllocationTracker.EMPTY,
                0
            );

            this.inRelationshipsBuilder = new RelationshipsBuilder(
                new DeduplicationStrategy[]{DeduplicationStrategy.SUM},
                AllocationTracker.EMPTY,
                0
            );

            AdjacencyBuilder outAdjacencyBuilder = AdjacencyBuilder.compressing(
                outRelationshipsBuilder,
                numberOfPages, pageSize,
                AllocationTracker.EMPTY,
                new LongAdder(),
                new int[0],
                new double[]{}
            );

            AdjacencyBuilder inAdjacencyBuilder = AdjacencyBuilder.compressing(
                inRelationshipsBuilder,
                numberOfPages, pageSize,
                AllocationTracker.EMPTY,
                new LongAdder(),
                new int[0],
                new double[]{}
            );

            this.relationshipImporter = new RelationshipImporter(
                AllocationTracker.EMPTY,
                outAdjacencyBuilder,
                inAdjacencyBuilder
            );
            this.imports = relationshipImporter.imports(false, true, true, false);

            relationshipBuffer = new RelationshipsBatchBuffer(idMap, -1, 10_000);
        }

        void add(long source, long target) {
            relationshipBuffer.add(idMap.toMappedNodeId(source), idMap.toMappedNodeId(target), -1L, -1L);
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
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false
            );
        }

        private void flushBuffer() {
            long newImportedInOut = imports.importRels(relationshipBuffer, null);
            importedRelationships += RawValues.getHead(newImportedInOut) / 2;
            relationshipBuffer.reset();
        }

        private Relationships buildRelationships() {
            ParallelUtil.run(relationshipImporter.flushTasks(), null);
            return new Relationships(
                -1,
                importedRelationships,
                inRelationshipsBuilder.adjacency(),
                outRelationshipsBuilder.adjacency(),
                inRelationshipsBuilder.globalAdjacencyOffsets(),
                outRelationshipsBuilder.globalAdjacencyOffsets(),
                Optional.empty(),
                null,
                null,
                null,
                null
            );
        }
    }
}
