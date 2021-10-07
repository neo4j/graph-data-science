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
package org.neo4j.gds.impl.similarity;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.extension.GdlSupportExtension;
import org.roaringbitmap.RoaringBitmap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.ArrayMatching.arrayContainingInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.impl.similarity.ANNUtils.initializeRoaringBitmaps;
import static org.neo4j.gds.impl.similarity.ApproxNearestNeighborsAlgorithm.NewOldGraph;
import static org.neo4j.gds.impl.similarity.ApproxNearestNeighborsAlgorithm.RelationshipImporter;

class NewOldGraphTest {

    private static final NodeMapping ID_MAP = idMap(5);
    private static final int CONCURRENCY = 1;
    private static final AllocationTracker ALLOCATION_TRACKER = AllocationTracker.empty();

    @Test
    void allRelationshipsNewByDefault() {
        RelationshipImporter importer = RelationshipImporter.of(ID_MAP, Pools.DEFAULT, ALLOCATION_TRACKER);
        importer.addRelationshipFromOriginalId(0, 1);
        importer.addRelationshipFromOriginalId(0, 2);
        importer.addRelationshipFromOriginalId(0, 3);

        RoaringBitmap[] visitedRelationships = initializeRoaringBitmaps(5);
        NewOldGraph graph = new NewOldGraph(
            importer.buildGraphStore(
                GdlSupportExtension.DATABASE_ID,
                ID_MAP,
                CONCURRENCY,
                ALLOCATION_TRACKER
            ).getUnion(),
            visitedRelationships
        );

        long[] newNeighbors = graph.findNewNeighbors(0).toArray();
        assertEquals(3, newNeighbors.length);
        assertThat(ArrayUtils.toObject(newNeighbors), arrayContainingInAnyOrder(1L, 2L, 3L));
    }

    @Test
    void newShouldFilterVisitedRelationships() {
        RelationshipImporter importer = RelationshipImporter.of(ID_MAP, Pools.DEFAULT, ALLOCATION_TRACKER);
        importer.addRelationshipFromOriginalId(0, 1);
        importer.addRelationshipFromOriginalId(0, 2);
        importer.addRelationshipFromOriginalId(0, 3);

        RoaringBitmap[] visitedRelationships = initializeRoaringBitmaps(5);
        visitedRelationships[0].add(1);

        NewOldGraph graph = new NewOldGraph(
            importer.buildGraphStore(
                GdlSupportExtension.DATABASE_ID,
                ID_MAP,
                CONCURRENCY,
                ALLOCATION_TRACKER
            ).getUnion(),
            visitedRelationships
        );
        long[] newNeighbors = graph.findNewNeighbors(0).toArray();
        assertEquals(2, newNeighbors.length);
        assertThat(ArrayUtils.toObject(newNeighbors), arrayContainingInAnyOrder(2L, 3L));
    }

    @Test
    void oldShouldReturnVisitedRelationships() {
        RelationshipImporter importer = RelationshipImporter.of(ID_MAP, Pools.DEFAULT, ALLOCATION_TRACKER);
        importer.addRelationshipFromOriginalId(0, 1);
        importer.addRelationshipFromOriginalId(0, 2);
        importer.addRelationshipFromOriginalId(0, 3);

        RoaringBitmap[] visitedRelationships = initializeRoaringBitmaps(5);
        visitedRelationships[0].add(1);

        NewOldGraph graph = new NewOldGraph(
            importer.buildGraphStore(
                GdlSupportExtension.DATABASE_ID,
                ID_MAP,
                CONCURRENCY,
                ALLOCATION_TRACKER
            ).getUnion(),
            visitedRelationships
        );
        long[] oldNeighbors = graph.findOldNeighbors(0).toArray();
        assertEquals(1, oldNeighbors.length);
        assertThat(ArrayUtils.toObject(oldNeighbors), arrayContainingInAnyOrder(1L));
    }

    private static NodeMapping idMap(int numberOfNodes) {
        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(numberOfNodes - 1)
            .allocationTracker(AllocationTracker.empty())
            .build();

        for (int i = 0; i < numberOfNodes; i++) {
            nodesBuilder.addNode(i);
        }
        return nodesBuilder.build().nodeMapping();
    }
}
