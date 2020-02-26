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
package org.neo4j.graphalgo.impl.similarity;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.loading.GraphGenerator;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.roaringbitmap.RoaringBitmap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.ArrayMatching.arrayContainingInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.impl.similarity.ANNUtils.initializeRoaringBitmaps;
import static org.neo4j.graphalgo.impl.similarity.ApproxNearestNeighborsAlgorithm.NewOldGraph;
import static org.neo4j.graphalgo.impl.similarity.ApproxNearestNeighborsAlgorithm.RelationshipImporter;

class NewOldGraphTest {

    private static final IdMap ID_MAP = idMap(5);
    private static final AllocationTracker TRACKER = AllocationTracker.EMPTY;

    @Test
    void allRelationshipsNewByDefault() {
        RelationshipImporter importer = RelationshipImporter.of(ID_MAP, Pools.DEFAULT, TRACKER);
        importer.addRelationship(0, 1);
        importer.addRelationship(0, 2);
        importer.addRelationship(0, 3);

        RoaringBitmap[] visitedRelationships = initializeRoaringBitmaps(5);
        NewOldGraph graph = new NewOldGraph(importer.buildGraphStore(ID_MAP, TRACKER).getUnion(), visitedRelationships);

        long[] newNeighbors = graph.findNewNeighbors(0).toArray();
        assertEquals(3, newNeighbors.length);
        assertThat(ArrayUtils.toObject(newNeighbors), arrayContainingInAnyOrder(1L, 2L, 3L));
    }

    @Test
    void newShouldFilterVisitedRelationships() {
        RelationshipImporter importer = RelationshipImporter.of(ID_MAP, Pools.DEFAULT, TRACKER);
        importer.addRelationship(0, 1);
        importer.addRelationship(0, 2);
        importer.addRelationship(0, 3);

        RoaringBitmap[] visitedRelationships = initializeRoaringBitmaps(5);
        visitedRelationships[0].add(1);

        NewOldGraph graph = new NewOldGraph(importer.buildGraphStore(ID_MAP, TRACKER).getUnion(), visitedRelationships);
        long[] newNeighbors = graph.findNewNeighbors(0).toArray();
        assertEquals(2, newNeighbors.length);
        assertThat(ArrayUtils.toObject(newNeighbors), arrayContainingInAnyOrder(2L, 3L));
    }

    @Test
    void oldShouldReturnVisitedRelationships() {
        RelationshipImporter importer = RelationshipImporter.of(ID_MAP, Pools.DEFAULT, TRACKER);
        importer.addRelationship(0, 1);
        importer.addRelationship(0, 2);
        importer.addRelationship(0, 3);

        RoaringBitmap[] visitedRelationships = initializeRoaringBitmaps(5);
        visitedRelationships[0].add(1);

        NewOldGraph graph = new NewOldGraph(importer.buildGraphStore(ID_MAP, TRACKER).getUnion(), visitedRelationships);
        long[] oldNeighbors = graph.findOldNeighbors(0).toArray();
        assertEquals(1, oldNeighbors.length);
        assertThat(ArrayUtils.toObject(oldNeighbors), arrayContainingInAnyOrder(1L));
    }

    private static IdMap idMap(int numberOfNodes) {
        GraphGenerator.NodeImporter nodeImporter = GraphGenerator.createNodeImporter(
            numberOfNodes,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );
        for (int i = 0; i < numberOfNodes; i++) {
            nodeImporter.addNode(i);
        }
        return nodeImporter.idMap();
    }
}
