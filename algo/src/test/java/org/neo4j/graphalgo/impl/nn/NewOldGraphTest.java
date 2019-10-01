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

package org.neo4j.graphalgo.impl.nn;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.IdMapBuilder;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.loading.NodeImporter;
import org.neo4j.graphalgo.core.loading.NodesBatchBuffer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.roaringbitmap.RoaringBitmap;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.ArrayMatching.arrayContainingInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.impl.nn.ANNUtils.hugeGraph;
import static org.neo4j.graphalgo.impl.nn.ANNUtils.initializeRoaringBitmaps;

class NewOldGraphTest {

    @Test
    void allRelationshipsNewByDefault() {

        int numberOfNodes = 5;
        HugeLongArrayBuilder idMapBuilder = HugeLongArrayBuilder.of(numberOfNodes, AllocationTracker.EMPTY);
        NodeImporter nodeImporter = new NodeImporter(idMapBuilder, null);

        NodesBatchBuffer buffer = new NodesBatchBuffer(null, -1, numberOfNodes, false);

        for (int i = 0; i < numberOfNodes; i++) {
             buffer.add(i, -1);
        }
        nodeImporter.importNodes(buffer, null);

        IdMap idMap = IdMapBuilder.build(idMapBuilder, numberOfNodes-1, 1, AllocationTracker.EMPTY);
        IdsAndProperties nodes = new IdsAndProperties(idMap, Collections.emptyMap());

        HugeRelationshipsBuilder.HugeRelationshipsBuilderWithBuffer relationshipsBuilder = new HugeRelationshipsBuilder(nodes).withBuffer();

        relationshipsBuilder.addRelationship(0, 1);
        relationshipsBuilder.addRelationship(0, 2);
        relationshipsBuilder.addRelationship(0, 3);

        RoaringBitmap[] visitedRelationships = initializeRoaringBitmaps(5);
        NewOldGraph graph = new NewOldGraph(hugeGraph(nodes, relationshipsBuilder.build()), visitedRelationships);

        long[] newNeighbors = graph.findNewNeighbors(0).toArray();
        assertEquals(3, newNeighbors.length);
        assertThat(ArrayUtils.toObject(newNeighbors), arrayContainingInAnyOrder(1L, 2L, 3L));
    }

    @Test
    void newShouldFilterVisitedRelationships() {

        int numberOfNodes = 5;
        HugeLongArrayBuilder idMapBuilder = HugeLongArrayBuilder.of(numberOfNodes, AllocationTracker.EMPTY);
        NodeImporter nodeImporter = new NodeImporter(idMapBuilder, null);

        NodesBatchBuffer buffer = new NodesBatchBuffer(null, -1, numberOfNodes, false);

        for (int i = 0; i < numberOfNodes; i++) {
            buffer.add(i, -1);
        }
        nodeImporter.importNodes(buffer, null);

        IdMap idMap = IdMapBuilder.build(idMapBuilder, numberOfNodes-1, 1, AllocationTracker.EMPTY);
        IdsAndProperties nodes = new IdsAndProperties(idMap, Collections.emptyMap());

        HugeRelationshipsBuilder.HugeRelationshipsBuilderWithBuffer relationshipsBuilder = new HugeRelationshipsBuilder(nodes).withBuffer();

        relationshipsBuilder.addRelationship(0, 1);
        relationshipsBuilder.addRelationship(0, 2);
        relationshipsBuilder.addRelationship(0, 3);

        RoaringBitmap[] visitedRelationships = initializeRoaringBitmaps(5);
        visitedRelationships[0].add(1);

        NewOldGraph graph = new NewOldGraph(hugeGraph(nodes, relationshipsBuilder.build()), visitedRelationships);

        long[] newNeighbors = graph.findNewNeighbors(0).toArray();
        assertEquals(2, newNeighbors.length);
        assertThat(ArrayUtils.toObject(newNeighbors), arrayContainingInAnyOrder( 2L, 3L));
    }

    @Test
    void oldShouldReturnVisitedRelationships() {

        int numberOfNodes = 5;
        HugeLongArrayBuilder idMapBuilder = HugeLongArrayBuilder.of(numberOfNodes, AllocationTracker.EMPTY);
        NodeImporter nodeImporter = new NodeImporter(idMapBuilder, null);

        NodesBatchBuffer buffer = new NodesBatchBuffer(null, -1, numberOfNodes, false);

        for (int i = 0; i < numberOfNodes; i++) {
            buffer.add(i, -1);
        }
        nodeImporter.importNodes(buffer, null);

        IdMap idMap = IdMapBuilder.build(idMapBuilder, numberOfNodes-1, 1, AllocationTracker.EMPTY);
        IdsAndProperties nodes = new IdsAndProperties(idMap, Collections.emptyMap());

        HugeRelationshipsBuilder.HugeRelationshipsBuilderWithBuffer relationshipsBuilder = new HugeRelationshipsBuilder(nodes).withBuffer();

        relationshipsBuilder.addRelationship(0, 1);
        relationshipsBuilder.addRelationship(0, 2);
        relationshipsBuilder.addRelationship(0, 3);

        RoaringBitmap[] visitedRelationships = initializeRoaringBitmaps(5);
        visitedRelationships[0].add(1);

        NewOldGraph graph = new NewOldGraph(hugeGraph(nodes, relationshipsBuilder.build()), visitedRelationships);

        long[] oldNeighbors = graph.findOldNeighbors(0).toArray();
        assertEquals(1, oldNeighbors.length);
        assertThat(ArrayUtils.toObject(oldNeighbors), arrayContainingInAnyOrder( 1L));
    }
}