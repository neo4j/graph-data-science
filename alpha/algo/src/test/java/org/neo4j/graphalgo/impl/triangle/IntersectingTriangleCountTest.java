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
package org.neo4j.graphalgo.impl.triangle;

import com.carrotsearch.hppc.LongHashSet;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.GraphGenerator;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.IdMapBuilder;
import org.neo4j.graphalgo.core.loading.NodeImporter;
import org.neo4j.graphalgo.core.loading.NodesBatchBuffer;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsIterableContaining.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IntersectingTriangleCountTest {

    @Test
    void noTriangles() {
        long[] inputs = new long[]{1, 2};
        IdMap idMap = createIdMap(inputs);

        GraphGenerator.RelImporter importer = new GraphGenerator.RelImporter(
            idMap,
            Projection.NATURAL,
            false,
            Aggregation.NONE,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        importer.add(1, 2);
        importer.add(2, 1);

        Graph graph = importer.buildGraph();
        IntersectingTriangleCount triangleCount = new IntersectingTriangleCount(graph, Pools.DEFAULT, 1, AllocationTracker.EMPTY);
        triangleCount.compute();

        assertEquals(0, triangleCount.getTriangleCount());

        List<IntersectingTriangleCount.Result> results = triangleCount.computeStream().collect(Collectors.toList());
        assertEquals(2, results.size());
        assertThat(results, hasItem(new IntersectingTriangleCount.Result(1L, 0L, 0.0)));
        assertThat(results, hasItem(new IntersectingTriangleCount.Result(2L, 0L, 0.0)));
    }

    @Test
    void noRelationships() {
        long[] inputs = new long[]{1, 2};
        IdMap idMap = createIdMap(inputs);

        GraphGenerator.RelImporter importer = new GraphGenerator.RelImporter(
            idMap,
            Projection.NATURAL,
            false,
            Aggregation.NONE,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );
        Graph graph = importer.buildGraph();

        IntersectingTriangleCount triangleCount = new IntersectingTriangleCount(graph, Pools.DEFAULT, 1, AllocationTracker.EMPTY);
        triangleCount.compute();

        assertEquals(0, triangleCount.getTriangleCount());

        List<IntersectingTriangleCount.Result> results = triangleCount.computeStream().collect(Collectors.toList());
        assertEquals(2, results.size());
        assertThat(results, hasItem(new IntersectingTriangleCount.Result(1L, 0L, 0.0)));
        assertThat(results, hasItem(new IntersectingTriangleCount.Result(2L, 0L, 0.0)));
    }

    @Test
    void oneTriangle() {
        long[] inputs = new long[]{1, 2, 3};
        IdMap idMap = createIdMap(inputs);

        GraphGenerator.RelImporter importer = new GraphGenerator.RelImporter(
            idMap,
            Projection.NATURAL,
            false,
            Aggregation.NONE,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        importer.add(1, 2);
        importer.add(2, 1);
        importer.add(2, 3);
        importer.add(3, 2);
        importer.add(3, 1);
        importer.add(1, 3);

        Graph graph = importer.buildGraph();
        IntersectingTriangleCount triangleCount = new IntersectingTriangleCount(graph, Pools.DEFAULT, 1, AllocationTracker.EMPTY);
        triangleCount.compute();

        assertEquals(1, triangleCount.getTriangleCount());

        List<IntersectingTriangleCount.Result> results = triangleCount.computeStream().collect(Collectors.toList());
        assertEquals(3, results.size());
        assertThat(results, hasItem(new IntersectingTriangleCount.Result(1L, 1L, 1.0)));
        assertThat(results, hasItem(new IntersectingTriangleCount.Result(2L, 1L, 1.0)));
        assertThat(results, hasItem(new IntersectingTriangleCount.Result(3L, 1L, 1.0)));
    }

    private IdMap createIdMap(long[] inputs) {
        HugeLongArrayBuilder idMapBuilder = HugeLongArrayBuilder.of(inputs.length, AllocationTracker.EMPTY);
        NodeImporter nodeImporter = new NodeImporter(idMapBuilder, null);

        NodesBatchBuffer buffer = new NodesBatchBuffer(null, new LongHashSet(), inputs.length, false);

        long maxNodeId = 0L;
        for (long input : inputs) {
            if (input > maxNodeId) {
                maxNodeId = input;
            }
            buffer.add(input, -1);
            if (buffer.isFull()) {
                nodeImporter.importNodes(buffer, null);
                buffer.reset();
            }
        }
        nodeImporter.importNodes(buffer, null);

        return IdMapBuilder.build(idMapBuilder, maxNodeId, 1, AllocationTracker.EMPTY);
    }

}
