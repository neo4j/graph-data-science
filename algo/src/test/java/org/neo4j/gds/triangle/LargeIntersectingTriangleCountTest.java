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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.gds.graphbuilder.DefaultBuilder;
import org.neo4j.gds.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LargeIntersectingTriangleCountTest extends BaseTest {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    private static final long TRIANGLE_COUNT = 1000L;

    private static long centerId;

    ImmutableTriangleCountBaseConfig.Builder defaultConfigBuilder() {
      return ImmutableTriangleCountBaseConfig.builder();
    }

    @BeforeEach
    void setup() {
        RelationshipType type = RelationshipType.withName(RELATIONSHIP);
        DefaultBuilder builder = GraphBuilder.create(db)
            .setLabel(LABEL)
            .setRelationship(RELATIONSHIP)
            .newDefaultBuilder();
        Node center = builder.createNode();
        builder.newRingBuilder()
            .createRing((int) TRIANGLE_COUNT)
            .forEachNodeInTx(node -> center.createRelationshipTo(node, type))
            .close();
        centerId = center.getId();
    }

    private Graph graph;

    @Test
    void testQueue() {
        loadGraph();
        var result = IntersectingTriangleCount.create(
            graph,
            defaultConfigBuilder().build(),
            Pools.DEFAULT
        ).compute();
        assertEquals(TRIANGLE_COUNT, result.globalTriangles());
        assertTriangles(result.globalTriangles());
    }

    @Test
    void testQueueParallel() {
        loadGraph();
        var result = IntersectingTriangleCount.create(
            graph,
            defaultConfigBuilder().concurrency(4).build(),
            Pools.DEFAULT
        ).compute();
        assertEquals(TRIANGLE_COUNT, result.globalTriangles());
        assertTriangles(result.globalTriangles());
    }

    private void assertTriangles(Object triangles) {
        if (triangles instanceof PagedAtomicIntegerArray) {
            assertTriangle((PagedAtomicIntegerArray) triangles);
        } else if (triangles instanceof AtomicIntegerArray) {
            assertTriangle((AtomicIntegerArray) triangles);
        }
    }

    private void assertTriangle(AtomicIntegerArray triangles) {
        final int centerMapped = Math.toIntExact(graph.toMappedNodeId(centerId));
        assertEquals(TRIANGLE_COUNT, triangles.get(centerMapped));
        for (int i = 0; i < triangles.length(); i++) {
            if (i == centerMapped) {
                continue;
            }
            assertEquals(2, triangles.get(i));
        }
    }

    private void assertTriangle(PagedAtomicIntegerArray triangles) {
        final int centerMapped = Math.toIntExact(graph.toMappedNodeId(centerId));
        assertEquals(TRIANGLE_COUNT, triangles.get(centerMapped));
        for (int i = 0; i < triangles.size(); i++) {
            if (i == centerMapped) {
                continue;
            }
            assertEquals(2, triangles.get(i));
        }
    }

    private void loadGraph() {
        graph = new StoreLoaderBuilder()
            .databaseService(db)
            .addNodeLabel(LABEL)
            .addRelationshipType(RELATIONSHIP)
            .globalOrientation(Orientation.UNDIRECTED)
            .build()
            .graph();
    }
}
