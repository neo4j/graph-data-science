/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LargeIntersectingTriangleCountTest extends AlgoTestBase {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    private static final long TRIANGLE_COUNT = 1000L;
    private static final double EXPECTED_COEFFICIENT = 0.666;

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
        IntersectingTriangleCount.TriangleCountResult result = new IntersectingTriangleCount(
            graph,
            defaultConfigBuilder().build(),
            Pools.DEFAULT,
            AllocationTracker.empty()
        ).compute();
        assertEquals(TRIANGLE_COUNT, result.globalTriangles());
        assertTriangles(result.globalTriangles());
    }

    @Test
    void testQueueParallel() {
        loadGraph();
        IntersectingTriangleCount.TriangleCountResult result = new IntersectingTriangleCount(
            graph,
            defaultConfigBuilder().concurrency(4).build(),
            Pools.DEFAULT,
            AllocationTracker.empty()
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

    private void assertCoefficients(Object coefficients) {
        if (coefficients instanceof double[]) {
            assertClusteringCoefficient((double[]) coefficients);
        } else if (coefficients instanceof PagedAtomicDoubleArray) {
            assertClusteringCoefficient((PagedAtomicDoubleArray) coefficients);
        } else if (coefficients instanceof AtomicDoubleArray) {
            assertClusteringCoefficient((AtomicDoubleArray) coefficients);
        }
    }

    private void assertClusteringCoefficient(double[] coefficients) {
        final int centerMapped = Math.toIntExact(graph.toMappedNodeId(centerId));
        for (int i = 0; i < coefficients.length; i++) {
            if (i == centerMapped) {
                continue;
            }
            assertEquals(EXPECTED_COEFFICIENT, coefficients[i], 0.01);
        }
    }

    private void assertClusteringCoefficient(AtomicDoubleArray coefficients) {
        final int centerMapped = Math.toIntExact(graph.toMappedNodeId(centerId));
        for (int i = 0; i < coefficients.length(); i++) {
            if (i == centerMapped) {
                continue;
            }
            assertEquals(EXPECTED_COEFFICIENT, coefficients.get(i), 0.01);
        }
    }

    private void assertClusteringCoefficient(PagedAtomicDoubleArray coefficients) {
        final int centerMapped = Math.toIntExact(graph.toMappedNodeId(centerId));
        for (int i = 0; i < coefficients.size(); i++) {
            if (i == centerMapped) {
                continue;
            }
            assertEquals(EXPECTED_COEFFICIENT, coefficients.get(i), 0.01);
        }
    }

    private void loadGraph() {
        graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel(LABEL)
            .addRelationshipType(RELATIONSHIP)
            .globalOrientation(Orientation.UNDIRECTED)
            .build()
            .graph();
    }
}
