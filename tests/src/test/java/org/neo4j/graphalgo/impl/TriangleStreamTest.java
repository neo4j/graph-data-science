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
package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.triangle.TriangleStream;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TriangleStreamTest {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    private static final long TRIANGLES = 1000;

    private static long centerId;

    private static Graph graph;
    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setupGraphDb() {
        DB = TestDatabaseCreator.createTestDatabase();

        RelationshipType type = RelationshipType.withName(RELATIONSHIP);
        DefaultBuilder builder = GraphBuilder.create(DB)
                .setLabel(LABEL)
                .setRelationship(RELATIONSHIP)
                .newDefaultBuilder();
        Node center = builder.createNode();
        builder.newRingBuilder()
                .createRing((int) TRIANGLES)
                .forEachNodeInTx(node -> center.createRelationshipTo(node, type));
        centerId = center.getId();
    }

    @AfterAll
    static void shutdownGraphDb() {
        if (DB != null) DB.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testSequential(Class<? extends GraphFactory> graphFactory) {
        assumeFalse(graphFactory.isAssignableFrom(GraphViewFactory.class));
        setup(graphFactory);
        final TripleConsumer mock = mock(TripleConsumer.class);

        new TriangleStream(graph, Pools.DEFAULT, 1)
                .resultStream()
                .forEach(r -> mock.consume(r.nodeA, r.nodeB, r.nodeC));

        verify(mock, times((int) TRIANGLES)).consume(eq(centerId), anyLong(), anyLong());
    }

    @AllGraphTypesWithoutCypherTest
    void testParallel(Class<? extends GraphFactory> graphFactory) {
        assumeFalse(graphFactory.isAssignableFrom(GraphViewFactory.class));
        setup(graphFactory);
        final TripleConsumer mock = mock(TripleConsumer.class);

        new TriangleStream(graph, Pools.DEFAULT, 8)
                .resultStream()
                .forEach(r -> mock.consume(r.nodeA, r.nodeB, r.nodeC));

        verify(mock, times((int) TRIANGLES)).consume(eq(centerId), anyLong(), anyLong());
    }

    interface TripleConsumer {
        void consume(long nodeA, long nodeB, long nodeC);
    }

    private void setup(Class<? extends GraphFactory> graphFactory) {
        graph = new GraphLoader(DB)
                .withDirection(Direction.BOTH)
                .withLabel(LABEL)
                .withRelationshipType(RELATIONSHIP)
                .undirected()
                .load(graphFactory);

    }
}
