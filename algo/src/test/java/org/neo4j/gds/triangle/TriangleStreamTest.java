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
import org.neo4j.gds.graphbuilder.DefaultBuilder;
import org.neo4j.gds.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TriangleStreamTest extends BaseTest {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    private static final int TRIANGLES = 1000;

    private long centerId;
    private Graph graph;

    @BeforeEach
    void setupGraphDb() {
        RelationshipType type = RelationshipType.withName(RELATIONSHIP);
        DefaultBuilder builder = GraphBuilder.create(db)
            .setLabel(LABEL)
            .setRelationship(RELATIONSHIP)
            .newDefaultBuilder();
        Node center = builder.createNode();
        builder.newRingBuilder()
            .createRing(TRIANGLES)
            .forEachNodeInTx(node -> center.createRelationshipTo(node, type))
            .close();
        centerId = center.getId();

        graph = new StoreLoaderBuilder()
            .databaseService(db)
            .addNodeLabel(LABEL)
            .addRelationshipType(RELATIONSHIP)
            .globalOrientation(Orientation.UNDIRECTED)
            .build()
            .graph();
    }

    @Test
    void testSequential() {
        TripleConsumer mock = mock(TripleConsumer.class);

        TriangleStream.create(graph, Pools.DEFAULT, 1)
                .compute()
                .forEach(r -> mock.consume(r.nodeA, r.nodeB, r.nodeC));

        verify(mock, times(TRIANGLES)).consume(eq(centerId), anyLong(), anyLong());
    }

    @Test
    void testParallel() {
        TripleConsumer mock = mock(TripleConsumer.class);

        TriangleStream.create(graph, Pools.DEFAULT, 8)
                .compute()
                .forEach(r -> mock.consume(r.nodeA, r.nodeB, r.nodeC));

        verify(mock, times(TRIANGLES)).consume(eq(centerId), anyLong(), anyLong());
    }

    interface TripleConsumer {
        void consume(long nodeA, long nodeB, long nodeC);
    }
}
