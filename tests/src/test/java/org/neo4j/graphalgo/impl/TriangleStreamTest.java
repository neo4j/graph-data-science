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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.triangle.TriangleStream;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class TriangleStreamTest {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    private static final long TRIANGLES = 1000;

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private static long centerId;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"}
        );
    }

    private Graph graph;

    @BeforeClass
    public static void setup() {
        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println(
                "setup took " + t + "ms for " + TRIANGLES + " nodes"))) {

            final RelationshipType type = RelationshipType.withName(RELATIONSHIP);
            final DefaultBuilder builder = GraphBuilder.create(DB)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newDefaultBuilder();
            final Node center = builder.createNode();
            builder.newRingBuilder()
                    .createRing((int) TRIANGLES)
                    .forEachNodeInTx(node -> center.createRelationshipTo(node, type));
            centerId = center.getId();
        }
    }

    public TriangleStreamTest(Class<? extends GraphFactory> graphImpl, String name) {
        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load " + name + " took " + t + "ms"))) {
            graph = new GraphLoader(DB)
                    .withDirection(Direction.BOTH)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withoutRelationshipWeights()
                    .withoutNodeWeights()
                    .asUndirected(true)
                    .load(graphImpl);
        }
    }

    @Test
    public void testSequential() {
        final TripleConsumer mock = mock(TripleConsumer.class);

        new TriangleStream(graph, Pools.DEFAULT, 1)
                .resultStream()
                .forEach(r -> mock.consume(r.nodeA, r.nodeB, r.nodeC));

        verify(mock, times((int) TRIANGLES)).consume(eq(centerId), anyLong(), anyLong());
    }

    @Test
    public void testParallel() {

        final TripleConsumer mock = mock(TripleConsumer.class);

        new TriangleStream(graph, Pools.DEFAULT, 8)
                .resultStream()
                .forEach(r -> mock.consume(r.nodeA, r.nodeB, r.nodeC));

        verify(mock, times((int) TRIANGLES)).consume(eq(centerId), anyLong(), anyLong());
    }

    interface TripleConsumer {
        void consume(long nodeA, long nodeB, long nodeC);
    }

}
