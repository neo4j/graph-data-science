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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class SubGraphGeneratorTest {

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = Direction.class)
    void unweighted(Direction direction) {
        int nodeCount = 4;
        SubGraphGenerator.NodeImporter nodeImporter = SubGraphGenerator.create(
            nodeCount,
            nodeCount,
            direction,
            false,
            false,
            AllocationTracker.EMPTY
        );

        for (int i = 0; i < nodeCount; i++) {
            nodeImporter.addNode(i);
        }

        SubGraphGenerator.RelImporter relImporter = nodeImporter.build();
        for (int i = 0; i < nodeCount; i++) {
            relImporter.add(i, (i + 1) % nodeCount);
        }
        Graph graph = relImporter.build();
        assertGraphEquals(fromGdl("(a)-->(b)-->(c)-->(d)-->(a)"), graph);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = Direction.class)
    void weighted(Direction direction) {
        Graph graph = generateGraph(direction, false);
        assertGraphEquals(fromGdl("(a)-[{w: 0.0}]->(b)-[{w: 1.0}]->(c)-[{w: 2.0}]->(d)-[{w: 3.0}]->(a)"), graph);
        assertEquals(direction, graph.getLoadDirection());
    }

    @Test
    void undirected() {
        Graph graph = generateGraph(Direction.OUTGOING, true);
        assertGraphEquals(fromGdl("(a)-[{w: 0.0}]->(b)-[{w: 1.0}]->(c)-[{w: 2.0}]->(d)-[{w: 3.0}]->(a)"), graph);
        assertEquals(Direction.OUTGOING, graph.getLoadDirection());
    }

    @Test
    void shouldFailOnIncomingWithUndirected() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> generateGraph(Direction.INCOMING, true)
        );
        assertTrue(exception.getMessage().contains("Direction must be OUTGOING if graph is undirected"));
    }

    @Test
    void shouldMergeParallelRelationships() {
        int nodeCount = 4;

        SubGraphGenerator.NodeImporter nodeImporter = SubGraphGenerator.create(
            nodeCount,
            nodeCount,
            Direction.BOTH,
            false,
            true,
            AllocationTracker.EMPTY
        );

        for (int i = 0; i < nodeCount; i++) {
            nodeImporter.addNode(i);
        }

        SubGraphGenerator.RelImporter relImporter = nodeImporter.build();
        for (int i = 0; i < nodeCount * 2; i++) {
            int index = i % nodeCount;
            relImporter.add(index, (index + 1) % nodeCount, index);
        }
        Graph graph = relImporter.build();
        assertGraphEquals(fromGdl("(a)-[{w: 0.0}]->(b)-[{w: 2.0}]->(c)-[{w: 4.0}]->(d)-[{w: 6.0}]->(a)"), graph);
    }

    private Graph generateGraph(Direction outgoing, boolean undirected) {
        int nodeCount = 4;

        SubGraphGenerator.NodeImporter nodeImporter = SubGraphGenerator.create(
            nodeCount,
            nodeCount,
            outgoing,
            undirected,
            true,
            AllocationTracker.EMPTY
        );

        for (int i = 0; i < nodeCount; i++) {
            nodeImporter.addNode(i);
        }

        SubGraphGenerator.RelImporter relImporter = nodeImporter.build();
        for (int i = 0; i < nodeCount; i++) {
            relImporter.add(i, (i + 1) % nodeCount, i);
        }
        return relImporter.build();
    }

}