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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.closeness.MSClosenessCentrality;

import java.util.function.DoubleConsumer;

import static org.mockito.AdditionalMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * (a)--(b) (d)--(e)
 *   \  /
 *   (c)
 *
 */
class ClosenessCentralityDiscoTest extends AlgoTestBase {

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +

                "CREATE" +
                " (a)-[:TYPE]->(b),\n" +
                " (a)-[:TYPE]->(c),\n" +
                " (b)-[:TYPE]->(c),\n" +

                " (d)-[:TYPE]->(e)";

        runQuery(cypher);
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    @Test
    void testHuge() {
        Graph graph = load(HugeGraphFactory.class);
        test(graph);
    }

    private Graph load(Class<? extends GraphFactory> factory) {
        return new GraphLoader(db, Pools.DEFAULT)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .undirected()
                .load(factory);
    }

    private void test(Graph g) {
        final MSClosenessCentrality algo = new MSClosenessCentrality(
                g,
                AllocationTracker.EMPTY,
                2,
                Pools.DEFAULT,
                true);
        final DoubleConsumer mock = mock(DoubleConsumer.class);
        algo.compute();
        algo.resultStream()
            .peek(System.out::println)
            .forEach(r -> mock.accept(r.centrality));

        verify(mock, times(3)).accept(eq(0.25, 0.01));
        verify(mock, times(2)).accept(eq(0.0, 0.01));
    }
}
