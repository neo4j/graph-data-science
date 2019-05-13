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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.triangle.BalancedTriads;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**

 *
 * @author mknblch
 */
public class BalancedTriadsTest {

    interface BalancedTriadTestConsumer {
        void accept(long node, long balanced, long unbalanced);
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setup() {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" + // center node
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE" +

                        " (a)-[:TYPE {w:1.0}]->(b),\n" +
                        " (a)-[:TYPE {w:-1.0}]->(c),\n" +
                        " (a)-[:TYPE {w:1.0}]->(d),\n" +
                        " (a)-[:TYPE {w:-1.0}]->(e),\n" +
                        " (a)-[:TYPE {w:1.0}]->(f),\n" +
                        " (a)-[:TYPE {w:-1.0}]->(g),\n" +

                        " (b)-[:TYPE {w:-1.0}]->(c),\n" +
                        " (c)-[:TYPE {w:1.0}]->(d),\n" +
                        " (d)-[:TYPE {w:-1.0}]->(e),\n" +
                        " (e)-[:TYPE {w:1.0}]->(f),\n" +
                        " (f)-[:TYPE {w:-1.0}]->(g),\n" +
                        " (g)-[:TYPE {w:1.0}]->(b)";

        DB.execute(cypher);

        graph = new GraphLoader(DB, Pools.DEFAULT)
                .withLabel("Node")
                .withRelationshipStatement("TYPE")
                .withRelationshipWeightsFromProperty("w", 0.0)
                .withSort(true)
                .asUndirected(true)
                .load(HugeGraphFactory.class);
    }

    @Test
    public void testStream() throws Exception {
        final BalancedTriadTestConsumer consumer = mock(BalancedTriadTestConsumer.class);
        new BalancedTriads(graph, Pools.DEFAULT, 4, AllocationTracker.EMPTY)
                .compute()
                .stream()
                .forEach(r -> consumer.accept(r.nodeId, r.balanced, r.unbalanced));
        verify(consumer, times(1)).accept(eq(0L), eq(3L), eq(3L));
        verify(consumer, times(6)).accept(anyInt(), eq(1L), eq(1L));
    }
}
