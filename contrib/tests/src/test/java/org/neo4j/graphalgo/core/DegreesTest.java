/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * A->B; A->C; B->C;
 *
 *
 *     OutD:   InD: BothD:
 * A:     2      0      2
 * B:     1      1      2
 * C:     0      2      2
 *
 *
 *
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class DegreesTest {

    private static final String unidirectional =
                    "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (b:Node {name:'b'})\n" +
                    "CREATE (c:Node {name:'c'})\n" + // shuffled

                    "CREATE" +

                    " (a)-[:TYPE]->(b),\n" +
                    " (a)-[:TYPE]->(c),\n" +
                    " (b)-[:TYPE]->(c)";

    private static final String bidirectional =
                    "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (b:Node {name:'b'})\n" +
                    "CREATE (c:Node {name:'c'})\n" + // shuffled

                    "CREATE" +

                    " (a)-[:TYPE]->(b),\n" +
                    " (b)-[:TYPE]->(a),\n" +
                    " (a)-[:TYPE]->(c),\n" +
                    " (c)-[:TYPE]->(a),\n" +
                    " (b)-[:TYPE]->(c),\n" +
                    " (c)-[:TYPE]->(b)";


    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private Class<? extends GraphFactory> graphImpl;
    private Graph graph;

    public DegreesTest(
            Class<? extends GraphFactory> graphImpl,
            String name) {
        this.graphImpl = graphImpl;
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {

        return Arrays.<Object[]>asList(
                new Object[]{HeavyGraphFactory.class, "heavy"},
                new Object[]{HugeGraphFactory.class, "huge"}
        );
    }

    private void setup(String cypher, Direction direction) {
        DB.execute(cypher);
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withDirection(direction == null ? Direction.BOTH : direction)
                .asUndirected(direction == null)
                .withRelationshipWeightsFromProperty("w", 0.0)
                .load(graphImpl);
    }

    private int nodeId(String name) {
        try (Transaction transaction = DB.beginTx()) {
            return graph.toMappedNodeId(DB.findNodes(Label.label("Node"), "name", name).next().getId());
        }
    }

    @Test
    public void testUnidirectionalOutgoing() {
        setup(unidirectional, Direction.OUTGOING);
        assertEquals(2, graph.degree(nodeId("a"), Direction.OUTGOING));
        assertEquals(1, graph.degree(nodeId("b"), Direction.OUTGOING));
        assertEquals(0, graph.degree(nodeId("c"), Direction.OUTGOING));
    }

    @Test
    public void testUnidirectionalIncoming() {
        setup(unidirectional, Direction.INCOMING);
        assertEquals(0, graph.degree(nodeId("a"), Direction.INCOMING));
        assertEquals(1, graph.degree(nodeId("b"), Direction.INCOMING));
        assertEquals(2, graph.degree(nodeId("c"), Direction.INCOMING));
    }

    @Test
    public void testUnidirectionalUndirected() {

        setup(unidirectional, Direction.BOTH);
        assertEquals(2, graph.degree(nodeId("a"), Direction.BOTH));
        assertEquals(2, graph.degree(nodeId("b"), Direction.BOTH));
        assertEquals(2, graph.degree(nodeId("c"), Direction.BOTH));
    }

    @Test
    public void testBidirectionalOutgoing() {
        setup(bidirectional, Direction.OUTGOING);
        assertEquals(2, graph.degree(nodeId("a"), Direction.OUTGOING));
        assertEquals(2, graph.degree(nodeId("b"), Direction.OUTGOING));
        assertEquals(2, graph.degree(nodeId("c"), Direction.OUTGOING));
    }

    @Test
    public void testBidirectionalIncoming() {
        setup(bidirectional, Direction.INCOMING);
        assertEquals(2, graph.degree(nodeId("a"), Direction.INCOMING));
        assertEquals(2, graph.degree(nodeId("b"), Direction.INCOMING));
        assertEquals(2, graph.degree(nodeId("c"), Direction.INCOMING));
    }

    @Test
    public void testBidirectionalBoth() {

        setup(bidirectional, Direction.BOTH);
        assertEquals(4, graph.degree(nodeId("a"), Direction.BOTH));
        assertEquals(4, graph.degree(nodeId("b"), Direction.BOTH));
        assertEquals(4, graph.degree(nodeId("c"), Direction.BOTH));
    }

    @Test
    public void testBidirectionalUndirected() {

        setup(bidirectional, null);
        assertEquals(2, graph.degree(nodeId("a"), Direction.OUTGOING));
        assertEquals(2, graph.degree(nodeId("b"), Direction.OUTGOING));
        assertEquals(2, graph.degree(nodeId("c"), Direction.OUTGOING));
    }

}
