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
package org.neo4j.graphalgo.core;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphView;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeFalse;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class WeightMapImportTest {

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.<Object[]>asList(
                new Object[]{HeavyGraphFactory.class, "heavy"},
                new Object[]{HugeGraphFactory.class, "huge"},
                new Object[]{GraphViewFactory.class, "view"}
        );
    }

    @Rule
    public ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Rule
    public ErrorCollector collector = new ErrorCollector();

    private Class<? extends GraphFactory> graphImpl;
    private Graph graph;

    public WeightMapImportTest(
            Class<? extends GraphFactory> graphImpl,
            String name) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void testWeightsOfInterconnectedNodesWithOutgoing() {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Direction.OUTGOING);

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);
    }

    @Test
    public void testWeightsOfTriangledNodesWithOutgoing() {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Direction.OUTGOING);

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);
        checkWeight(2, Direction.OUTGOING, 3.0);
    }

    @Test
    public void testWeightsOfInterconnectedNodesWithIncoming() {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Direction.INCOMING);

        checkWeight(0, Direction.INCOMING, 2.0);
        checkWeight(1, Direction.INCOMING, 1.0);
    }

    @Test
    public void testWeightsOfTriangledNodesWithIncoming() {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Direction.INCOMING);

        checkWeight(0, Direction.INCOMING, 3.0);
        checkWeight(1, Direction.INCOMING, 1.0);
        checkWeight(2, Direction.INCOMING, 2.0);
    }

    @Test
    public void testWeightsOfInterconnectedNodesWithBoth() {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Direction.BOTH);

        // loading both overwrites the weights in the following order,
        // which expects the GraphFactory to load OUTGOINGs before INCOMINGs
        //   (a)-[{w:1}]->(b)  |  (a)<-[{w:2}]-(b)  |  (b)-[{w:2}]->(a)  |  (b)<-[{w:1}]-(a)
        // therefore the final weight for in/outs of either a/b is 1,
        // the weight of 2 is discarded.
        // This cannot be represented in the graph view
        assumeFalse("GraphView is not able to represent the test case", graph instanceof GraphView);

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);

        checkWeight(0, Direction.INCOMING, 2.0);
        checkWeight(1, Direction.INCOMING, 1.0);

        checkWeight(0, Direction.BOTH, fromGraph(1.0, 1.0), 1.0, 2.0);
        checkWeight(1, Direction.BOTH, fromGraph(2.0, 2.0), 2.0, 1.0);
    }

    @Test
    public void testWeightsOfTriangledNodesWithBoth() {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Direction.BOTH);

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);
        checkWeight(2, Direction.OUTGOING, 3.0);

        checkWeight(0, Direction.INCOMING, 3.0);
        checkWeight(1, Direction.INCOMING, 1.0);
        checkWeight(2, Direction.INCOMING, 2.0);

        checkWeight(0, Direction.BOTH, fromGraph(1.0, 0.0), 1.0, 3.0);
        checkWeight(1, Direction.BOTH, fromGraph(2.0, 0.0), 2.0, 1.0);
        checkWeight(2, Direction.BOTH, fromGraph(3.0, 0.0), 3.0, 2.0);
    }

    private void setup(String cypher, Direction direction) {
        DB.execute(cypher);
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withDirection(direction)
                .withRelationshipWeightsFromProperty("w", 0.0)
                .load(graphImpl);
    }

    private void checkWeight(int nodeId, Direction direction, double... expecteds) {
        graph.forEachRelationship(nodeId, direction, checks(direction, expecteds, expecteds));
    }

    private double[] fromGraph(double... exptecteds) {
        return exptecteds;
    }

    private void checkWeight(int nodeId, Direction direction, double[] expectedFromGraph, double... expectedFromIterator) {
        graph.forEachRelationship(nodeId, direction, checks(direction, expectedFromIterator, expectedFromGraph));
    }

    private WeightedRelationshipConsumer checks(Direction direction, double[] expectedFromIterator, double[] expectedFromGraph) {
        AtomicInteger i = new AtomicInteger();
        int limit = Math.min(expectedFromIterator.length, expectedFromGraph.length);
        return (s, t, w) -> {
            String rel = String.format("(%d %s %d)", s, arrow(direction), t);
            if (i.get() >= limit) {
                collector.addError(new RuntimeException(String.format("Unexpected relationship: %s = %.1f", rel, w)));
                return false;
            }
            double actual = (direction == Direction.INCOMING) ? graph.weightOf(t, s) : graph.weightOf(s, t);
            final int index = i.getAndIncrement();
            double expectedIterator = expectedFromIterator[index];
            double expectedGraph = expectedFromGraph[index];
            collector.checkThat(String.format("%s (RI+W): %.1f != %.1f", rel, actual, expectedGraph), actual, is(closeTo(expectedGraph, 1e-4)));
            collector.checkThat(String.format("%s (WRI): %.1f != %.1f", rel, w, expectedIterator), w, is(closeTo(expectedIterator, 1e-4)));
            return true;
        };
    }

    private static String arrow(Direction direction) {
        switch (direction) {
            case OUTGOING:
                return "->";
            case INCOMING:
                return "<-";
            case BOTH:
                return "<->";
            default:
                return "???";
        }
    }
}
