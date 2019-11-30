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
package org.neo4j.graphalgo.impl.spanningTree;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.impl.spanningTrees.Prim;
import org.neo4j.graphalgo.impl.spanningTrees.SpanningTree;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Tests if MSTPrim returns a valid tree for each node
 *
 *         a                  a                  a
 *     1 /   \ 2            /  \                  \
 *      /     \            /    \                  \
 *     b --3-- c          b      c          b       c
 *     |       |  =min=>  |      |  =max=>  |       |
 *     4       5          |      |          |       |
 *     |       |          |      |          |       |
 *     d --6-- e          d      e          d-------e
 */
class PrimTest extends AlgoTestBase {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (y:Node {name: 'y'})" +
            ", (z:Node {name: 'z'})" +
            ", (a)-[:TYPE {cost: 1.0}]->(b)" +
            ", (a)-[:TYPE {cost: 2.0}]->(c)" +
            ", (b)-[:TYPE {cost: 3.0}]->(c)" +
            ", (b)-[:TYPE {cost: 4.0}]->(d)" +
            ", (c)-[:TYPE {cost: 5.0}]->(e)" +
            ", (d)-[:TYPE {cost: 6.0}]->(e)";

    private static final Label label = Label.label("Node");
    private static int a, b, c, d, e, y, z;

    private Graph graph;

    @BeforeEach
    void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testMaximumFromA(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertMaximum(new Prim(graph, graph).computeMaximumSpanningTree(a).getSpanningTree());
    }

    @AllGraphTypesWithoutCypherTest
    void testMaximumFromB(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertMaximum(new Prim(graph, graph).computeMaximumSpanningTree(b).getSpanningTree());
    }

    @AllGraphTypesWithoutCypherTest
    void testMaximumFromC(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertMaximum(new Prim(graph, graph).computeMaximumSpanningTree(c).getSpanningTree());
    }

    @AllGraphTypesWithoutCypherTest
    void testMaximumFromD(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertMaximum(new Prim(graph, graph).computeMaximumSpanningTree(d).getSpanningTree());
    }

    @AllGraphTypesWithoutCypherTest
    void testMaximumFromE(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertMaximum(new Prim(graph, graph).computeMaximumSpanningTree(e).getSpanningTree());
    }

    @AllGraphTypesWithoutCypherTest
    void testMinimumFromA(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertMinimum(new Prim(graph, graph).computeMinimumSpanningTree(a).getSpanningTree());
    }

    @AllGraphTypesWithoutCypherTest
    void testMinimumFromB(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertMinimum(new Prim(graph, graph).computeMinimumSpanningTree(b).getSpanningTree());
    }

    @AllGraphTypesWithoutCypherTest
    void testMinimumFromC(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertMinimum(new Prim(graph, graph).computeMinimumSpanningTree(c).getSpanningTree());
    }

    @AllGraphTypesWithoutCypherTest
    void testMinimumFromD(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertMinimum(new Prim(graph, graph).computeMinimumSpanningTree(d).getSpanningTree());
    }

    @AllGraphTypesWithoutCypherTest
    void testMinimumFromE(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        assertMinimum(new Prim(graph, graph).computeMinimumSpanningTree(d).getSpanningTree());
    }

    private void setup(Class<? extends GraphFactory> graphImpl) {
        graph = new GraphLoader(db)
                .withLabel(label)
                .withRelationshipType("TYPE")
                .withRelationshipProperties(PropertyMapping.of("cost", Double.MAX_VALUE))
                .undirected()
                .load(graphImpl);

        try (Transaction transaction = db.beginTx()) {
            a = Math.toIntExact(graph.toMappedNodeId(db.findNode(label, "name", "a").getId()));
            b = Math.toIntExact(graph.toMappedNodeId(db.findNode(label, "name", "b").getId()));
            c = Math.toIntExact(graph.toMappedNodeId(db.findNode(label, "name", "c").getId()));
            d = Math.toIntExact(graph.toMappedNodeId(db.findNode(label, "name", "d").getId()));
            e = Math.toIntExact(graph.toMappedNodeId(db.findNode(label, "name", "e").getId()));
            y = Math.toIntExact(graph.toMappedNodeId(db.findNode(label, "name", "y").getId()));
            z = Math.toIntExact(graph.toMappedNodeId(db.findNode(label, "name", "z").getId()));
            transaction.success();
        }
    }

    private void assertMinimum(SpanningTree mst) {
        assertEquals(5, mst.effectiveNodeCount);
        assertEquals(-1 , mst.parent[y]);
        assertEquals(-1 , mst.parent[z]);
    }

    private void assertMaximum(SpanningTree mst) {
        assertEquals(5, mst.effectiveNodeCount);
        assertEquals(-1 , mst.parent[y]);
        assertEquals(-1 , mst.parent[z]);
    }
}
