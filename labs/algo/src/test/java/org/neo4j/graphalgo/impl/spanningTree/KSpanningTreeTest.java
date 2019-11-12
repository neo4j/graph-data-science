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
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.impl.spanningTrees.KSpanningTree;
import org.neo4j.graphalgo.impl.spanningTrees.SpanningTree;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 *          1
 *  (x) >(a)---(d)    (x)  (a)   (d)
 *      /3 \2 /3   =>     /     /
 *    (b)---(c)         (b)   (c)
 *        1
 */
class KSpanningTreeTest {

    private static final String DB_CYPHER =
            "CREATE " +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (x:Node {name: 'x'})" +
            ", (a)-[:TYPE {w: 3.0}]->(b)" +
            ", (a)-[:TYPE {w: 2.0}]->(c)" +
            ", (a)-[:TYPE {w: 1.0}]->(d)" +
            ", (b)-[:TYPE {w: 1.0}]->(c)" +
            ", (d)-[:TYPE {w: 3.0}]->(c)";

    private static final Label node = Label.label("Node");

    private GraphDatabaseAPI db;

    @BeforeEach
    void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void shutdown() {
        if (db != null) db.shutdown();
    }

    private Graph graph;
    private int a, b, c, d, x;

    @AllGraphTypesWithoutCypherTest
    void testMaximumKSpanningTree(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        final SpanningTree spanningTree = new KSpanningTree(graph, graph, graph)
                .compute(a, 2, true)
                .getSpanningTree();

        assertEquals(spanningTree.head(a), spanningTree.head(b));
        assertEquals(spanningTree.head(c), spanningTree.head(d));
        assertNotEquals(spanningTree.head(a), spanningTree.head(c));
        assertNotEquals(spanningTree.head(a), spanningTree.head(x));
        assertNotEquals(spanningTree.head(c), spanningTree.head(x));
    }

    @AllGraphTypesWithoutCypherTest
    void testMinimumKSpanningTree(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        final SpanningTree spanningTree = new KSpanningTree(graph, graph, graph)
                .compute(a, 2, false)
                .getSpanningTree();

        assertEquals(spanningTree.head(a), spanningTree.head(d));
        assertEquals(spanningTree.head(b), spanningTree.head(c));
        assertNotEquals(spanningTree.head(a), spanningTree.head(b));
        assertNotEquals(spanningTree.head(a), spanningTree.head(x));
        assertNotEquals(spanningTree.head(b), spanningTree.head(x));
    }

    private void setup(Class<? extends GraphFactory> graphImpl) {
        graph = new GraphLoader(db)
                .withRelationshipProperties(PropertyMapping.of("w", 1.0))
                .withAnyRelationshipType()
                .withAnyLabel()
                .undirected()
                .load(graphImpl);

        try (Transaction tx = db.beginTx()) {
            a = Math.toIntExact(graph.toMappedNodeId(db.findNode(node, "name", "a").getId()));
            b = Math.toIntExact(graph.toMappedNodeId(db.findNode(node, "name", "b").getId()));
            c = Math.toIntExact(graph.toMappedNodeId(db.findNode(node, "name", "c").getId()));
            d = Math.toIntExact(graph.toMappedNodeId(db.findNode(node, "name", "d").getId()));
            x = Math.toIntExact(graph.toMappedNodeId(db.findNode(node, "name", "x").getId()));
            tx.success();
        }
    }
}
