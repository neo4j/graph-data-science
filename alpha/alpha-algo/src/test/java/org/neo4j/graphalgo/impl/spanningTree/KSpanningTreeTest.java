/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphalgo.impl.spanningTrees.KSpanningTree;
import org.neo4j.graphalgo.impl.spanningTrees.Prim;
import org.neo4j.graphalgo.impl.spanningTrees.SpanningTree;
import org.neo4j.graphdb.Label;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.findNode;

/**
 *          1
 *  (x) >(a)---(d)    (x)  (a)   (d)
 *      /3 \2 /3   =>     /     /
 *    (b)---(c)         (b)   (c)
 *        1
 */
class KSpanningTreeTest extends AlgoTestBase {

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

    @BeforeEach
    void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    private Graph graph;
    private int a, b, c, d, x;

    @Test
    void testMaximumKSpanningTree() {
        loadGraph();
        final SpanningTree spanningTree = new KSpanningTree(graph, graph, graph, Prim.MAX_OPERATOR, a, 2)
                .compute();

        assertEquals(spanningTree.head(a), spanningTree.head(b));
        assertEquals(spanningTree.head(c), spanningTree.head(d));
        assertNotEquals(spanningTree.head(a), spanningTree.head(c));
        assertNotEquals(spanningTree.head(a), spanningTree.head(x));
        assertNotEquals(spanningTree.head(c), spanningTree.head(x));
    }

    @Test
    void testMinimumKSpanningTree() {
        loadGraph();
        final SpanningTree spanningTree = new KSpanningTree(graph, graph, graph, Prim.MIN_OPERATOR, a, 2)
                .compute();

        assertEquals(spanningTree.head(a), spanningTree.head(d));
        assertEquals(spanningTree.head(b), spanningTree.head(c));
        assertNotEquals(spanningTree.head(a), spanningTree.head(b));
        assertNotEquals(spanningTree.head(a), spanningTree.head(x));
        assertNotEquals(spanningTree.head(b), spanningTree.head(x));
    }

    private void loadGraph() {
        graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .globalOrientation(Orientation.UNDIRECTED)
            .addRelationshipProperty(PropertyMapping.of("w", 1.0))
            .build()
            .graph(NativeFactory.class);

        runInTransaction(db, () -> {
            a = Math.toIntExact(graph.toMappedNodeId(findNode(db, node, "name", "a").getId()));
            b = Math.toIntExact(graph.toMappedNodeId(findNode(db, node, "name", "b").getId()));
            c = Math.toIntExact(graph.toMappedNodeId(findNode(db, node, "name", "c").getId()));
            d = Math.toIntExact(graph.toMappedNodeId(findNode(db, node, "name", "d").getId()));
            x = Math.toIntExact(graph.toMappedNodeId(findNode(db, node, "name", "x").getId()));
        });
    }
}
