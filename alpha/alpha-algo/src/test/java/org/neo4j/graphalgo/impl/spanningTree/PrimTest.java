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
import org.neo4j.graphalgo.impl.spanningTrees.Prim;
import org.neo4j.graphalgo.impl.spanningTrees.SpanningTree;
import org.neo4j.graphdb.Label;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.findNode;


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

    @Test
    void testMaximumFromA() {
        loadGraph();
        assertMaximum(new Prim(graph, graph, Prim.MAX_OPERATOR, a).compute());
    }

    @Test
    void testMaximumFromB() {
        loadGraph();
        assertMaximum(new Prim(graph, graph, Prim.MAX_OPERATOR, b).compute());
    }

    @Test
    void testMaximumFromC() {
        loadGraph();
        assertMaximum(new Prim(graph, graph, Prim.MAX_OPERATOR, c).compute());
    }

    @Test
    void testMaximumFromD() {
        loadGraph();
        assertMaximum(new Prim(graph, graph, Prim.MAX_OPERATOR, d).compute());
    }

    @Test
    void testMaximumFromE() {
        loadGraph();
        assertMaximum(new Prim(graph, graph, Prim.MAX_OPERATOR, e).compute());
    }

    @Test
    void testMinimumFromA() {
        loadGraph();
        assertMinimum(new Prim(graph, graph, Prim.MIN_OPERATOR, a).compute());
    }

    @Test
    void testMinimumFromB() {
        loadGraph();
        assertMinimum(new Prim(graph, graph, Prim.MIN_OPERATOR, b).compute());
    }

    @Test
    void testMinimumFromC() {
        loadGraph();
        assertMinimum(new Prim(graph, graph, Prim.MIN_OPERATOR, c).compute());
    }

    @Test
    void testMinimumFromD() {
        loadGraph();
        assertMinimum(new Prim(graph, graph, Prim.MIN_OPERATOR, d).compute());
    }

    @Test
    void testMinimumFromE() {
        loadGraph();
        assertMinimum(new Prim(graph, graph, Prim.MIN_OPERATOR, e).compute());
    }

    private void loadGraph() {
        graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel(label.name())
            .addRelationshipType("TYPE")
            .globalOrientation(Orientation.UNDIRECTED)
            .addRelationshipProperty(PropertyMapping.of("cost", Double.MAX_VALUE))
            .build()
            .graph(NativeFactory.class);

        runInTransaction(db, () -> {
            a = Math.toIntExact(graph.toMappedNodeId(findNode(db, label, "name", "a").getId()));
            b = Math.toIntExact(graph.toMappedNodeId(findNode(db, label, "name", "b").getId()));
            c = Math.toIntExact(graph.toMappedNodeId(findNode(db, label, "name", "c").getId()));
            d = Math.toIntExact(graph.toMappedNodeId(findNode(db, label, "name", "d").getId()));
            e = Math.toIntExact(graph.toMappedNodeId(findNode(db, label, "name", "e").getId()));
            y = Math.toIntExact(graph.toMappedNodeId(findNode(db, label, "name", "y").getId()));
            z = Math.toIntExact(graph.toMappedNodeId(findNode(db, label, "name", "z").getId()));
        });
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
