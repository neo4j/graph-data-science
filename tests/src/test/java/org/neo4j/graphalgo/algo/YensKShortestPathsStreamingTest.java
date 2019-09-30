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
package org.neo4j.graphalgo.algo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.KShortestPathsProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Graph:
 *
 *            (0)
 *          /  |  \
 *       (4)--(5)--(1)
 *         \  /  \ /
 *         (3)---(2)
 *
 */
class YensKShortestPathsStreamingTest {

    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setupGraph() throws KernelException {
        DB = TestDatabaseCreator.createTestDatabase();
        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +
                "CREATE (f:Node {name:'f'})\n" +
                "CREATE (a)-[:TYPE {cost:1.0}]->(b)\n" +
                "CREATE (a)-[:TYPE {cost:5.0}]->(d)\n" +
                "CREATE (a)-[:TYPE {cost:7.0}]->(e)\n" +
                "CREATE (e)-[:TYPE {cost:3.0}]->(f)\n" +
                "CREATE (f)-[:TYPE {cost:4.0}]->(d)\n" +
                "CREATE (b)-[:TYPE {cost:2.0}]->(d)\n";

        DB.execute(cypher);
        DB.execute("MATCH (c:Node {name: 'c'}) DELETE c");
        DB.getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(KShortestPathsProc.class);
    }

    @AfterAll
    static void teardownGraph() {
        DB.shutdown();
    }

    @Test
    void shouldReturnNodesAndCosts() {
        final String cypher =
                "MATCH (a:Node{name:'a'}), (d:Node{name:'d'}) " +
                "CALL algo.kShortestPaths.stream(a, d, 42, 'cost') " +
                "YIELD index, sourceNodeId, targetNodeId, nodeIds, costs\n" +
                "RETURN *";

        Map<Long, List<Long>> expectedNodes = new HashMap<>();
        expectedNodes.put(0L, getNodeIds("a", "b", "d"));
        expectedNodes.put(1L, getNodeIds("a", "d"));
        expectedNodes.put(2L, getNodeIds("a", "e", "f", "d"));

        Map<Long, List<Double>> expectedCosts = new HashMap<>();
        expectedCosts.put(0L, Arrays.asList(1.0D, 2.0D));
        expectedCosts.put(1L, Arrays.asList(5.0D));
        expectedCosts.put(2L, Arrays.asList(7.0D, 3.0, 4.0));

        // 9 possible paths without loop
        DB.execute(cypher).accept(row -> {
            long rowIndex = row.getNumber("index").longValue();
            assertEquals(expectedNodes.get(rowIndex), row.get("nodeIds"));
            assertEquals(expectedCosts.get(rowIndex), row.get("costs"));

            return true;
        });
    }

    @Test
    void shouldReturnPaths() {
        final String cypher =
                "MATCH (a:Node{name:'a'}), (d:Node{name:'d'}) " +
                        "CALL algo.kShortestPaths.stream(a, d, 42, 'cost', {path: true}) " +
                        "YIELD index, sourceNodeId, targetNodeId, nodeIds, costs, path\n" +
                        "RETURN *";

        Map<Long, List<Node>> expectedNodes = new HashMap<>();
        expectedNodes.put(0L, getNodes("a", "b", "d"));
        expectedNodes.put(1L, getNodes("a", "d"));
        expectedNodes.put(2L, getNodes("a", "e", "f", "d"));

        Map<Long, List<Double>> expectedCosts = new HashMap<>();
        expectedCosts.put(0L, Arrays.asList(1.0D, 2.0D));
        expectedCosts.put(1L, Arrays.asList(5.0D));
        expectedCosts.put(2L, Arrays.asList(7.0D, 3.0, 4.0));

        DB.execute(cypher).accept(row -> {
            Path path = (Path) row.get("path");

            List<Node> actualNodes = StreamSupport.stream(path.nodes().spliterator(), false).collect(toList());
            List<Double> actualCosts = StreamSupport.stream(path.relationships().spliterator(), false)
                    .map(rel -> (double)rel.getProperty("cost")).collect(toList());

            long rowIndex = row.getNumber("index").longValue();
            assertEquals(expectedNodes.get(rowIndex), actualNodes);
            assertEquals(expectedCosts.get(rowIndex), actualCosts);

            return true;
        });
    }

    @Test
    void shouldNotStoreWeightsOnVirtualPathIfIgnoringProperty() {
        final String cypher =
                "MATCH (a:Node{name:'a'}), (d:Node{name:'d'}) " +
                        "CALL algo.kShortestPaths.stream(a, d, 42, null, {path: true}) " +
                        "YIELD index, sourceNodeId, targetNodeId, nodeIds, costs, path\n" +
                        "RETURN *";

        Map<Long, List<Node>> expectedNodes = new HashMap<>();
        expectedNodes.put(0L, getNodes("a", "d"));
        expectedNodes.put(1L, getNodes("a", "b", "d"));
        expectedNodes.put(2L, getNodes("a", "e", "f", "d"));

        Map<Long, List<Double>> expectedCosts = new HashMap<>();
        expectedCosts.put(0L, Arrays.asList(1.0D, 2.0D));
        expectedCosts.put(1L, Arrays.asList(5.0D));
        expectedCosts.put(2L, Arrays.asList(7.0D, 3.0, 4.0));

        DB.execute(cypher).accept(row -> {
            Path path = (Path) row.get("path");

            List<Node> actualNodes = StreamSupport.stream(path.nodes().spliterator(), false).collect(toList());

            long rowIndex = row.getNumber("index").longValue();
            assertEquals(expectedNodes.get(rowIndex), actualNodes);

            assertFalse(path.relationships().iterator().next().hasProperty("cost"));

            return true;
        });
    }

    private List<Long> getNodeIds(String... nodes) {
        List<Long> nodeIds;
        try (Transaction tx = DB.beginTx()) {
            nodeIds = Arrays.stream(nodes)
                    .map(name -> DB.findNode(Label.label("Node"), "name", name).getId())
                    .collect(toList());
        }
        return nodeIds;
    }
    private List<Node> getNodes(String... nodes) {
        List<Node> nodeIds;
        try (Transaction tx = DB.beginTx()) {
            nodeIds = Arrays.stream(nodes)
                    .map(name -> DB.findNode(Label.label("Node"), "name", name))
                    .collect(toList());
        }
        return nodeIds;

    }

}
